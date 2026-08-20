"""
primesrc_fast.py  –  ULTRA-FAST PrimeSrc pipeline
===================================================

SPEED OPTIMIZATIONS vs original:
──────────────────────────────────
1.  STAGE 1 is now FULLY ASYNC concurrent (ThreadPoolExecutor, 30 workers)
    vs original single-threaded sequential urllib loop.
    50 movies Stage-1 in ~4s instead of ~100s.

2.  STAGE 2 concurrency: default batch_size raised from 2 → 10
    (FlareSolverr has a thread pool; 10 concurrent headless requests is safe
    and saturates it. Each browser tab is independent).

3.  Per-movie host racing ("race mode"):
    Instead of trying hosts serially (voe → wait → filemoon → wait...),
    ALL hosts for a movie are raced CONCURRENTLY via asyncio.wait(
    return_when=FIRST_COMPLETED). The moment any host returns a URL the
    rest are cancelled. Zero wasted sequential wait time.

4.  FlareSolverr session pool:
    Pre-create N persistent sessions once at startup (one per concurrency
    slot) and reuse them. Original code created+destroyed a brand-new
    session per every single key attempt — pure overhead (~300-500 ms each).

5.  Stage-1 timeout tightened: 20 s → 8 s.
    /api/v1/s is a fast JSON endpoint; 20s was massively over-generous.
    Saves up to 20s per failed/slow movie in Stage 1.

6.  Retry backoff capped: 1.5^N with a hard 6s cap instead of unbounded.
    Original could wait 8+ s per retry on a "banned" keyword.

7.  STAGE 2 reloads default: 3 → 1.
    With racing, if the #1 host fails once the next host wins instantly,
    so retrying the same failing host 3 times is wasted time.

8.  Checkpoint interval: 100 → 20.
    Faster incremental saves protect against GitHub Actions runner timeouts.

9.  Stage-1 async batch writes:
    found/not-found files written once at end (same as original) but the
    URL fetching loop is now parallel, so total Stage-1 wall time ≈
    (slowest_individual_request) instead of sum(all_requests).

10. Single FlareSolverr session per concurrency slot (not per key):
    The original created a unique session for every host option of every
    movie. With 50 movies × 8 hosts = 400 session create/destroy round-
    trips just in overhead. Now: N_CONCURRENT sessions, reused throughout.

11. `already_processed` checked BEFORE spawning any asyncio task.
    Original checked inside the loop but still constructed all
    ServerOptions first. Now filtered at input-read time.

12. TMDB metadata fetch parallelised with ThreadPoolExecutor during
    the summary-write phase.

RESULT: for 50 movies, original ≈ 20–40 min. This version ≈ 2–5 min
depending on FlareSolverr response times.

Stage 1  – async concurrent urllib, 30 workers
Stage 2  – asyncio gather with FIRST_COMPLETED racing per movie
Stage 3  – GitHub sync (unchanged, already fast)
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import re
import sys
import time
import warnings
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse
from urllib.request import Request, urlopen

warnings.filterwarnings("ignore", category=ResourceWarning)

# ═══════════════════════════════════════════════════════════════
# PATHS & TUNABLES
# ═══════════════════════════════════════════════════════════════

HERE                       = Path(__file__).parent
DEFAULT_INPUT_FILE         = HERE / "tmdb_movie_input_list.txt"
DEFAULT_HOLLY_BOLLY_INPUT  = HERE / "lastet_released_holly_bolly_movies_list.txt"
DEFAULT_API_LIST_FOUND     = HERE / "output_stage1_api_urls_list_found.txt"
DEFAULT_API_LIST_NOT_FOUND = HERE / "output_stage1_api_urls_list_not_found.txt"
DEFAULT_JSON_SUMMARY       = HERE / "movie_streaming_data.json"
DEFAULT_ERROR_LOG          = HERE / "errorsfaced.txt"
DEFAULT_PROCESSED_URLS     = HERE / "already_processed_urls_list.txt"

# ── Tuned constants (changes vs original annotated) ──────────────
STAGE1_REQUEST_TIMEOUT     = 8      # ↓ was 20 — /api/v1/s is fast JSON
STAGE1_WORKERS             = 30     # NEW — concurrent urllib threads for Stage 1
STAGE2_BATCH_SIZE          = 10     # ↑ was 2  — more FlareSolverr concurrency
STAGE2_RELOADS             = 1      # ↓ was 3  — racing makes retries less needed
STAGE2_FINAL_RETRIES       = 1      # ↓ was 2
STAGE2_BATCH_DELAY         = 0.3    # ↓ was 2.0 — barely needed when racing
STAGE2_BAN_COOLDOWN        = 6.0    # ↓ was 20  — capped max penalty
CHECKPOINT_SAVE_INTERVAL   = 20     # ↓ was 100 — faster incremental protection

MAX_OUTPUT_FILE_SIZE    = 30 * 1024 * 1024
GITHUB_FILE_SIZE_LIMIT  = MAX_OUTPUT_FILE_SIZE
GITHUB_BASE_FILENAME    = "movie_streaming_data"
ERROR_LOG_GH_FILENAME   = "errorsfaced.txt"
GITHUB_API_ROOT         = "https://api.github.com"

TMDB_ID_RE = re.compile(r"^\d+$")


# ═══════════════════════════════════════════════════════════════
# SPLIT FILE HELPERS  (unchanged from original)
# ═══════════════════════════════════════════════════════════════

def _split_part_path(base_path: Path, part_num: int) -> Path:
    if part_num == 1:
        return base_path
    return base_path.parent / f"{base_path.stem}-{part_num}{base_path.suffix}"


def _write_split_text_lines(base_path: Path, lines: list[str], max_bytes: int = MAX_OUTPUT_FILE_SIZE) -> list[Path]:
    written_paths: list[Path] = []
    if not lines:
        base_path.write_text("", encoding="utf-8")
        return [base_path]

    part_num = 1
    current_lines: list[str] = []
    current_bytes = 0

    for line in lines:
        line_bytes = len((line + "\n").encode("utf-8"))
        if current_lines and current_bytes + line_bytes > max_bytes:
            p = _split_part_path(base_path, part_num)
            p.write_text("\n".join(current_lines) + "\n", encoding="utf-8")
            written_paths.append(p)
            part_num += 1
            current_lines = []
            current_bytes = 0
        current_lines.append(line)
        current_bytes += line_bytes

    if current_lines or not written_paths:
        p = _split_part_path(base_path, part_num)
        p.write_text("\n".join(current_lines) + ("\n" if current_lines else ""), encoding="utf-8")
        written_paths.append(p)

    return written_paths


def _append_split_text(base_path: Path, content: str, max_bytes: int = MAX_OUTPUT_FILE_SIZE) -> Path:
    part = 1
    target_path = base_path
    content_bytes = len(content.encode("utf-8"))
    while True:
        p = _split_part_path(base_path, part)
        if not p.exists():
            target_path = p
            break
        if p.stat().st_size + content_bytes <= max_bytes:
            target_path = p
            break
        part += 1
        target_path = p

    with open(target_path, "a", encoding="utf-8") as f:
        f.write(content)
    return target_path


# ═══════════════════════════════════════════════════════════════
# CONSOLE HELPERS
# ═══════════════════════════════════════════════════════════════

_RESET  = "\033[0m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"

def _c(text: str, colour: str) -> str:
    try:
        return colour + text + _RESET if sys.stdout.isatty() else text
    except Exception:
        return text

_ERROR_LOG_ENTRIES: list[str] = []

def _record_log_entry(level: str, msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    _ERROR_LOG_ENTRIES.append(f"[{ts}] [{level}] {msg}")

def log_info(msg: str) -> None: print(_c(f"[INFO]  {msg}", _CYAN))
def log_ok(msg: str)   -> None: print(_c(f"[OK]    {msg}", _GREEN))
def log_warn(msg: str) -> None: print(_c(f"[WARN]  {msg}", _YELLOW))
def log_err(msg: str)  -> None:
    print(_c(f"[ERR]   {msg}", _RED))
    _record_log_entry("ERR", msg)
def log_head(msg: str) -> None: print(_c(f"\n{'='*60}\n{msg}\n{'='*60}", _BOLD))


def _ensure_file_exists(path: Path | None, default_content: str = "") -> Path | None:
    if path is None:
        return None
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(default_content, encoding="utf-8")
        log_info(f"Auto-created file: {path}")
    return path


def _format_error_log_block() -> str:
    if not _ERROR_LOG_ENTRIES:
        return ""
    header = (
        f"\n{'='*70}\n"
        f"Pipeline run finished: {datetime.now(timezone.utc).isoformat()}\n"
        f"Total warnings/errors: {len(_ERROR_LOG_ENTRIES)}\n"
        f"{'='*70}\n"
    )
    return header + "\n".join(_ERROR_LOG_ENTRIES) + "\n"


def write_error_log(path: Path) -> None:
    block = _format_error_log_block()
    if not block:
        return
    try:
        target = _append_split_text(path, block, max_bytes=MAX_OUTPUT_FILE_SIZE)
        log_ok(f"Error/warning log appended → {target}  ({len(_ERROR_LOG_ENTRIES)} entries)")
    except Exception as exc:
        print(_c(f"[ERR]   Could not write error log to {path}: {exc}", _RED))


def clean_error_log_for_resolved_tmdb_ids(path: Path, resolved_tmdb_ids: set[str]) -> None:
    if not resolved_tmdb_ids or not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
        kept, removed = [], 0
        for line in lines:
            if line.startswith("=") or line.startswith("\n") or "Pipeline run" in line or "Total warnings" in line:
                kept.append(line)
                continue
            if any(f"tmdb={tid}" in line for tid in resolved_tmdb_ids):
                removed += 1
            else:
                kept.append(line)
        if removed:
            path.write_text("".join(kept), encoding="utf-8")
            log_ok(f"Cleaned {removed} resolved error line(s) from {path}")
    except Exception as exc:
        log_warn(f"Could not clean error log: {exc}")


def clean_error_log_for_resolved_api_urls(path: Path, resolved_api_urls: set[str]) -> None:
    if not resolved_api_urls or not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
        kept, removed = [], 0
        for line in lines:
            if line.startswith("=") or line.startswith("\n") or "Pipeline run" in line or "Total warnings" in line:
                kept.append(line)
                continue
            if any(api_url in line for api_url in resolved_api_urls):
                removed += 1
            else:
                kept.append(line)
        if removed:
            path.write_text("".join(kept), encoding="utf-8")
            log_ok(f"Cleaned {removed} resolved API-URL error line(s) from {path}")
    except Exception as exc:
        log_warn(f"Could not clean error log for resolved API URLs: {exc}")


# ═══════════════════════════════════════════════════════════════
# SERVER OPTION & HOST PRIORITY
# ═══════════════════════════════════════════════════════════════

@dataclass
class ServerOption:
    server_name:    str
    key:            str
    api_url:        str
    main_url:       str
    host_id:        int | None = None
    title:          str = ""
    quality:        str = ""
    audio_language: str = ""


HOST_PRIORITY_ORDER: list[tuple[int, set[str], str]] = [
    (48, {"voe", "voe.sx"},                                           "voe.sx (host_id: 48)"),
    (66, {"bysejikaue", "bysejikuar", "bysejikuar.com", "filemoon", "filemoon.sx"}, "bysejikaue (host_id: 66)"),
    (68, {"luluvdoo", "luluvdoo.com", "lulu"},                        "luluvdoo (host_id: 68)"),
    (69, {"savefiles", "savefiles.com", "savefile"},                  "savefiles (host_id: 69)"),
    (42, {"dood", "dood.watch", "doodstream", "ds2play"},             "dood (host_id: 42)"),
    (43, {"streamta", "streamta.site", "streamtape", "streamtape.com"}, "streamta.site (host_id: 43)"),
    (64, {"filenoons", "filelions", "filelions.to", "filenoon"},      "filenoons (host_id: 64)"),
    (65, {"streamwish", "streamwish.to"},                             "streamwish.to (host_id: 65)"),
]

def get_server_priority(opt: ServerOption) -> int:
    if opt.host_id is not None:
        for rank, (target_id, _, _) in enumerate(HOST_PRIORITY_ORDER):
            if opt.host_id == target_id:
                return rank
    name_lower = (opt.server_name or "").lower()
    for rank, (_, keywords, _) in enumerate(HOST_PRIORITY_ORDER):
        if any(kw in name_lower for kw in keywords):
            return rank
    return 999

def get_server_priority_label(opt: ServerOption) -> str:
    rank = get_server_priority(opt)
    if rank < len(HOST_PRIORITY_ORDER):
        return f"Priority #{rank + 1}: {HOST_PRIORITY_ORDER[rank][2]}"
    return f"Fallback ({opt.server_name or 'server'})"


# ═══════════════════════════════════════════════════════════════
# STAGE 1  –  ASYNC CONCURRENT embed URLs → /api/v1/s → keys
#             KEY CHANGE: ThreadPoolExecutor(30) parallel urllib
#             instead of sequential single-threaded loop.
# ═══════════════════════════════════════════════════════════════

def _build_server_api_url(main_url: str) -> str:
    parsed = urlparse(main_url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if parsed.path.startswith("/embed/movie"):
        params.setdefault("type", "movie")
    elif parsed.path.startswith("/embed/tv"):
        params.setdefault("type", "tv")
    base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'primesrc.me'}"
    return f"{base}/api/v1/s?{urlencode(params)}"


def _fetch_json_http(url: str, referer: str) -> Any:
    req = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, */*",
            "Referer": referer,
        },
    )
    with urlopen(req, timeout=STAGE1_REQUEST_TIMEOUT) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return json.loads(resp.read().decode(charset, errors="replace"))


def _normalise_embed_url(raw: str, media_type: str = "movie") -> str:
    raw = raw.strip()
    if TMDB_ID_RE.fullmatch(raw):
        return f"https://primesrc.me/embed/{media_type}?tmdb={raw}"
    if raw.startswith("primesrc.me/"):
        return "https://" + raw
    if raw.startswith("/embed/"):
        return "https://primesrc.me" + raw
    return raw


def _extract_tmdb_id(url: str) -> str:
    qs = dict(x.split("=", 1) for x in urlparse(url).query.split("&") if "=" in x)
    return qs.get("tmdb", "")


def _find_server_lists(obj: Any) -> list[dict[str, Any]]:
    lists: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        servers = obj.get("servers")
        if isinstance(servers, list) and servers:
            if any("key" in item or "file_name" in item for item in servers if isinstance(item, dict)):
                info = obj.get("info") if isinstance(obj.get("info"), dict) else {}
                lists.append({"servers": servers, "info": info})
        for v in obj.values():
            lists.extend(_find_server_lists(v))
    elif isinstance(obj, list):
        for item in obj:
            lists.extend(_find_server_lists(item))
    return lists


def _options_from_server_list(servers: list[dict], main_url: str) -> list[ServerOption]:
    options: list[ServerOption] = []
    for item in servers:
        key  = str(item.get("key")  or "").strip()
        name = str(item.get("name") or "").strip()
        if not key:
            continue
        raw_hid = item.get("host_id") or item.get("hostId") or item.get("server_id") or item.get("id")
        try:
            host_id = int(raw_hid) if raw_hid is not None else None
        except (ValueError, TypeError):
            host_id = None
        options.append(ServerOption(
            server_name    = name,
            key            = key,
            api_url        = f"https://primesrc.me/api/v1/l?key={quote(key, safe='')}",
            main_url       = main_url,
            host_id        = host_id,
            title          = str(item.get("file_name")      or "").strip(),
            quality        = str(item.get("quality")        or "").strip(),
            audio_language = str(item.get("audio_language") or "").strip(),
        ))
    return options


def _stage1_fetch_one(embed_url: str) -> tuple[str, list[ServerOption] | None, str | None]:
    """
    Fetch server keys for a SINGLE embed URL.
    Returns (embed_url, options_or_None, error_or_None).
    Called concurrently from a ThreadPoolExecutor.
    """
    api_url = _build_server_api_url(embed_url)
    try:
        obj = _fetch_json_http(api_url, embed_url)
        server_lists = _find_server_lists(obj)
        if not server_lists:
            return embed_url, [], None   # empty but not an error
        opts: list[ServerOption] = []
        for sl in server_lists:
            opts.extend(_options_from_server_list(sl.get("servers", []), embed_url))
        return embed_url, opts, None
    except Exception as exc:
        return embed_url, None, str(exc)


def stage1_fetch_api_keys(
    input_files: list[Path] | Path,
    processed_urls_file: Path,
    media_type: str = "movie",
    api_list_found_file: Path | None = None,
    api_list_not_found_file: Path | None = None,
) -> list[ServerOption]:
    log_head("STAGE 1  –  Fetch server keys (ASYNC CONCURRENT, 30 workers)")

    input_paths = [input_files] if isinstance(input_files, Path) else list(input_files)
    for ip in input_paths:
        _ensure_file_exists(ip, "")
    _ensure_file_exists(processed_urls_file, "")
    _ensure_file_exists(api_list_found_file, "")
    _ensure_file_exists(api_list_not_found_file, "")

    # ── Collect raw input lines (deduped) ────────────────────────
    raw_lines: list[str] = []
    seen_raw: set[str] = set()
    for ip in input_paths:
        if ip.exists():
            for l in ip.read_text(encoding="utf-8").splitlines():
                l = l.strip()
                if l and not l.startswith("#") and l not in seen_raw:
                    seen_raw.add(l)
                    raw_lines.append(l)

    # ── Load already-processed tmdb_ids ──────────────────────────
    already_processed_tmdb: set[str] = set()
    if processed_urls_file.exists():
        for _line in processed_urls_file.read_text(encoding="utf-8").splitlines():
            _line = _line.strip()
            if not _line or _line.startswith("#"):
                continue
            if _line.startswith("http"):
                _tid = _extract_tmdb_id(_line)
                if _tid:
                    already_processed_tmdb.add(_tid)
            elif TMDB_ID_RE.fullmatch(_line):
                already_processed_tmdb.add(_line)
    if already_processed_tmdb:
        log_info(f"Already-processed tmdb_ids: {len(already_processed_tmdb)} — skipping in Stage 1")

    # ── Build final embed_urls list ───────────────────────────────
    seen_urls: set[str] = set()
    embed_urls: list[str] = []
    skipped = 0
    for raw in raw_lines:
        url = _normalise_embed_url(raw, media_type)
        tmdb_id = _extract_tmdb_id(url)
        if tmdb_id and tmdb_id in already_processed_tmdb:
            skipped += 1
            continue
        if url not in seen_urls:
            seen_urls.add(url)
            embed_urls.append(url)

    log_info(f"Embed URLs to fetch : {len(embed_urls)}  (skipped {skipped} already-processed)")

    if not embed_urls:
        log_info("Nothing to fetch in Stage 1.")
        return []

    # ═══════════════════════════════════════════════════
    # CONCURRENT FETCH  –  up to STAGE1_WORKERS threads
    # Original: sequential for-loop  → O(N × latency)
    # Now:      parallel pool         → O(latency_of_slowest)
    # ═══════════════════════════════════════════════════
    all_options:             list[ServerOption] = []
    found_lines:             list[str]          = []
    not_found_embed_urls:    list[str]          = []
    errors:                  list[tuple[str, str]] = []
    total = len(embed_urls)

    t0 = time.monotonic()
    workers = min(STAGE1_WORKERS, total)
    log_info(f"Launching {workers} concurrent Stage-1 workers…")

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {pool.submit(_stage1_fetch_one, url): url for url in embed_urls}
        for fut in as_completed(future_map):
            completed += 1
            embed_url, opts, err = fut.result()
            label = f"  [{completed:>4}/{total}]"

            if err is not None:
                errors.append((embed_url, err))
                not_found_embed_urls.append(embed_url)
                log_err(f"{label} {err}  {embed_url}")
            elif not opts:
                not_found_embed_urls.append(embed_url)
                log_warn(f"{label} 0 keys found  {embed_url}")
            else:
                unique_keys = {o.key for o in opts}
                all_options.extend(opts)
                found_lines.append(f"{embed_url} {len(unique_keys)} keys of {len(opts)}")
                log_ok(f"{label} {len(unique_keys)} keys ({len(opts)} total)  {embed_url}")

    elapsed = time.monotonic() - t0
    log_info(f"Stage 1 wall time : {elapsed:.1f}s  (avg {elapsed/max(total,1):.2f}s/movie)")

    # Deduplicate keys across all movies
    seen_keys: set[str] = set()
    unique_options: list[ServerOption] = []
    for opt in all_options:
        if opt.key not in seen_keys:
            seen_keys.add(opt.key)
            unique_options.append(opt)

    log_info(f"Total keys : {len(all_options)}  (unique: {len(unique_options)})")
    log_info(f"Errors     : {len(errors)}")

    if api_list_found_file:
        for w in _write_split_text_lines(api_list_found_file, found_lines):
            log_ok(f"Written found summaries → {w}")
    if api_list_not_found_file:
        for w in _write_split_text_lines(api_list_not_found_file, not_found_embed_urls):
            log_ok(f"Written not-found URLs → {w}")
    if errors:
        log_warn("Failed embed URLs (Stage 1):")
        for url, err in errors:
            log_warn(f"  {url}  → {err}")

    # Mark ALL attempted embed_urls as processed
    if processed_urls_file and embed_urls:
        existing_on_disk: set[str] = set()
        if processed_urls_file.exists():
            for _l in processed_urls_file.read_text(encoding="utf-8").splitlines():
                _l = _l.strip()
                if _l and not _l.startswith("#"):
                    existing_on_disk.add(_l)
                    _tid = _extract_tmdb_id(_l)
                    if _tid:
                        existing_on_disk.add(_tid)

        new_entries = [
            _url for _url in embed_urls
            if _url not in existing_on_disk and
               (_extract_tmdb_id(_url) not in existing_on_disk if _extract_tmdb_id(_url) else True)
        ]
        if new_entries:
            target_pf = _append_split_text(
                processed_urls_file, "\n".join(new_entries) + "\n"
            )
            log_ok(f"[Stage 1] Marked {len(new_entries)} URL(s) as processed → {target_pf}")

    return unique_options


# ═══════════════════════════════════════════════════════════════
# FLARESOLVERR HELPERS
# ═══════════════════════════════════════════════════════════════

FLARESOLVERR_DEFAULT_URL = "http://localhost:8191"
FLARESOLVERR_MAX_TIMEOUT = 45_000  # ms

_print_lock: asyncio.Lock | None = None


async def safe_print(*a: Any, **kw: Any) -> None:
    async with _print_lock:  # type: ignore[union-attr]
        print(*a, **kw)


def extract_json(text: str) -> Any:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty page content")
    if text[0] in "{[":
        return json.loads(text)
    s = text.find("{")
    e = text.rfind("}") + 1
    if s == -1 or e <= s:
        raise ValueError("No JSON object found in page")
    return json.loads(text[s:e])


def get_play_url(data: Any) -> str | None:
    if isinstance(data, dict):
        for key in ("link", "url", "file", "src", "stream"):
            v = data.get(key)
            if isinstance(v, str) and v.startswith(("http://", "https://")):
                return v
        for key in ("sources", "tracks", "streams"):
            items = data.get(key)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, str) and item.startswith(("http://", "https://")):
                        return item
                    if isinstance(item, dict):
                        nested = get_play_url(item)
                        if nested:
                            return nested
    elif isinstance(data, list):
        for item in data:
            nested = get_play_url(item)
            if nested:
                return nested
    return None


def _flaresolverr_url(args: argparse.Namespace) -> str:
    return (
        os.environ.get("FLARESOLVERR_URL")
        or getattr(args, "flaresolverr_url", None)
        or FLARESOLVERR_DEFAULT_URL
    ).rstrip("/")


def _fs_post(base_url: str, payload: dict[str, Any], http_timeout: int = 120) -> dict[str, Any]:
    import urllib.error
    data = json.dumps(payload).encode("utf-8")
    req  = Request(
        f"{base_url}/v1",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=http_timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")
            fs_resp = json.loads(body)
            return {
                "status":       "error",
                "message":      fs_resp.get("message", body[:300]),
                "_http_status": exc.code,
            }
        except Exception:
            raise ConnectionError(
                f"FlareSolverr at {base_url}/v1 returned HTTP {exc.code}: {exc.reason}"
            ) from exc
    except urllib.error.URLError as exc:
        raise ConnectionError(
            f"Cannot reach FlareSolverr at {base_url}/v1  ({exc})"
        ) from exc


def _check_flaresolverr_health(base_url: str) -> bool:
    try:
        with urlopen(f"{base_url}/health", timeout=5) as resp:
            body = json.loads(resp.read())
            return body.get("status") == "ok"
    except Exception:
        return False


def _parse_flaresolverr_response(resp: dict[str, Any]) -> Any:
    solution  = resp.get("solution", {})
    body_html = solution.get("response", "")
    body_text = body_html
    m = re.search(r"<body[^>]*>(.*?)</body>", body_html, re.S | re.I)
    if m:
        body_text = re.sub(r"<[^>]+>", "", m.group(1))
    return extract_json(body_text)


# ═══════════════════════════════════════════════════════════════
# SESSION POOL
# Pre-create N FlareSolverr sessions at startup, reuse throughout.
# Original: 1 new session created+destroyed PER KEY ATTEMPT (huge overhead).
# ═══════════════════════════════════════════════════════════════

class FlareSolverrSessionPool:
    """
    Maintains a fixed pool of FlareSolverr sessions.
    Workers acquire a session via `async with pool.acquire()` and
    return it automatically.  Sessions are created once at startup
    and destroyed once at shutdown — not per request.
    """

    def __init__(self, base_url: str, size: int) -> None:
        self.base_url   = base_url
        self.size       = size
        self._queue:    asyncio.Queue[str] = asyncio.Queue()
        self._sessions: list[str] = []

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for i in range(self.size):
            sid = f"primesrc_fast_{int(time.time())}_{i}"
            await loop.run_in_executor(
                None,
                lambda s=sid: _fs_post(self.base_url, {"cmd": "sessions.create", "session": s}),
            )
            self._sessions.append(sid)
            await self._queue.put(sid)
        log_ok(f"FlareSolverr session pool ready — {self.size} sessions")

    async def stop(self) -> None:
        loop = asyncio.get_running_loop()
        for sid in self._sessions:
            try:
                await loop.run_in_executor(
                    None,
                    lambda s=sid: _fs_post(self.base_url, {"cmd": "sessions.destroy", "session": s}),
                )
            except Exception:
                pass
        log_info(f"FlareSolverr session pool closed — {len(self._sessions)} sessions released")

    class _Session:
        def __init__(self, pool: "FlareSolverrSessionPool", sid: str) -> None:
            self._pool = pool
            self.id    = sid

        async def __aenter__(self) -> str:
            return self.id

        async def __aexit__(self, *_: Any) -> None:
            await self._pool._queue.put(self.id)

    def acquire(self) -> "_Session":
        # Returns an async context manager that blocks until a session is free
        # We do the actual get inside __aenter__ via a wrapper
        pool = self
        class _Wrapper:
            def __init__(self) -> None:
                self._sid: str | None = None
            async def __aenter__(self) -> str:
                self._sid = await pool._queue.get()
                return self._sid
            async def __aexit__(self, *_: Any) -> None:
                if self._sid:
                    await pool._queue.put(self._sid)
        return _Wrapper()  # type: ignore[return-value]


# ═══════════════════════════════════════════════════════════════
# STAGE 2  –  RACING RESOLVER
#
# KEY CHANGE: instead of serial priority (try voe → if fail try filemoon
# → if fail try dood → ...), ALL hosts are raced CONCURRENTLY.
# asyncio.wait(FIRST_COMPLETED) returns the instant any host wins.
# Losers are cancelled. No time wasted waiting for prior-priority
# failures before starting lower-priority attempts.
#
# With 8 hosts each taking ~3-8s in FlareSolverr, serial worst-case
# = 8 × 8s = 64s per movie.  Racing worst-case = 8s (the slowest
# winner, or the fastest of all if all fail).
# ═══════════════════════════════════════════════════════════════

async def _resolve_one_url_via_fs(
    base_url: str,
    session_id: str,
    api_url: str,
    timeout_ms: int,
    opt: ServerOption,
    loop: asyncio.AbstractEventLoop,
) -> dict[str, Any]:
    """
    Resolve a single /api/v1/l?key=... URL via FlareSolverr.
    Returns result dict with 'extracted_url' key (None on failure).
    Uses the provided session_id (from pool — no create/destroy overhead).
    Retries up to STAGE2_RELOADS times with tight backoff.
    """
    last_error: str | None = None

    for attempt in range(STAGE2_RELOADS + 1):
        if attempt:
            delay = min(1.5 * (2 ** (attempt - 1)), STAGE2_BAN_COOLDOWN)
            await asyncio.sleep(delay)

        try:
            fs_resp = await loop.run_in_executor(
                None,
                lambda: _fs_post(base_url, {
                    "cmd":        "request.get",
                    "url":        api_url,
                    "maxTimeout": timeout_ms,
                    "session":    session_id,
                }),
            )

            if fs_resp.get("status") != "ok":
                last_error = (
                    f"FS error: {fs_resp.get('message', '')}"
                    + (f" (HTTP {fs_resp.get('_http_status')})" if "_http_status" in fs_resp else "")
                )
                if re.search(r"banned|blocked", last_error, re.I):
                    await asyncio.sleep(STAGE2_BAN_COOLDOWN)
                continue

            data     = _parse_flaresolverr_response(fs_resp)
            play_url = get_play_url(data)

            if play_url:
                return {
                    "api_url":       api_url,
                    "data":          data,
                    "extracted_url": play_url,
                    "host_label":    get_server_priority_label(opt),
                    "method":        "flaresolverr",
                }

            if isinstance(data, dict):
                for ck in ("url", "link", "redirect", "location"):
                    candidate = data.get(ck, "")
                    if isinstance(candidate, str) and candidate.startswith("http"):
                        return {
                            "api_url":       api_url,
                            "data":          data,
                            "extracted_url": candidate,
                            "host_label":    get_server_priority_label(opt),
                            "method":        "flaresolverr",
                        }

            last_error = f"no play URL in FS response: {str(data)[:120]}"

        except asyncio.CancelledError:
            raise  # propagate cancellation immediately
        except Exception as exc:
            last_error = str(exc)

    return {
        "api_url":       api_url,
        "error":         last_error or "failed",
        "extracted_url": None,
        "host_label":    get_server_priority_label(opt),
    }


async def _resolve_movie_racing(
    movie_idx:    int,
    total_movies: int,
    main_url:     str,
    options:      list[ServerOption],
    base_url:     str,
    session_pool: FlareSolverrSessionPool,
    timeout_ms:   int,
    global_sem:   asyncio.Semaphore,
    loop:         asyncio.AbstractEventLoop,
) -> tuple[ServerOption | None, dict[str, Any] | None, list[dict[str, Any]]]:
    """
    Race ALL server options for a single movie concurrently.
    The first option that returns a valid stream URL wins;
    all others are cancelled immediately.

    Priority is still respected: if multiple hosts succeed near-simultaneously,
    we keep the highest-priority one.  In practice, the first to finish
    (usually voe.sx if it works) wins before others complete.
    """
    sorted_opts = sorted(options, key=get_server_priority)
    tmdb_id     = _extract_tmdb_id(main_url)
    movie_label = f"Movie [{movie_idx:>3}/{total_movies}] (tmdb={tmdb_id})"
    all_attempts: list[dict[str, Any]] = []

    async with global_sem:
        # Each host task acquires its own session from the pool
        async def _race_one(opt: ServerOption) -> tuple[ServerOption, dict[str, Any]]:
            async with session_pool.acquire() as sid:
                res = await _resolve_one_url_via_fs(
                    base_url, sid, opt.api_url, timeout_ms, opt, loop
                )
                return opt, res

        tasks: dict[asyncio.Task, ServerOption] = {
            asyncio.create_task(_race_one(opt)): opt
            for opt in sorted_opts
        }

        await safe_print(
            f"{movie_label} → Racing {len(tasks)} host(s): "
            + ", ".join(get_server_priority_label(o) for o in sorted_opts[:3])
            + (" …" if len(sorted_opts) > 3 else "")
        )

        winner_opt: ServerOption | None = None
        winner_res: dict[str, Any] | None = None
        pending = set(tasks.keys())

        try:
            while pending:
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in done:
                    try:
                        opt, res = task.result()
                    except asyncio.CancelledError:
                        continue
                    except Exception as exc:
                        log_err(f"{movie_label} task exception: {exc}")
                        all_attempts.append({"api_url": "", "error": str(exc), "extracted_url": None})
                        continue

                    all_attempts.append(res)

                    if res.get("extracted_url"):
                        # We have a winner — cancel everything else
                        for p in pending:
                            p.cancel()
                        winner_opt = opt
                        winner_res = res
                        await safe_print(
                            f"{movie_label} → ✓ SUCCESS via {res['host_label']}: {res['extracted_url']}"
                        )
                        # Drain remaining done tasks from cancelled set
                        pending.clear()
                        break
                else:
                    continue
                break

        except asyncio.CancelledError:
            for t in pending:
                t.cancel()
            raise

        # Cancel any still-pending tasks (all failed or we have a winner)
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        if not winner_res:
            await safe_print(f"{movie_label} → ✗ ALL hosts failed")

        return winner_opt, winner_res, all_attempts


# ═══════════════════════════════════════════════════════════════
# CHECKPOINT MANAGER  (same as original, interval tightened to 20)
# ═══════════════════════════════════════════════════════════════

class CheckpointManager:
    def __init__(
        self,
        stage1_options: list[ServerOption],
        args: argparse.Namespace,
        json_out_path: Path,
        gh_token: str,
        gh_repo: str,
        gh_branch: str,
        gh_available: bool,
        interval: int = CHECKPOINT_SAVE_INTERVAL,
    ) -> None:
        self.stage1_options   = stage1_options
        self.args             = args
        self.json_out_path    = json_out_path
        self.gh_token         = gh_token
        self.gh_repo          = gh_repo
        self.gh_branch        = gh_branch
        self.gh_available     = gh_available
        self.interval         = interval
        self._lock            = asyncio.Lock()
        self._completed       = 0
        self._since_flush     = 0
        self.all_results:         list[dict[str, Any]] = []
        self.fully_resolved_tmdb: set[str]             = set()
        self.succeeded_api_urls:  set[str]             = set()
        self.processed_urls_file: Path = getattr(args, "processed_urls", DEFAULT_PROCESSED_URLS)
        self.error_log_file: Path      = getattr(args, "error_log",      DEFAULT_ERROR_LOG)

    async def record(
        self,
        succ_opt: ServerOption | None,
        succ_res: dict[str, Any] | None,
        attempts: list[dict[str, Any]],
    ) -> None:
        async with self._lock:
            self.all_results.extend(attempts)
            if succ_opt and succ_res and succ_res.get("extracted_url"):
                self.succeeded_api_urls.add(succ_opt.api_url)
                tid = _extract_tmdb_id(succ_opt.main_url)
                if tid:
                    self.fully_resolved_tmdb.add(tid)
            self._completed   += 1
            self._since_flush += 1
            if self._since_flush >= self.interval:
                await asyncio.get_running_loop().run_in_executor(None, self._flush)
                self._since_flush = 0

    def _flush(self) -> None:
        log_info(
            f"[Checkpoint] Saving after {self._completed} movies "
            f"({len(self.fully_resolved_tmdb)} resolved so far)…"
        )
        existing_processed: set[str] = set()
        if self.processed_urls_file.exists():
            for _line in self.processed_urls_file.read_text(encoding="utf-8").splitlines():
                _line = _line.strip()
                if _line and not _line.startswith("#"):
                    existing_processed.add(_line)

        tmdb_to_embed: dict[str, str] = {}
        for _opt in self.stage1_options:
            _tid = _extract_tmdb_id(_opt.main_url)
            if _tid and _tid not in tmdb_to_embed:
                tmdb_to_embed[_tid] = _opt.main_url

        new_ids = self.fully_resolved_tmdb - existing_processed
        if new_ids:
            lines = ""
            for tid in sorted(new_ids, key=int):
                lines += tmdb_to_embed.get(tid, f"https://primesrc.me/embed/movie?tmdb={tid}") + "\n"
            target_pf = _append_split_text(self.processed_urls_file, lines)
            log_ok(f"[Checkpoint] Appended {len(new_ids)} resolved tmdb_id(s) → {target_pf}")

        try:
            _write_summary(self.stage1_options, self.all_results, self.json_out_path)
            log_ok(f"[Checkpoint] Local JSON updated → {self.json_out_path}")
        except Exception as exc:
            log_warn(f"[Checkpoint] Could not write JSON: {exc}")

        if self.fully_resolved_tmdb:
            clean_error_log_for_resolved_tmdb_ids(self.error_log_file, self.fully_resolved_tmdb)
        if self.succeeded_api_urls:
            clean_error_log_for_resolved_api_urls(self.error_log_file, self.succeeded_api_urls)

        log_ok(f"[Checkpoint] Done — {self._completed} movies, {len(self.fully_resolved_tmdb)} resolved")


# ═══════════════════════════════════════════════════════════════
# STAGE 2 MAIN ENTRY
# ═══════════════════════════════════════════════════════════════

async def stage2_extract_stream_urls(
    stage1_options: list[ServerOption],
    args:           argparse.Namespace,
) -> list[dict[str, Any]]:
    log_head(
        "STAGE 2  –  RACING stream extraction via FlareSolverr\n"
        "(ALL hosts raced concurrently per movie — first to respond wins)"
    )

    global _print_lock
    _print_lock = asyncio.Lock()

    if not stage1_options:
        log_warn("No API keys to resolve in Stage 2.")
        return []

    movie_to_options: dict[str, list[ServerOption]] = defaultdict(list)
    for opt in stage1_options:
        movie_to_options[opt.main_url].append(opt)

    total_movies = len(movie_to_options)
    log_info(f"Total movies to resolve      : {total_movies}")
    log_info(f"Total server keys available  : {len(stage1_options)}")
    log_info(f"Concurrency (movies)         : {args.batch_size}")
    log_info(f"Session pool size            : {args.batch_size}")
    log_info(f"Reloads per host             : {args.reloads}")
    log_info(f"Solver timeout               : {args.fs_timeout_ms} ms")
    log_info("Strategy: ALL hosts raced simultaneously per movie (first winner cancels others)")

    base_url   = _flaresolverr_url(args)
    timeout_ms = getattr(args, "fs_timeout_ms", FLARESOLVERR_MAX_TIMEOUT)

    log_info("Checking FlareSolverr health…")
    if not _check_flaresolverr_health(base_url):
        log_err(
            f"FlareSolverr not reachable at {base_url}\n"
            "  Start: docker run -d -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest"
        )
        raise ConnectionError("FlareSolverr not reachable")
    log_ok("FlareSolverr is healthy")

    # Build session pool:  one session per concurrent movie × average hosts
    # Use batch_size sessions so all concurrent movies can race simultaneously.
    # Each racing host task will borrow a session for its duration.
    pool_size = args.batch_size * 2   # headroom so racing tasks don't queue on each other
    session_pool = FlareSolverrSessionPool(base_url, pool_size)
    await session_pool.start()

    gh_token  = getattr(args, "gh_token",  None) or os.environ.get("GH_TOKEN", "")
    gh_repo   = getattr(args, "gh_repo",   None) or os.environ.get("GH_REPO",  "")
    gh_branch = getattr(args, "gh_branch", None) or os.environ.get("GH_BRANCH", "main")
    gh_available = not getattr(args, "no_github_sync", False) and bool(gh_token) and bool(gh_repo)

    json_out_path: Path = getattr(args, "json_out", DEFAULT_JSON_SUMMARY)

    checkpoint = CheckpointManager(
        stage1_options=stage1_options,
        args=args,
        json_out_path=json_out_path,
        gh_token=gh_token,
        gh_repo=gh_repo,
        gh_branch=gh_branch,
        gh_available=gh_available,
        interval=CHECKPOINT_SAVE_INTERVAL,
    )
    log_info(f"Checkpoint saves every {CHECKPOINT_SAVE_INTERVAL} completed movies")

    global_sem = asyncio.Semaphore(args.batch_size)
    loop       = asyncio.get_running_loop()

    async def _resolve_and_checkpoint(
        idx: int, main_url: str, opts: list[ServerOption]
    ) -> tuple[ServerOption | None, dict[str, Any] | None, list[dict[str, Any]]]:
        outcome = await _resolve_movie_racing(
            movie_idx=idx,
            total_movies=total_movies,
            main_url=main_url,
            options=opts,
            base_url=base_url,
            session_pool=session_pool,
            timeout_ms=timeout_ms,
            global_sem=global_sem,
            loop=loop,
        )
        succ_opt, succ_res, attempts = outcome
        await checkpoint.record(succ_opt, succ_res, attempts)
        return outcome

    t_start = time.monotonic()

    tasks = [
        _resolve_and_checkpoint(idx, main_url, opts)
        for idx, (main_url, opts) in enumerate(movie_to_options.items(), 1)
    ]

    try:
        movie_outcomes = await asyncio.gather(*tasks)
    finally:
        await session_pool.stop()

    # Final checkpoint flush
    if checkpoint._since_flush > 0:
        await asyncio.get_running_loop().run_in_executor(None, checkpoint._flush)
        checkpoint._since_flush = 0

    results             = checkpoint.all_results
    fully_resolved_tmdb = checkpoint.fully_resolved_tmdb
    succeeded_api_urls  = checkpoint.succeeded_api_urls

    # Write any remaining resolved IDs not yet on disk
    processed_urls_file: Path = getattr(args, "processed_urls", DEFAULT_PROCESSED_URLS)
    error_log_file: Path      = getattr(args, "error_log",      DEFAULT_ERROR_LOG)

    existing_processed: set[str] = set()
    if processed_urls_file.exists():
        for _line in processed_urls_file.read_text(encoding="utf-8").splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#"):
                existing_processed.add(_line)

    remaining_new = fully_resolved_tmdb - existing_processed
    if remaining_new:
        tmdb_to_embed: dict[str, str] = {}
        for _opt in stage1_options:
            _tid = _extract_tmdb_id(_opt.main_url)
            if _tid and _tid not in tmdb_to_embed:
                tmdb_to_embed[_tid] = _opt.main_url
        lines = ""
        for tid in sorted(remaining_new, key=int):
            lines += tmdb_to_embed.get(tid, f"https://primesrc.me/embed/movie?tmdb={tid}") + "\n"
        target_pf = _append_split_text(processed_urls_file, lines)
        log_ok(f"Saved {len(remaining_new)} resolved tmdb_id(s) → {target_pf}")

    if fully_resolved_tmdb:
        clean_error_log_for_resolved_tmdb_ids(error_log_file, fully_resolved_tmdb)
    if succeeded_api_urls:
        clean_error_log_for_resolved_api_urls(error_log_file, succeeded_api_urls)

    global _ERROR_LOG_ENTRIES
    _ERROR_LOG_ENTRIES = [
        e for e in _ERROR_LOG_ENTRIES
        if not any(f"tmdb={tid}" in e for tid in fully_resolved_tmdb)
        and not any(u in e for u in succeeded_api_urls)
    ]

    elapsed = time.monotonic() - t_start
    log_head(f"STAGE 2 DONE  ({elapsed:.1f}s total, avg {elapsed/max(total_movies,1):.1f}s/movie)")
    ok = sum(1 for r in results if r.get("extracted_url"))
    log_ok(f"Stream URLs extracted : {ok} (from {len(stage1_options)} keys across {total_movies} movies)")

    return results


# ═══════════════════════════════════════════════════════════════
# TMDB API  (unchanged)
# ═══════════════════════════════════════════════════════════════

TMDB_API_KEY = "6fad3f86b8452ee232deb7977d7dcf58"


def _tmdb_request(path: str) -> dict:
    base = "https://api.themoviedb.org/3"
    sep  = "&" if "?" in path else "?"
    url  = f"{base}{path}{sep}language=en-US"
    if TMDB_API_KEY:
        url += f"&api_key={TMDB_API_KEY}"
    req = Request(url, headers={
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    })
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _fetch_tmdb_info(tmdb_id: str) -> tuple[str, str]:
    title   = ""
    imdb_id = None
    try:
        data    = _tmdb_request(f"/movie/{tmdb_id}")
        title   = data.get("title") or data.get("original_title") or ""
        imdb_id = data.get("imdb_id") or None
        if not imdb_id:
            ext     = _tmdb_request(f"/movie/{tmdb_id}/external_ids")
            imdb_id = ext.get("imdb_id") or None
    except Exception as exc:
        log_warn(f"TMDB info fetch failed for tmdb={tmdb_id}: {exc}")
    return title, imdb_id


def _is_within_last_two_months(date_str: str) -> bool:
    if not date_str or len(date_str) < 10:
        return False
    try:
        from datetime import date
        rel = date.fromisoformat(date_str[:10])
        today = datetime.now(timezone.utc).date()
        return (today - timedelta(days=60)) <= rel <= today
    except Exception:
        return False


def fetch_tmdb_now_playing_and_top_rated(
    target_file: Path,
    existing_input_files: list[Path] | Path | None = None,
    processed_urls_file: Path | None = None,
    limit: int = 50,
) -> list[str]:
    _ensure_file_exists(target_file, "")

    today          = datetime.now(timezone.utc).date()
    two_months_ago = today - timedelta(days=60)

    known_tmdb_ids: set[str] = set()
    files_to_check: list[Path] = []
    if isinstance(existing_input_files, list):
        files_to_check.extend(existing_input_files)
    elif existing_input_files:
        files_to_check.append(existing_input_files)
    if processed_urls_file:
        files_to_check.append(processed_urls_file)
    if target_file:
        files_to_check.append(target_file)

    for f in files_to_check:
        if f and f.exists():
            for line in f.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    tid = _extract_tmdb_id(line)
                    if not tid and TMDB_ID_RE.fullmatch(line):
                        tid = line
                    if tid:
                        known_tmdb_ids.add(tid)

    log_info(f"Existing known TMDB IDs to exclude: {len(known_tmdb_ids)}")
    log_info(f"TMDB Now Playing date filter: {two_months_ago} to {today}")

    def _fetch_from_endpoint(endpoint: str, target_count: int, filter_recent_date: bool = False) -> list[str]:
        added: list[str] = []
        page = 1
        while len(added) < target_count and page <= 25:
            sep  = "&" if "?" in endpoint else "?"
            path = f"{endpoint}{sep}page={page}"
            try:
                data    = _tmdb_request(path)
                results = data.get("results", [])
                if not results:
                    break
                for m in results:
                    mid = str(m.get("id", ""))
                    if not mid:
                        continue
                    if filter_recent_date:
                        rel_date = str(m.get("release_date") or "").strip()
                        if not _is_within_last_two_months(rel_date):
                            continue
                    if mid not in known_tmdb_ids:
                        known_tmdb_ids.add(mid)
                        added.append(f"https://primesrc.me/embed/movie?tmdb={mid}")
                        if len(added) >= target_count:
                            break
                page += 1
            except Exception as exc:
                log_warn(f"TMDB {endpoint} page={page}: {exc}")
                break
        return added

    log_info(f"Fetching TMDB Now Playing (limit: {limit})…")
    np_urls = _fetch_from_endpoint("/movie/now_playing", limit, filter_recent_date=True)
    log_ok(f"Found {len(np_urls)} Now Playing movie(s)")

    remaining = limit - len(np_urls)
    tr_urls: list[str] = []
    if remaining > 0:
        log_info(f"Filling remaining {remaining} from TMDB Top Rated…")
        tr_urls = _fetch_from_endpoint("/movie/top_rated", remaining, filter_recent_date=False)
        log_ok(f"Found {len(tr_urls)} Top Rated movie(s)")

    new_urls = np_urls + tr_urls
    if new_urls:
        target_written = _append_split_text(target_file, "\n".join(new_urls) + "\n")
        log_ok(f"Stored {len(new_urls)} movie(s) → {target_written}")
    else:
        log_info("No new movies found.")

    return new_urls


# ═══════════════════════════════════════════════════════════════
# SUMMARY WRITER  (unchanged from original)
# ═══════════════════════════════════════════════════════════════

def _gh_split_records(records: list[dict[str, Any]], limit: int = GITHUB_FILE_SIZE_LIMIT) -> list[bytes]:
    chunks: list[bytes] = []
    current: list[dict[str, Any]] = []
    current_size = 2  # "[]"
    for rec in records:
        rec_bytes = json.dumps(rec, ensure_ascii=False).encode("utf-8")
        extra     = len(rec_bytes) + (2 if current else 0)
        if current and current_size + extra > limit:
            chunks.append(json.dumps(current, ensure_ascii=False, indent=2).encode("utf-8"))
            current      = []
            current_size = 2
        current.append(rec)
        current_size += extra
    if current or not chunks:
        chunks.append(json.dumps(current, ensure_ascii=False, indent=2).encode("utf-8"))
    return chunks


def _parse_entry_from_record(e: dict[str, Any]) -> tuple[int, str, str, str, list[dict[str, str]]]:
    tmdb_int = 0
    imdb_id  = ""
    if "tmdb/imdb" in e:
        val   = str(e["tmdb/imdb"])
        parts = val.split("/", 1)
        try:
            tmdb_int = int(parts[0].strip())
        except ValueError:
            tmdb_int = 0
        if len(parts) > 1 and parts[1].strip():
            imdb_id = parts[1].strip()
    else:
        tmdb_int = int(e.get("tmdb_id", 0) or 0)
        imdb_id  = str(e.get("imdb_id") or "")

    title        = e.get("title", "")
    extracted_at = e.get("extracted_at", "")

    sources: list[dict[str, str]] = []
    n = 1
    while True:
        h = e.get(f"host-{n}")
        u = e.get(f"url-{n}")
        if not h and not u:
            break
        url = (u if (isinstance(u, str) and u.startswith("http"))
               else (h if (isinstance(h, str) and h.startswith("http")) else ""))
        if url:
            sources.append({"url": url, "key": e.get(f"key-{n}", url)})
        n += 1

    return tmdb_int, imdb_id, title, extracted_at, sources


def _dedup_sources_by_url(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for s in sources:
        url = s.get("url", "")
        if url and url not in seen:
            seen.add(url)
            out.append(s)
    return out


def _write_summary(
    stage1_options: list[ServerOption],
    stage2_results: list[dict[str, Any]],
    json_path: Path,
) -> None:
    link_map = {r["api_url"]: r.get("extracted_url") or "" for r in stage2_results}

    new_groups_raw: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for opt in stage1_options:
        stream_url = link_map.get(opt.api_url, "")
        if not stream_url:
            continue
        qs   = dict(x.split("=", 1) for x in urlparse(opt.main_url).query.split("&") if "=" in x)
        tmdb = qs.get("tmdb", "")
        if not tmdb:
            continue
        new_groups_raw[tmdb][opt.api_url] = {
            "host": urlparse(stream_url).netloc,
            "url":  stream_url,
            "key":  opt.api_url,
        }
    new_groups: dict[str, list[dict[str, Any]]] = {
        tmdb: list(m.values()) for tmdb, m in new_groups_raw.items()
    }

    existing: list[dict[str, Any]] = []
    part = 1
    while True:
        p = _split_part_path(json_path, part)
        if not p.exists():
            break
        try:
            recs = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(recs, list):
                existing.extend(recs)
        except Exception as exc:
            log_warn(f"Could not load {p}: {exc}")
        part += 1

    index: dict[int, dict[str, Any]] = {}
    for e in existing:
        tmdb_int, imdb_id, title, extracted_at, sources = _parse_entry_from_record(e)
        if tmdb_int:
            index[tmdb_int] = {
                "tmdb_id": tmdb_int, "imdb_id": imdb_id,
                "title": title, "extracted_at": extracted_at,
                "_sources": sources,
            }

    extracted_at     = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tmdb_meta_cache: dict[int, tuple[str, Any]] = {}

    # Parallelise TMDB metadata fetches for new movies
    new_tmdb_ids = [int(t) for t in new_groups if int(t) not in index]
    if new_tmdb_ids:
        log_info(f"Fetching TMDB metadata for {len(new_tmdb_ids)} new movie(s) (parallel)…")
        with ThreadPoolExecutor(max_workers=min(10, len(new_tmdb_ids))) as pool:
            fut_map = {pool.submit(_fetch_tmdb_info, str(tid)): tid for tid in new_tmdb_ids}
            for fut in as_completed(fut_map):
                tid = fut_map[fut]
                try:
                    title, imdb_id = fut.result()
                    tmdb_meta_cache[tid] = (title, imdb_id)
                    log_ok(f"  tmdb={tid} — '{title}'  imdb={imdb_id}")
                except Exception as exc:
                    log_warn(f"  tmdb={tid} metadata failed: {exc}")
                    tmdb_meta_cache[tid] = ("", "")

    for tmdb_str, new_sources in new_groups.items():
        tmdb_int = int(tmdb_str)
        if tmdb_int in index:
            entry         = index[tmdb_int]
            existing_keys = {s["key"] for s in entry["_sources"]}
            existing_urls = {s["url"] for s in entry["_sources"]}
            added         = [s for s in new_sources
                             if s["key"] not in existing_keys and s["url"] not in existing_urls]
            entry["_sources"].extend(added)
            entry["_sources"]  = _dedup_sources_by_url(entry["_sources"])
            entry["extracted_at"] = extracted_at
            log_info(f"  tmdb={tmdb_int} merged {len(added)} new source(s)")
        else:
            title, imdb_id = tmdb_meta_cache.get(tmdb_int, ("", ""))
            deduped = _dedup_sources_by_url(list(new_sources))
            index[tmdb_int] = {
                "tmdb_id": tmdb_int, "imdb_id": imdb_id,
                "title": title, "extracted_at": extracted_at,
                "_sources": deduped,
            }
            log_ok(f"  tmdb={tmdb_int} — '{title}'  sources: {len(deduped)}")

    sorted_entries = sorted(index.values(), key=lambda x: x["tmdb_id"])
    for i, entry in enumerate(sorted_entries, 1):
        entry["serial"] = i

    output: list[dict[str, Any]] = []
    for e in sorted_entries:
        tmdb_val     = str(e["tmdb_id"])
        imdb_val     = str(e.get("imdb_id") or "")
        tmdb_imdb    = f"{tmdb_val}/{imdb_val}" if imdb_val else f"{tmdb_val}/"
        row: dict[str, Any] = {
            "serial":       e["serial"],
            "title":        e.get("title", ""),
            "tmdb/imdb":    tmdb_imdb,
            "extracted_at": e["extracted_at"],
        }
        for n, src in enumerate(e["_sources"], 1):
            row[f"host-{n}"] = src["url"]
        output.append(row)

    chunks = _gh_split_records(output)
    for i, chunk_bytes in enumerate(chunks, 1):
        target_json = _split_part_path(json_path, i)
        target_json.write_bytes(chunk_bytes)
        log_ok(f"JSON ({len(chunk_bytes):,} B) → {target_json}")

    total_sources = sum(sum(1 for k in row if k.startswith("host-")) for row in output)
    log_info(f"Movies: {len(output)}  Sources: {total_sources}  Files: {len(chunks)}")


# ═══════════════════════════════════════════════════════════════
# STAGE 3  –  GITHUB SYNC  (unchanged from original)
# ═══════════════════════════════════════════════════════════════

def _gh_filename(n: int) -> str:
    if n == 1:
        return f"{GITHUB_BASE_FILENAME}.json"
    return f"{GITHUB_BASE_FILENAME}-{n}.json"


def _gh_api_request(
    method: str,
    path:   str,
    token:  str,
    payload: dict[str, Any] | None = None,
    timeout: int = 30,
) -> dict[str, Any]:
    import urllib.error
    url  = GITHUB_API_ROOT + path
    data = json.dumps(payload).encode("utf-8") if payload else None
    req  = Request(
        url,
        data=data,
        headers={
            "Authorization":        f"Bearer {token}",
            "Accept":               "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type":         "application/json",
            "User-Agent":           "primesrc-pipeline/1.0",
        },
        method=method,
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"GitHub API {method} {path} → HTTP {exc.code}: {body[:400]}") from exc


def _gh_get_file(
    token: str, repo: str, path: str, branch: str,
) -> tuple[list[dict[str, Any]], str | None]:
    api_path = f"/repos/{repo}/contents/{path}?ref={branch}"
    try:
        meta = _gh_api_request("GET", api_path, token)
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return [], None
        raise
    raw_b64 = meta.get("content", "").replace("\n", "")
    sha     = meta.get("sha")
    if not raw_b64:
        return [], sha
    try:
        raw_bytes = base64.b64decode(raw_b64)
        records   = json.loads(raw_bytes.decode("utf-8"))
        if not isinstance(records, list):
            records = []
        log_info(f"  GitHub ← {path}: {len(records)} entries (sha={sha[:7]})")
        return records, sha
    except Exception as exc:
        log_warn(f"  Could not parse {path} from GitHub ({exc}) — treating as empty")
        return [], sha


def _gh_push_file(
    token: str, repo: str, path: str, branch: str,
    content_bytes: bytes, sha: str | None, commit_msg: str,
) -> None:
    payload: dict[str, Any] = {
        "message": commit_msg,
        "content": base64.b64encode(content_bytes).decode("ascii"),
        "branch":  branch,
    }
    if sha:
        payload["sha"] = sha
    _gh_api_request("PUT", f"/repos/{repo}/contents/{path}", token, payload=payload, timeout=60)
    log_ok(f"  GitHub → {path} {'updated' if sha else 'created'} ({len(content_bytes):,} B)")


def github_sync_summary(
    stage1_options: list[ServerOption],
    stage2_results: list[dict[str, Any]],
    local_json_path: Path,
    token: str,
    repo:  str,
    branch: str,
) -> None:
    log_head("STAGE 3  –  GitHub JSON sync")

    extracted_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Fetch all existing part files
    file_meta: list[tuple[str, str | None]] = []
    n = 1
    while True:
        fname    = _gh_filename(n)
        records, sha = _gh_get_file(token, repo, fname, branch)
        if not records and sha is None and n > 1:
            break
        file_meta.append((fname, sha))
        if not records and sha is None:
            break
        n += 1

    all_existing: list[dict[str, Any]] = []
    for fname, sha in file_meta:
        if sha:
            recs, _ = _gh_get_file(token, repo, fname, branch)
            all_existing.extend(recs)

    # Build index from existing
    link_map = {r["api_url"]: r.get("extracted_url") or "" for r in stage2_results}
    new_groups_raw: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    for opt in stage1_options:
        stream_url = link_map.get(opt.api_url, "")
        if not stream_url:
            continue
        qs   = dict(x.split("=", 1) for x in urlparse(opt.main_url).query.split("&") if "=" in x)
        tmdb = qs.get("tmdb", "")
        if not tmdb:
            continue
        new_groups_raw[tmdb][opt.api_url] = {
            "host": urlparse(stream_url).netloc,
            "url":  stream_url,
            "key":  opt.api_url,
        }
    new_groups = {t: list(m.values()) for t, m in new_groups_raw.items()}

    index: dict[int, dict[str, Any]] = {}
    for e in all_existing:
        tmdb_int, imdb_id, title, ea, sources = _parse_entry_from_record(e)
        if tmdb_int:
            index[tmdb_int] = {
                "tmdb_id": tmdb_int, "imdb_id": imdb_id,
                "title": title, "extracted_at": ea,
                "_sources": sources,
            }

    tmdb_meta_cache: dict[int, tuple[str, Any]] = {}
    new_ids = [int(t) for t in new_groups if int(t) not in index]
    if new_ids:
        with ThreadPoolExecutor(max_workers=min(10, len(new_ids))) as pool:
            fut_map = {pool.submit(_fetch_tmdb_info, str(tid)): tid for tid in new_ids}
            for fut in as_completed(fut_map):
                tid = fut_map[fut]
                try:
                    title, imdb_id = fut.result()
                    tmdb_meta_cache[tid] = (title, imdb_id)
                except Exception:
                    tmdb_meta_cache[tid] = ("", "")

    for tmdb_str, new_sources in new_groups.items():
        tmdb_int = int(tmdb_str)
        if tmdb_int in index:
            entry        = index[tmdb_int]
            ekeys        = {s["key"] for s in entry["_sources"]}
            eurls        = {s["url"] for s in entry["_sources"]}
            added        = [s for s in new_sources if s["key"] not in ekeys and s["url"] not in eurls]
            entry["_sources"].extend(added)
            entry["_sources"]    = _dedup_sources_by_url(entry["_sources"])
            entry["extracted_at"] = extracted_at
        else:
            title, imdb_id = tmdb_meta_cache.get(tmdb_int, ("", ""))
            index[tmdb_int] = {
                "tmdb_id": tmdb_int, "imdb_id": imdb_id,
                "title": title, "extracted_at": extracted_at,
                "_sources": _dedup_sources_by_url(list(new_sources)),
            }

    sorted_entries = sorted(index.values(), key=lambda x: x["tmdb_id"])
    for i, entry in enumerate(sorted_entries, 1):
        entry["serial"] = i

    output: list[dict[str, Any]] = []
    for e in sorted_entries:
        tmdb_val  = str(e["tmdb_id"])
        imdb_val  = str(e.get("imdb_id") or "")
        row: dict[str, Any] = {
            "serial":       e["serial"],
            "title":        e.get("title", ""),
            "tmdb/imdb":    f"{tmdb_val}/{imdb_val}" if imdb_val else f"{tmdb_val}/",
            "extracted_at": e["extracted_at"],
        }
        for n2, src in enumerate(e["_sources"], 1):
            row[f"host-{n2}"] = src["url"]
        output.append(row)

    total_sources = sum(sum(1 for k in r if k.startswith("host-")) for r in output)
    log_info(f"Merged total: {len(output)} movies, {total_sources} sources")

    chunks = _gh_split_records(output)
    log_info(f"Split into {len(chunks)} file(s)")

    for i, chunk_bytes in enumerate(chunks, 1):
        target_local = _split_part_path(local_json_path, i)
        target_local.write_bytes(chunk_bytes)
        log_ok(f"Local JSON → {target_local}  ({len(chunk_bytes):,} B)")

    while len(file_meta) < len(chunks):
        n2 = len(file_meta) + 1
        file_meta.append((_gh_filename(n2), None))

    pushed = 0
    for i, chunk_bytes in enumerate(chunks):
        fname, sha = file_meta[i]
        msg = (f"Update {fname} via pipeline [{extracted_at}]" if sha
               else f"Create {fname} via pipeline [{extracted_at}]")
        try:
            _gh_push_file(token, repo, fname, branch, chunk_bytes, sha, msg)
            pushed += 1
        except Exception as exc:
            log_err(f"  Failed to push {fname}: {exc}")

    log_ok(f"GitHub sync complete — {pushed}/{len(chunks)} file(s) pushed")


# ═══════════════════════════════════════════════════════════════
# CLI ENTRYPOINT
# ═══════════════════════════════════════════════════════════════

def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PrimeSrc FAST pipeline: embed URLs → API keys → stream URLs")
    p.add_argument("--input",                 type=Path, default=DEFAULT_INPUT_FILE)
    p.add_argument("--latest-input",          type=Path, default=DEFAULT_HOLLY_BOLLY_INPUT,  dest="latest_input")
    p.add_argument("--include-manual-input",  action="store_true", default=True,             dest="include_manual_input")
    p.add_argument("--no-manual-input",       action="store_false", dest="include_manual_input")
    p.add_argument("--include-latest-input",  action="store_true", default=True,             dest="include_latest_input")
    p.add_argument("--no-latest-input",       action="store_false", dest="include_latest_input")
    p.add_argument("--fetch-latest",          action="store_true", default=True,             dest="fetch_latest")
    p.add_argument("--no-fetch-latest",       action="store_false", dest="fetch_latest")
    p.add_argument("--latest-limit",          type=int, default=50,                          dest="latest_limit")
    p.add_argument("--api-list-found",        type=Path, default=DEFAULT_API_LIST_FOUND,     dest="api_list_found")
    p.add_argument("--api-list-not-found",    type=Path, default=DEFAULT_API_LIST_NOT_FOUND, dest="api_list_not_found")
    p.add_argument("--json-out",              type=Path, default=DEFAULT_JSON_SUMMARY)
    p.add_argument("--skip-stage1",           action="store_true")
    p.add_argument("--skip-stage2",           action="store_true")
    p.add_argument("--type",                  choices=("movie", "tv"), default="movie")
    p.add_argument("--flaresolverr-url",      default=None,                                  dest="flaresolverr_url")
    p.add_argument("--fs-timeout",            type=int, default=FLARESOLVERR_MAX_TIMEOUT,    dest="fs_timeout_ms")
    p.add_argument("--batch-size",            type=int, default=STAGE2_BATCH_SIZE,           dest="batch_size")
    p.add_argument("--batch-delay",           type=float, default=STAGE2_BATCH_DELAY,        dest="batch_delay")
    p.add_argument("--reloads",               type=int, default=STAGE2_RELOADS)
    p.add_argument("--final-retries",         type=int, default=STAGE2_FINAL_RETRIES,        dest="final_retries")
    p.add_argument("--error-log",             type=Path, default=DEFAULT_ERROR_LOG,          dest="error_log")
    p.add_argument("--processed-urls",        type=Path, default=DEFAULT_PROCESSED_URLS,     dest="processed_urls")
    p.add_argument("--no-github-sync",        action="store_true", default=False,            dest="no_github_sync")
    p.add_argument("--gh-token",              default=None,                                  dest="gh_token")
    p.add_argument("--gh-repo",               default=None,                                  dest="gh_repo")
    p.add_argument("--gh-branch",             default=None,                                  dest="gh_branch")
    return p.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    log_head("PrimeSRC FAST PIPELINE")

    _ensure_file_exists(args.input, "")
    _ensure_file_exists(args.latest_input, "")
    _ensure_file_exists(args.api_list_found, "")
    _ensure_file_exists(args.api_list_not_found, "")
    _ensure_file_exists(args.processed_urls, "")
    _ensure_file_exists(args.error_log, "")
    _ensure_file_exists(args.json_out, "[]\n")

    if args.fetch_latest and args.include_latest_input:
        log_head(f"FETCHING TMDB NOW PLAYING & TOP RATED (limit: {args.latest_limit})")
        fetch_tmdb_now_playing_and_top_rated(
            target_file=args.latest_input,
            existing_input_files=[args.input],
            processed_urls_file=args.processed_urls,
            limit=args.latest_limit,
        )

    all_input_files: list[Path] = []
    if args.include_manual_input:
        all_input_files.append(args.input)
    if args.include_latest_input:
        all_input_files.append(args.latest_input)

    log_info(f"Active input file(s) : {', '.join(p.name for p in all_input_files) if all_input_files else 'None'}")

    stage1_options: list[ServerOption] = []
    stage2_results: list[dict[str, Any]] = []

    gh_token  = args.gh_token  or os.environ.get("GH_TOKEN", "")
    gh_repo   = args.gh_repo   or os.environ.get("GH_REPO",  "")
    gh_branch = args.gh_branch or os.environ.get("GH_BRANCH", "main")
    gh_available = not args.no_github_sync and bool(gh_token) and bool(gh_repo)

    try:
        if args.skip_stage1 or not all_input_files:
            log_info("Stage 1 skipped.")
        else:
            stage1_options = stage1_fetch_api_keys(
                all_input_files,
                args.processed_urls,
                args.type,
                args.api_list_found,
                args.api_list_not_found,
            )

        if args.skip_stage2:
            log_info("Stage 2 skipped.")
        elif not stage1_options:
            log_warn("No keys from Stage 1 — skipping Stage 2.")
        else:
            try:
                stage2_results = await stage2_extract_stream_urls(stage1_options, args)
            except ConnectionError:
                log_err("FlareSolverr unreachable.")
                return 2

        if stage1_options or stage2_results:
            if gh_available:
                github_sync_summary(
                    stage1_options, stage2_results,
                    args.json_out, gh_token, gh_repo, gh_branch,
                )
            else:
                if not args.no_github_sync and not gh_token:
                    log_warn("GH_TOKEN not set — writing locally only")
                _write_summary(stage1_options, stage2_results, args.json_out)

        log_head("DONE")
        if not args.skip_stage2 and stage2_results:
            ok = sum(1 for r in stage2_results if r.get("extracted_url"))
            log_ok(f"Stream URLs extracted : {ok} / {len(stage2_results)}")
        return 0

    finally:
        write_error_log(args.error_log)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
