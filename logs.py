"""In-memory broadcast log ring buffer with per-level counters.

Everything here is defensive: logging must never raise into a broadcast
worker, and a bad level/source must never drop a record.
"""

import threading
from collections import deque
from datetime import datetime

_MAX_RECORDS = 800
_MAX_MESSAGE = 1200

LEVELS = ("info", "success", "warn", "error")

_lock = threading.Lock()
_seq = 0
_buffer = deque(maxlen=_MAX_RECORDS)
_counters = {level: 0 for level in LEVELS}
_sources = {}


def _clean_level(level):
    level = str(level or "").strip().lower()
    return level if level in LEVELS else "info"


def _clean_message(message):
    text = str(message)
    if len(text) > _MAX_MESSAGE:
        text = text[:_MAX_MESSAGE] + " …"
    return text


def blog(level, message, source="broadcast", mirror=True):
    """Append one record. Never raises."""
    global _seq
    level = _clean_level(level)
    try:
        text = _clean_message(message)
    except Exception:
        text = "<unprintable>"
    try:
        source = str(source or "broadcast")[:40]
    except Exception:
        source = "broadcast"

    with _lock:
        _seq += 1
        rec = {
            "id": _seq,
            "ts": datetime.now().strftime("%H:%M:%S"),
            "level": level,
            "source": source,
            "message": text,
        }
        _buffer.append(rec)
        _counters[level] = _counters.get(level, 0) + 1
        _sources[source] = _sources.get(source, 0) + 1

    if mirror:
        try:
            print(f"[{rec['ts']}] [{level.upper()}] {text}", flush=True)
        except Exception:
            pass
    return rec


def info(m, source="broadcast"):
    return blog("info", m, source)


def success(m, source="broadcast"):
    return blog("success", m, source)


def warn(m, source="broadcast"):
    return blog("warn", m, source)


def error(m, source="broadcast"):
    return blog("error", m, source)


def get_logs(since_id=0, limit=300, level="all", source=None):
    try:
        since_id = int(since_id or 0)
    except (TypeError, ValueError):
        since_id = 0
    try:
        limit = max(1, int(limit or 300))
    except (TypeError, ValueError):
        limit = 300

    with _lock:
        items = list(_buffer)

    if since_id:
        items = [r for r in items if r["id"] > since_id]
    if level and level != "all":
        items = [r for r in items if r["level"] == level]
    if source and source != "all":
        items = [r for r in items if r["source"] == source]
    return items[-limit:]


def clear_logs():
    with _lock:
        n = len(_buffer)
        _buffer.clear()
        for key in _counters:
            _counters[key] = 0
        _sources.clear()
    return n


def get_counters():
    with _lock:
        counters = dict(_counters)
    counters["total"] = sum(counters.get(level, 0) for level in LEVELS)
    return counters


def reset_counters():
    with _lock:
        for key in _counters:
            _counters[key] = 0


def last_error():
    with _lock:
        for rec in reversed(_buffer):
            if rec["level"] == "error":
                return dict(rec)
    return None


def sources():
    with _lock:
        return dict(_sources)