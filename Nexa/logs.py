import threading
from collections import deque
from datetime import datetime

_MAX = 600

_lock = threading.Lock()
_seq = 0
_buffer = deque(maxlen=_MAX)
_counters = {"info": 0, "warn": 0, "error": 0, "success": 0}

LEVELS = ("info", "warn", "error", "success")


def blog(level, message, source="broadcast", mirror=True):
    global _seq
    if level not in LEVELS:
        level = "info"
    with _lock:
        _seq += 1
        rec = {
            "id": _seq,
            "ts": datetime.now().strftime("%H:%M:%S"),
            "level": level,
            "source": source,
            "message": str(message),
        }
        _buffer.append(rec)
        _counters[level] = _counters.get(level, 0) + 1
    if mirror:
        try:
            print(f"[{rec['ts']}] [{level.upper()}] {rec['message']}", flush=True)
        except Exception:
            pass
    return rec


def info(m, source="broadcast"):    return blog("info", m, source)
def success(m, source="broadcast"): return blog("success", m, source)
def warn(m, source="broadcast"):    return blog("warn", m, source)
def error(m, source="broadcast"):   return blog("error", m, source)


def get_logs(since_id=0, limit=300, level=None):
    with _lock:
        items = list(_buffer)
    if since_id:
        items = [r for r in items if r["id"] > since_id]
    if level and level != "all":
        items = [r for r in items if r["level"] == level]
    return items[-limit:]


def clear_logs():
    with _lock:
        n = len(_buffer)
        _buffer.clear()
    return n


def get_counters():
    with _lock:
        return dict(_counters)


def reset_counters():
    with _lock:
        for k in _counters:
            _counters[k] = 0


def last_error():
    with _lock:
        for rec in reversed(_buffer):
            if rec["level"] == "error":
                return rec
    return None