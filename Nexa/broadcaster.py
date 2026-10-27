"""
Ads broadcaster (patched, single file, drop-in replacement).

Same public API: ads_broadcaster.start/stop/run_now/send_test/is_running.

Scheduling model
- Every due user is claimed with an atomic compare-and-swap on next_ad_time,
  so run_now() / overlapping ticks can never double-send.
- next_ad_time is anchored (old + interval), so tick latency does not drift
  the cadence. If a user is more than half an interval late it re-anchors.
- Users already running are never started twice (in-flight guard) and are
  only claimed when a worker slot is free, so nothing waits in a queue with
  its timer already advanced.
- Users whose next_ad_time is missing/None are picked up (they were ignored
  before and only got scheduled after a restart).
"""
import asyncio
import html as _html
import os
import re
import threading
import time
import traceback
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

from pyrogram import Client
import pyrogram.errors as _perr
from pyrogram.errors import FloodWait, BadRequest

try:
    from pyrogram.enums import ParseMode
    HTML_MODE = ParseMode.HTML
    # Pyrogram 2.x: parse_mode=None still parses markdown+html. DISABLED is real plain text.
    PLAIN_MODE = getattr(ParseMode, "DISABLED", None)
except ImportError:  # Pyrogram 1.x
    HTML_MODE = "html"
    PLAIN_MODE = None

import Nexa.database as dbmod
from Nexa.logs import info, success, warn, error

from config import (
    API_ID, API_HASH, BROADCAST_WORKERS, TICK_SECONDS,
    CONTACT_LIMIT_PER_ROUND, CONTACT_DM_GAP_SEC, GROUP_DM_GAP_SEC,
    DEFAULT_INTERVAL_SEC, UPLOAD_FOLDER,
)

WORK_TIMEOUT = 600          # minimum per-user round budget (seconds)
DELETE_MAX_ATTEMPTS = 3
DELETE_BATCH = 100
INSERT_CHUNK_SIZE = 500
TRACK_FLUSH = 50            # flush tracking rows to Mongo every N messages
FLOOD_MAX_WAIT = 120        # sleep through FloodWaits up to this, abort the round beyond it
MAX_SEND_TRIES = 3
CAPTION_LIMIT = 1024
TEXT_LIMIT = 4096
BUDGET_MARGIN = 20          # soft deadline = hard timeout - margin
TELEGRAM_SERVICE_ID = 777000


# ---------------------------------------------------------------- errors

def _errs(*names):
    """Resolve pyrogram error classes by name; missing ones are skipped (version safe)."""
    return tuple(c for c in (getattr(_perr, n, None) for n in names) if c is not None)


class _BadSession(Exception):
    pass


class _RoundAbort(Exception):
    def __init__(self, reason, wait=0):
        super().__init__(reason)
        self.reason = reason
        self.wait = wait


class _PeerFloodStop(Exception):
    pass


# Chat-level problems: skip this chat, keep going.
SKIP_ERRORS = _errs(
    "PeerIdInvalid", "ChannelInvalid", "ChannelPrivate", "ChatWriteForbidden",
    "UserIsBlocked", "UserPrivacyRestricted", "InputUserDeactivated",
    "UserBannedInChannel", "SlowmodeWait", "ChatAdminRequired", "ChatRestricted",
    "UserNotParticipant", "UserIsBot", "ChatSendPlainForbidden", "TopicClosed",
    "ForumTopicClosed", "ChatGuestSendForbidden",
)
# Account-level problems: stop the round and disable the user.
FATAL_ERRORS = _errs(
    "UserDeactivated", "UserDeactivatedBan", "AuthKeyUnregistered", "AuthKeyInvalid",
    "AuthKeyDuplicated", "SessionRevoked", "SessionExpired",
) + (_BadSession,)
PEER_FLOOD = _errs("PeerFlood")
PEER_INVALID = _errs("PeerIdInvalid")
MEDIA_FORBIDDEN = _errs("ChatSendMediaForbidden", "ChatSendPhotosForbidden", "ChatSendVideosForbidden")


def _is_parse_error(e):
    s = str(e).lower()
    return any(k in s for k in ("entity", "entities", "can't parse", "cant parse", "parse"))


def _is_file_ref_error(e):
    s = str(e).upper()
    return any(k in s for k in ("FILE_REFERENCE", "FILE_ID", "MEDIA_EMPTY", "MEDIA_INVALID", "DECODE"))


def _flood_value(fw):
    v = getattr(fw, "value", None)
    if v is None:
        v = getattr(fw, "x", None)
    try:
        return max(1, int(v))
    except (TypeError, ValueError):
        return 5


# --------------------------------------------------------------- helpers

def _now():
    # pymongo stores naive datetimes as UTC, so use naive UTC everywhere.
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _naive(dt):
    if not isinstance(dt, datetime):
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _truthy(v, default=False):
    """bool("false") is True in Python - panels often store toggles as strings."""
    if v is None:
        return default
    if isinstance(v, str):
        return v.strip().lower() in ("1", "true", "yes", "on", "y", "enabled")
    return bool(v)


def _u16(s):
    return len(s.encode("utf-16-le")) // 2


def _esc(s):
    return _html.escape(s, quote=False)


def _html_to_plain(s):
    return _html.unescape(re.sub(r"<[^>]+>", "", s))


def _chunked(seq, size):
    buf = []
    for x in seq:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def _chat_type_value(t):
    return getattr(t, "value", t)


def _coerce_chat_id(v):
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        if v in ("me", "self"):
            return "me"
        try:
            return int(v)
        except ValueError:
            return v
    return v


_MEDIA_WARNED = {}


def _resolve_media(media_path):
    if not media_path:
        return None
    if not isinstance(media_path, str):
        media_path = str(media_path)

    base = os.path.basename(media_path)
    candidates = [
        media_path,
        os.path.join(UPLOAD_FOLDER, base),
        os.path.join(os.getcwd(), base),
        os.path.join(UPLOAD_FOLDER, media_path.lstrip("/\\")),
    ]
    for cand in candidates:
        try:
            if cand and os.path.isfile(cand):
                return os.path.abspath(cand)
        except (OSError, ValueError):
            continue

    t = time.time()
    if t - _MEDIA_WARNED.get(media_path, 0) > 1800:  # re-warn every 30 min, not once forever
        _MEDIA_WARNED[media_path] = t
        error(
            f"MEDIA MISSING: '{media_path}' not on disk - sending text only. "
            f"Heroku wipes /app/uploads on restart, re-upload it."
        )
    return None


def _build_payload(config):
    raw = config.get("caption") or ""
    if not isinstance(raw, str):
        raw = str(raw)
    if _truthy(config.get("caption_is_html")):
        base_html, base_plain = raw, _html_to_plain(raw)
    else:
        base_html, base_plain = _esc(raw), raw

    html_links, plain_links = [], []
    ads = config.get("ads") or []
    if not isinstance(ads, list):
        ads = []
    for ad in ads:
        if not isinstance(ad, dict) or not _truthy(ad.get("enabled", True), True):
            continue
        url = str(ad.get("url") or "").strip()
        if not url.startswith(("http://", "https://")) or re.search(r"\s", url):
            continue
        text = str(ad.get("text") or ad.get("label") or "").strip()
        if not text:
            continue
        html_links.append(f'• <a href="{_html.escape(url, quote=True)}">{_esc(text)}</a>')
        plain_links.append(f"• {text}: {url}")

    caption_html, caption_plain = base_html, base_plain
    if html_links:
        caption_html = (caption_html + "\n\n" + "\n".join(html_links)).strip()
        caption_plain = (caption_plain + "\n\n" + "\n".join(plain_links)).strip()

    # Hard cap: nothing longer than a Telegram text message can ever be sent.
    vis = max(_u16(_html_to_plain(caption_html)), _u16(caption_plain))
    if vis > TEXT_LIMIT:
        cut = caption_plain[: TEXT_LIMIT - 10].rstrip()
        while _u16(cut) > TEXT_LIMIT - 10:
            cut = cut[:-50]
        cut += "…"
        caption_html, caption_plain, vis = _esc(cut), cut, _u16(cut)

    media_type = config.get("media_type") or "photo"
    if media_type not in ("photo", "video"):
        media_type = "photo"
    media_path = _resolve_media(config.get("media_path") or config.get("photo_path"))

    return {
        "caption": caption_html,
        "plain": caption_plain,
        "media_path": media_path,
        "media_ref": None,       # file_id cache: upload once, reuse for every chat
        "media_type": media_type,
        "delete_prev": _truthy(config.get("delete_after_round")),
        # media captions max out at 1024: send media bare, then the text as its own message
        "split": bool(media_path) and vis > CAPTION_LIMIT,
        "use_plain": False,      # per-round flag, set only by a real parse error
    }


# ---------------------------------------------------------- round state

class _Round:
    def __init__(self, user, bid, payload, stop, uid, phone):
        self.user = user
        self.bid = bid
        self.p = payload
        self.stop = stop
        self.uid = uid
        self.phone = phone
        self.deadline = float("inf")
        self.delivered = 0
        self.skipped = Counter()
        self.failed = Counter()
        self.flood_wait = 0
        self.peer_flood = False
        self.fatal = None
        self.msgs_col = dbmod.broadcast_msgs_col
        self.buffer = []
        self.stored = 0
        self.total_tracked = 0
        self._flush_at = TRACK_FLUSH

    def expired(self):
        return self.stop.is_set() or time.monotonic() >= self.deadline

    def track(self, chat_id, message_id):
        """Queue a tracking row; flushed incrementally so a crash/timeout loses nothing."""
        if self.msgs_col is None:
            return
        try:
            cid = "me" if chat_id == "me" else int(chat_id)
        except (TypeError, ValueError):
            return
        self.buffer.append({
            "user_id": self.uid,
            "phone": self.phone,
            "broadcast_id": self.bid,
            "chat_id": cid,
            "message_id": message_id,
            "delete_attempts": 0,
            "sent_at": _now(),
        })
        self.total_tracked += 1
        if len(self.buffer) >= self._flush_at:
            self.flush()
            self._flush_at = len(self.buffer) + TRACK_FLUSH

    def flush(self):
        if self.msgs_col is None or not self.buffer:
            return
        docs, self.buffer = self.buffer, []
        for chunk in _chunked(docs, INSERT_CHUNK_SIZE):
            try:
                self.msgs_col.insert_many(chunk, ordered=False)
                self.stored += len(chunk)
            except Exception as e:
                error(f"[{self.phone}] track insert failed ({len(chunk)}): {type(e).__name__}: {e}")
                self.buffer.extend(chunk)   # retried on the next flush


# ----------------------------------------------------------- broadcaster

class AdsBroadcaster:
    def __init__(self):
        self._running = False
        self._thread = None
        self._stop_event = threading.Event()
        self._wake = threading.Event()
        self._lock = threading.Lock()
        self._tick_lock = threading.Lock()
        self._inflight = set()
        self._inflight_lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=max(1, int(BROADCAST_WORKERS)), thread_name_prefix="bc")
        self._indexes_done = False

    # ---- lifecycle

    def is_running(self):
        return self._running

    def start(self):
        with self._lock:
            if self._running:
                return False
            old = self._thread
            if old is not None and old.is_alive():
                old.join(timeout=10)
                if old.is_alive():
                    warn("previous broadcaster loop is still shutting down - try again shortly")
                    return False
            # fresh events per run: workers of an older run keep their own (set) event
            self._stop_event = threading.Event()
            self._wake = threading.Event()
            self._running = True
            self._thread = threading.Thread(
                target=self._run, args=(self._stop_event, self._wake), daemon=True,
            )
            self._thread.start()
        success("Broadcaster started")
        return True

    def stop(self):
        with self._lock:
            if not self._running:
                return False
            self._stop_event.set()
            self._wake.set()
            self._running = False
            t = self._thread
        if t:
            t.join(timeout=10)
        dbmod.update_ads_config({"next_broadcast_at": None})
        info("Broadcaster stopped")
        return True

    def run_now(self):
        threading.Thread(target=self._tick_safe, args=(self._stop_event,), daemon=True).start()
        return True

    def send_test(self, user):
        bid = f"test_{_now():%Y%m%d_%H%M%S_%f}"
        if not self._claim_inflight(user["_id"]):
            warn(f"TEST skipped for {user.get('phone', '?')}: a round is already running")
            return False
        info(f"TEST send queued for {user.get('phone', '?')}")
        # fresh never-set event so a test works even while the loop is stopped
        return self._submit(user, bid, None, None, threading.Event())

    # ---- loop

    def _run(self, stop, wake):
        self._ensure_indexes()
        try:
            n = dbmod.backfill_next_ad_time()
            if n:
                info(f"Scheduled {n} previously unscheduled users")
        except Exception as e:
            error(f"BACKFILL ERROR: {type(e).__name__}: {e}")
            print(traceback.format_exc(), flush=True)

        while not stop.is_set():
            self._tick_safe(stop)
            wake.wait(TICK_SECONDS)
            wake.clear()

    def _ensure_indexes(self):
        if self._indexes_done:
            return
        self._indexes_done = True
        try:
            if dbmod.broadcast_msgs_col is not None:
                dbmod.broadcast_msgs_col.create_index("user_id")
            if dbmod.users_col is not None:
                dbmod.users_col.create_index([("ads_enabled", 1), ("next_ad_time", 1)])
        except Exception as e:
            warn(f"index creation skipped: {type(e).__name__}: {e}")

    def _tick_safe(self, stop):
        try:
            with self._tick_lock:           # run_now() and the loop never tick concurrently
                self._tick(stop)
        except Exception as e:
            error(f"TICK CRASH: {type(e).__name__}: {e}")
            print(traceback.format_exc(), flush=True)

    # ---- in-flight guard

    def _claim_inflight(self, _id):
        with self._inflight_lock:
            if _id in self._inflight:
                return False
            self._inflight.add(_id)
            return True

    def _release_inflight(self, _id):
        with self._inflight_lock:
            self._inflight.discard(_id)

    def _inflight_ids(self):
        with self._inflight_lock:
            return list(self._inflight)

    # ---- scheduling

    @staticmethod
    def _next_slot(old, now, interval):
        """Anchored cadence: old+interval when on time, now+interval when badly late."""
        step = timedelta(seconds=interval)
        old_n = _naive(old)
        if old_n is not None and timedelta(0) <= (now - old_n) < step / 2:
            return old_n + step
        return now + step

    def _tick(self, stop):
        users_col = dbmod.users_col
        ads_config_col = dbmod.ads_config_col

        if users_col is None or ads_config_col is None:
            warn("tick skipped: database handles not ready")
            return

        config = ads_config_col.find_one({"_id": "main_config"})
        if config is None:
            warn("tick skipped: no config document")
            return
        if not _truthy(config.get("ads_enabled", False)):
            return

        try:
            interval = max(60, int(config.get("interval", DEFAULT_INTERVAL_SEC)))
        except (TypeError, ValueError):
            interval = max(60, int(DEFAULT_INTERVAL_SEC))

        now = _now()

        # users without a session can never send: disable instead of rescheduling forever
        res = users_col.update_many(
            {"ads_enabled": True, "$or": [{"session_string": None}, {"session_string": ""}]},
            {"$set": {"ads_enabled": False, "status": "no_session"}},
        )
        if res.modified_count:
            warn(f"disabled {res.modified_count} user(s) with no session_string (status=no_session)")

        # interval was shortened: pull far-future schedules in (flood penalties are left alone)
        horizon = now + timedelta(seconds=interval)
        users_col.update_many(
            {
                "ads_enabled": True,
                "next_ad_time": {"$gt": horizon},
                "$or": [{"flood_until": None}, {"flood_until": {"$lte": now}}],
            },
            {"$set": {"next_ad_time": horizon}},
        )

        # only claim as many users as there are free worker slots
        inflight = self._inflight_ids()
        capacity = max(1, int(BROADCAST_WORKERS)) - len(inflight)
        if capacity <= 0:
            self._refresh_next_broadcast()
            return

        due_q = {
            "ads_enabled": True,
            "$or": [{"next_ad_time": None}, {"next_ad_time": {"$lte": now}}],
        }
        if inflight:
            due_q["_id"] = {"$nin": inflight}
        due = list(users_col.find(due_q).sort("next_ad_time", 1).limit(capacity))

        if not due:
            self._refresh_next_broadcast()
            return

        bid = f"round_{now:%Y%m%d_%H%M%S_%f}"
        info(f"ROUND {bid}: {len(due)} user(s) due")

        claimed = 0
        for user in due:
            if stop.is_set():
                break
            uid = user["_id"]
            if not self._claim_inflight(uid):
                continue
            try:
                old = user.get("next_ad_time")
                new = self._next_slot(old, now, interval)
                # compare-and-swap: only one claimer can win this user
                res = users_col.update_one(
                    {"_id": uid, "next_ad_time": old},
                    {"$set": {"next_ad_time": new}},
                )
                if res.modified_count != 1:
                    self._release_inflight(uid)
                    continue
                old_n = _naive(old)
                if old_n is not None:
                    late = (now - old_n).total_seconds()
                    if late > max(60, TICK_SECONDS * 2):
                        warn(f"[{user.get('phone', '?')}] due {int(late)}s ago (waited for a free worker)")
                if self._submit(user, bid, new, interval, stop):
                    claimed += 1
            except Exception as e:
                self._release_inflight(uid)
                error(f"CLAIM ERROR [{user.get('phone', '?')}]: {type(e).__name__}: {e}")

        try:
            if claimed:
                dbmod.inc_ads_config("total_broadcasts", 1)
                dbmod.update_ads_config({"last_broadcast_at": now})
            self._refresh_next_broadcast()
        except Exception as e:
            error(f"SCHEDULER UPDATE ERROR: {type(e).__name__}: {e}")

    def _submit(self, user, bid, planned_next, interval, stop):
        """Caller must already hold the in-flight claim for this user."""
        try:
            future = self._pool.submit(self._send_to_user, user, bid, planned_next, interval, stop)
            future.add_done_callback(self._worker_done)
            return True
        except Exception as e:
            self._release_inflight(user["_id"])
            error(f"SUBMIT FAILED: {type(e).__name__}: {e}")
            return False

    def _worker_done(self, future):
        try:
            exc = future.exception()
        except Exception as e:
            error(f"FUTURE ERROR: {type(e).__name__}: {e}")
            return
        if exc is not None:
            error(f"WORKER CRASH: {type(exc).__name__}: {exc}")

    def _refresh_next_broadcast(self):
        users_col = dbmod.users_col
        if users_col is None:
            return
        doc = users_col.find_one(
            {"ads_enabled": True, "next_ad_time": {"$ne": None}},
            sort=[("next_ad_time", 1)],
        )
        next_time = doc.get("next_ad_time") if doc is not None else None
        dbmod.update_ads_config({"next_broadcast_at": next_time})

    # ------------------------------------------------------ fetchers

    async def _fetch_dialogs_safe(self, client, phone):
        best = []
        for attempt in range(3):
            got = []
            try:
                async for d in client.get_dialogs():
                    got.append(d)
                info(f"[{phone}] fetched {len(got)} dialogs")
                if not got:
                    warn(f"[{phone}] get_dialogs returned 0 dialogs")
                return got
            except FATAL_ERRORS:
                raise
            except FloodWait as fw:
                wait = min(_flood_value(fw), FLOOD_MAX_WAIT)
                warn(f"[{phone}] FloodWait {wait}s on get_dialogs")
                await asyncio.sleep(wait + 1)
            except Exception as e:
                warn(f"[{phone}] get_dialogs attempt {attempt + 1}: {type(e).__name__}: {e}")
                await asyncio.sleep(2)
            if len(got) > len(best):
                best = got
        if best:
            warn(f"[{phone}] get_dialogs failed, using partial list ({len(best)})")
        else:
            error(f"[{phone}] get_dialogs FAIL - no groups/DMs this round")
        return best

    async def _fetch_contacts_safe(self, client, phone):
        for attempt in range(3):
            try:
                raw = await client.get_contacts() or []
                cleaned = [
                    x for x in raw
                    if x is not None
                    and not getattr(x, "is_bot", False)
                    and not getattr(x, "is_deleted", False)
                ]
                info(f"[{phone}] fetched {len(cleaned)} contacts (of {len(raw)} raw)")
                return cleaned
            except FATAL_ERRORS:
                raise
            except FloodWait as fw:
                wait = min(_flood_value(fw), FLOOD_MAX_WAIT)
                warn(f"[{phone}] FloodWait {wait}s on get_contacts")
                await asyncio.sleep(wait + 1)
            except Exception as e:
                warn(f"[{phone}] get_contacts attempt {attempt + 1}: {type(e).__name__}: {e}")
                await asyncio.sleep(2)
        error(f"[{phone}] get_contacts FAIL - no contact DMs this round")
        return []

    # --------------------------------------------------------- sending

    async def _with_parse(self, p, send, html_body, plain_body):
        """HTML first; fall back to plain only for a real parse error, per round, never globally."""
        if not p["use_plain"]:
            try:
                return await send(HTML_MODE, html_body)
            except SKIP_ERRORS:
                raise
            except (BadRequest, ValueError) as e:
                if not _is_parse_error(e):
                    raise
                p["use_plain"] = True
                warn(f"HTML parse failed ({type(e).__name__}: {e}) - plain text for the rest of this round")
        return await send(PLAIN_MODE, plain_body)

    async def _send_text(self, c, chat_id, p):
        async def go(parse, body):
            return await c.send_message(chat_id, body or "\u200b", parse_mode=parse)
        return await self._with_parse(p, go, p["caption"], p["plain"])

    async def _send_media(self, c, chat_id, p, with_caption):
        video = p["media_type"] == "video"

        async def attempt(src):
            async def go(parse, body):
                body = body if with_caption else ""
                if video:
                    return await c.send_video(chat_id, video=src, caption=body, parse_mode=parse)
                return await c.send_photo(chat_id, photo=src, caption=body, parse_mode=parse)
            return await self._with_parse(p, go, p["caption"], p["plain"])

        if p["media_ref"]:
            try:
                return await attempt(p["media_ref"])
            except (BadRequest, ValueError) as e:
                if isinstance(e, SKIP_ERRORS) or not _is_file_ref_error(e):
                    raise
                warn(f"cached file_id rejected ({type(e).__name__}) - re-uploading")
                p["media_ref"] = None

        msg = await attempt(p["media_path"])
        fid = getattr(getattr(msg, "video" if video else "photo", None), "file_id", None)
        if fid:
            p["media_ref"] = fid      # every later chat reuses this, no re-upload
        return msg

    async def _send_layout(self, c, chat_id, ctx):
        p = ctx.p

        def track(msg):
            if chat_id == "me":
                cid = "me"
            else:
                cid = getattr(getattr(msg, "chat", None), "id", None) or chat_id
            ctx.track(cid, msg.id)

        if not p["media_path"]:
            track(await self._send_text(c, chat_id, p))
            return

        try:
            track(await self._send_media(c, chat_id, p, not p["split"]))
        except MEDIA_FORBIDDEN:
            # group forbids media: still deliver the text
            if not p["plain"]:
                raise
            track(await self._send_text(c, chat_id, p))
            return

        if p["split"] and p["plain"]:
            try:
                track(await self._send_text(c, chat_id, p))
            except FATAL_ERRORS:
                raise
            except Exception as e:
                # media already delivered; do not retry (would duplicate it)
                warn(f"[{ctx.phone}] text part to {chat_id} failed: {type(e).__name__}: {e}")

    async def _send_one(self, ctx, c, chat_id, username=None):
        """True = delivered, False = skipped/failed. Raises _RoundAbort / _PeerFloodStop / fatal."""
        tried_username = False
        for attempt in range(MAX_SEND_TRIES):
            try:
                await self._send_layout(c, chat_id, ctx)
                return True
            except FATAL_ERRORS:
                raise
            except PEER_FLOOD:
                raise _PeerFloodStop()
            except FloodWait as fw:
                wait = _flood_value(fw)
                if wait > FLOOD_MAX_WAIT or attempt == MAX_SEND_TRIES - 1:
                    raise _RoundAbort(f"FloodWait {wait}s", wait)
                warn(f"[{ctx.phone}] FloodWait {wait}s on {chat_id}, sleeping")
                await asyncio.sleep(wait + 1)
            except SKIP_ERRORS as e:
                if username and not tried_username and PEER_INVALID and isinstance(e, PEER_INVALID):
                    tried_username = True          # contact not in peer cache: resolve via @username
                    chat_id = username
                    continue
                ctx.skipped[type(e).__name__] += 1
                return False
            except Exception as e:
                ctx.failed[type(e).__name__] += 1
                warn(f"[{ctx.phone}] send {chat_id}: {type(e).__name__}: {e}")
                return False
        return False

    async def _run_phase(self, c, ctx, label, targets, gap, offset_key=None, limit=None, private=False):
        phone = ctx.phone
        total = len(targets)
        if not total:
            info(f"[{phone}] no {label} targets")
            return
        if private and ctx.peer_flood:
            warn(f"[{phone}] {label} skipped - PeerFlood earlier this round")
            return

        offset = 0
        if offset_key:
            offset = int(ctx.user.get(offset_key) or 0) % total
            targets = targets[offset:] + targets[:offset]
        if limit:
            targets = targets[:limit]

        attempted = delivered = 0
        note = ""
        try:
            for chat_id, uname in targets:
                if ctx.expired():
                    note = " (stopped early: time budget/stop)"
                    break
                try:
                    ok = await self._send_one(ctx, c, chat_id, uname)
                except _PeerFloodStop:
                    if private:
                        ctx.peer_flood = True
                    note = " (PeerFlood - stopped this phase)"
                    warn(f"[{phone}] PEER_FLOOD on {label}")
                    break
                attempted += 1
                if ok:
                    delivered += 1
                    ctx.delivered += 1
                    await asyncio.sleep(gap)
                else:
                    await asyncio.sleep(0.05)
        finally:
            # advance the rotating window by what was really attempted, even on timeout/abort
            if offset_key and attempted:
                try:
                    dbmod.users_col.update_one(
                        {"_id": ctx.user["_id"]},
                        {"$set": {offset_key: (offset + attempted) % total}},
                    )
                except Exception as e:
                    warn(f"[{phone}] could not save {offset_key}: {type(e).__name__}")
            info(f"[{phone}] {label}: {delivered}/{attempted} delivered of {total}{note}")

    # --------------------------------------------- delete previous ads

    async def _delete_previous(self, c, ctx):
        """Delete last round's ads. Needs dialogs/contacts fetched first (peer cache)."""
        msgs = ctx.msgs_col
        phone = ctx.phone
        if msgs is None:
            warn(f"[{phone}] delete skipped: broadcast_msgs_col unavailable")
            return
        rows = list(msgs.find({"user_id": ctx.uid}))
        if not rows:
            info(f"[{phone}] no previous messages to delete")
            return

        by_chat = defaultdict(list)
        corrupt = []
        for row in rows:
            cid = _coerce_chat_id(row.get("chat_id"))
            if cid is None or row.get("message_id") is None:
                corrupt.append(row["_id"])
            else:
                by_chat[cid].append(row)
        if corrupt:
            msgs.delete_many({"_id": {"$in": corrupt}})

        ok = kept = 0
        purged = len(corrupt)
        flood_stop = False

        for cid, rlist in by_chat.items():
            if flood_stop or ctx.expired():
                break
            for batch in _chunked(rlist, DELETE_BATCH):
                if ctx.expired():
                    break
                ids = [r["message_id"] for r in batch]
                row_ids = [r["_id"] for r in batch]
                outcome = "retry"
                for attempt in range(2):
                    try:
                        await c.delete_messages(cid, ids)
                        outcome = "ok"
                        break
                    except FATAL_ERRORS:
                        raise
                    except FloodWait as fw:
                        wait = _flood_value(fw)
                        if wait > FLOOD_MAX_WAIT:
                            warn(f"[{phone}] delete FloodWait {wait}s - leaving the rest for next round")
                            flood_stop = True
                            break
                        await asyncio.sleep(wait + 1)
                    except SKIP_ERRORS as e:
                        warn(f"[{phone}] delete {cid} unrecoverable: {type(e).__name__}")
                        outcome = "gone"
                        break
                    except Exception as e:
                        warn(f"[{phone}] delete {cid} failed: {type(e).__name__}: {e}")
                        break

                if outcome in ("ok", "gone"):
                    msgs.delete_many({"_id": {"$in": row_ids}})
                    if outcome == "ok":
                        ok += len(batch)
                    else:
                        purged += len(batch)
                else:
                    drop = [r["_id"] for r in batch if int(r.get("delete_attempts", 0)) + 1 >= DELETE_MAX_ATTEMPTS]
                    keep = [r["_id"] for r in batch if r["_id"] not in set(drop)]
                    if drop:
                        msgs.delete_many({"_id": {"$in": drop}})
                        purged += len(drop)
                    if keep:
                        msgs.update_many({"_id": {"$in": keep}}, {"$inc": {"delete_attempts": 1}})
                        kept += len(keep)
                    if flood_stop:
                        break
                await asyncio.sleep(0.3)

        info(f"[{phone}] delete prev: {ok} ok, {kept} retry next round, {purged} dropped ({len(rows)} total)")

    # -------------------------------------------------------- worker

    def _send_to_user(self, user, bid, planned_next, interval, stop):
        phone = user.get("phone", "?")
        try:
            self._send_to_user_inner(user, bid, planned_next, interval, stop)
        except Exception as e:
            error(f"[{phone}] UNCAUGHT {type(e).__name__}: {e}")
            print(traceback.format_exc(), flush=True)
        finally:
            self._release_inflight(user["_id"])
            self._wake.set()      # a worker slot is free: let the loop claim the next due user now

    def _send_to_user_inner(self, user, bid, planned_next, interval, stop):
        users_col = dbmod.users_col
        ads_config_col = dbmod.ads_config_col

        uid = user.get("user_id")
        if uid is None:
            uid = user["_id"]
        phone = user.get("phone", "?")
        ss = user.get("session_string")

        if not ss:
            warn(f"[{phone}] SKIP - no session_string in user doc")
            return
        if ads_config_col is None or users_col is None:
            warn(f"[{phone}] SKIP - database collection unavailable")
            return

        config = ads_config_col.find_one({"_id": "main_config"})
        if not config:
            warn(f"[{phone}] SKIP - no config doc")
            return
        if not _truthy(config.get("ads_enabled", False)):
            warn(f"[{phone}] SKIP - broadcast disabled")
            return

        payload = _build_payload(config)
        cfg_media = config.get("media_path") or config.get("photo_path")
        media_state = "yes" if payload["media_path"] else ("MISSING" if cfg_media else "no")
        info(
            f"[{phone}] start | media={media_state} | caption={len(payload['plain'])}ch "
            f"| split={payload['split']} | delete_prev={payload['delete_prev']}"
        )

        ctx = _Round(user, bid, payload, stop, uid, phone)
        budget = max(WORK_TIMEOUT, int((interval or 0) - 15))
        ctx.deadline = time.monotonic() + budget - BUDGET_MARGIN

        async def work():
            c = Client(
                name=f"bc_{uid}_{uuid.uuid4().hex[:8]}",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=ss,
                in_memory=True,
                no_updates=True,
            )
            try:
                await c.connect()
                me = await c.get_me()
                if not me:
                    raise _BadSession("empty get_me")
                c.me = me

                # 1) populate the peer cache FIRST (delete + send both need it)
                contacts = await self._fetch_contacts_safe(c, phone)
                contact_ids = {x.id for x in contacts}
                dialogs = await self._fetch_dialogs_safe(c, phone)

                # 2) previous ads: delete (toggle ON) or just forget them (toggle OFF).
                #    Either way only the newest round stays tracked, so the collection stays small.
                try:
                    if payload["delete_prev"]:
                        await self._delete_previous(c, ctx)
                    elif ctx.msgs_col is not None:
                        ctx.msgs_col.delete_many({"user_id": uid})
                except FATAL_ERRORS:
                    raise
                except Exception as e:
                    error(f"[{phone}] delete step failed (continuing to send): {type(e).__name__}: {e}")

                # 3) Saved Messages
                try:
                    if await self._send_one(ctx, c, "me"):
                        ctx.delivered += 1
                        success(f"[{phone}] self delivered")
                except _PeerFloodStop:
                    pass

                # 4) groups (rotating start so a cut-short round never starves the same groups)
                groups = [
                    (d.chat.id, None) for d in dialogs
                    if d and d.chat and _chat_type_value(d.chat.type) in ("group", "supergroup")
                ]
                await self._run_phase(c, ctx, "group(s)", groups, GROUP_DM_GAP_SEC, offset_key="group_offset")

                # 5) regular DMs (non-contacts), rotating window
                dms = [
                    (d.chat.id, getattr(d.chat, "username", None)) for d in dialogs
                    if d and d.chat
                    and _chat_type_value(d.chat.type) == "private"
                    and d.chat.id != me.id
                    and d.chat.id != TELEGRAM_SERVICE_ID
                    and d.chat.id not in contact_ids
                    and not getattr(d.chat, "is_bot", False)
                ]
                await self._run_phase(
                    c, ctx, "dm(s)", dms, CONTACT_DM_GAP_SEC,
                    offset_key="dm_offset", limit=CONTACT_LIMIT_PER_ROUND, private=True,
                )

                # 6) contacts, rotating window
                targets = [(x.id, getattr(x, "username", None)) for x in contacts]
                await self._run_phase(
                    c, ctx, "contact(s)", targets, CONTACT_DM_GAP_SEC,
                    offset_key="contact_offset", limit=CONTACT_LIMIT_PER_ROUND, private=True,
                )
            except _RoundAbort as ab:
                ctx.flood_wait = max(ctx.flood_wait, ab.wait)
                error(f"[{phone}] round aborted: {ab.reason}")
            finally:
                ctx.flush()
                try:
                    await c.disconnect()
                except Exception:
                    pass

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(asyncio.wait_for(work(), timeout=budget))
        except asyncio.TimeoutError:
            error(f"[{phone}] TIMEOUT after {budget}s (progress was saved)")
        except FATAL_ERRORS as e:
            ctx.fatal = type(e).__name__
            users_col.update_one(
                {"_id": user["_id"]},
                {"$set": {"ads_enabled": False, "status": "expired", "status_reason": ctx.fatal}},
            )
            error(f"[{phone}] session dead ({ctx.fatal}) - ads disabled for this user")
        except Exception as e:
            error(f"[{phone}] ROUND ERROR {type(e).__name__}: {e}")
            print(traceback.format_exc(), flush=True)
        finally:
            try:
                loop.close()
            except Exception:
                pass
            ctx.flush()
            self._finalize(ctx, planned_next)

    def _finalize(self, ctx, planned_next):
        users_col = dbmod.users_col
        phone = ctx.phone
        extra = ""
        if ctx.skipped:
            extra += f" | skipped {dict(ctx.skipped)}"
        if ctx.failed:
            extra += f" | failed {dict(ctx.failed)}"
        extra += f" | tracked {ctx.stored}/{ctx.total_tracked}"

        if ctx.fatal:
            return
        try:
            upd = {}
            if ctx.delivered > 0:
                upd["$set"] = {"last_ad_time": _now(), "last_broadcast_id": ctx.bid}
                upd["$inc"] = {"deliveries": 1}
                upd["$unset"] = {"flood_until": ""}
                success(f"[{phone}] FINISHED - {ctx.delivered} chat(s){extra}")
            else:
                upd["$inc"] = {"failures": 1}
                error(f"[{phone}] FINISHED with 0 deliveries{extra}")

            # long FloodWait: do not hammer Telegram, push the next round past the wait
            if ctx.flood_wait and planned_next is not None:
                resume = _now() + timedelta(seconds=ctx.flood_wait + 5)
                if resume > planned_next:
                    upd.setdefault("$set", {}).update({"next_ad_time": resume, "flood_until": resume})
                    upd.pop("$unset", None)
                    warn(f"[{phone}] next round delayed until {resume:%H:%M:%S} UTC (flood)")
            users_col.update_one({"_id": ctx.user["_id"]}, upd)
        except Exception as e:
            error(f"[{phone}] STATS UPDATE ERROR: {type(e).__name__}: {e}")


ads_broadcaster = AdsBroadcaster()
