import asyncio
import os
import random
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from pyrogram import Client
from pyrogram.errors import (
    FloodWait, BadRequest, UserDeactivated, AuthKeyUnregistered,
    PeerIdInvalid, ChannelInvalid, ChannelPrivate, ChatWriteForbidden,
    ChatForbidden, UserNotParticipant, UsernameNotOccupied, UsernameInvalid,
)

try:
    from pyrogram.enums import ParseMode
except ImportError:
    from pyrogram.types import ParseMode

import Nexa.database as dbmod
from Nexa.logs import info, success, warn, error

try:
    from Nexa.session_tools import DEAD_ERRORS
except ImportError:
    DEAD_ERRORS = ()

import config as _config
from config import (
    API_ID, API_HASH, BROADCAST_WORKERS, TICK_SECONDS,
    CONTACT_LIMIT_PER_ROUND, CONTACT_DM_GAP_SEC, GROUP_DM_GAP_SEC,
    DEFAULT_INTERVAL_SEC, UPLOAD_FOLDER,
)

# Permanent peer problems: retrying next round will not help, so cache them.
SKIP_ERRORS = (
    PeerIdInvalid, ChannelInvalid, ChannelPrivate, ChatWriteForbidden,
    ChatForbidden, UserNotParticipant, UsernameNotOccupied, UsernameInvalid,
)

# Session-level failures: the whole user is dead, not just one chat.
DEAD_ERRORS = tuple(DEAD_ERRORS) + (UserDeactivated, AuthKeyUnregistered)

GROUP_LIMIT_PER_ROUND = int(getattr(_config, "GROUP_LIMIT_PER_ROUND", 120))
WORK_TIMEOUT = 600
TELEGRAM_SERVICE_IDS = {777000, 4240000, 1087968824}

_PARSE = {"fails": 0, "until": 0.0, "warned": 0.0}


def _is_dead(exc):
    return isinstance(exc, DEAD_ERRORS)


def _parse_mode():
    if _PARSE["fails"] >= 3 and time.time() < _PARSE["until"]:
        return None
    return ParseMode.HTML


def _parse_failed(reason):
    _PARSE["fails"] += 1
    _PARSE["until"] = time.time() + 300
    if time.time() - _PARSE["warned"] > 300:
        _PARSE["warned"] = time.time()
        warn(f"HTML parse mode rejected ({reason}) - plain text for 5 min")


def _parse_ok():
    _PARSE["fails"] = 0


_MEDIA_WARNED = set()


def _resolve_media(media_path):
    if not media_path:
        return None
    if not isinstance(media_path, str):
        media_path = str(media_path)

    base = os.path.basename(media_path)
    for cand in (media_path,
                 os.path.join(UPLOAD_FOLDER, base),
                 os.path.join(os.getcwd(), base),
                 os.path.join(UPLOAD_FOLDER, media_path.lstrip("/\\"))):
        try:
            if cand and os.path.isfile(cand):
                return os.path.abspath(cand)
        except (OSError, ValueError):
            continue

    if media_path not in _MEDIA_WARNED:
        _MEDIA_WARNED.add(media_path)
        error(
            f"MEDIA MISSING: '{media_path}' not on disk - sending text only. "
            f"Heroku wipes /uploads on restart, re-upload it."
        )
    return None


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


def _norm_chat_key(v):
    return "me" if v in ("me", "self") else v


def _esc(text):
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _build_payload(config):
    base = config.get("caption") or ""
    if not isinstance(base, str):
        base = str(base)
    if not config.get("caption_is_html"):
        base = _esc(base)

    link_lines = []
    ads = config.get("ads") or []
    if isinstance(ads, list):
        for ad in ads:
            if not isinstance(ad, dict) or not ad.get("enabled", True):
                continue
            url = ad.get("url") or ""
            if not isinstance(url, str):
                url = str(url)
            url = url.strip()
            if not url.startswith(("http://", "https://")):
                continue
            text = (ad.get("text") or ad.get("label") or "").strip()
            if not text:
                continue
            link_lines.append(f'• <a href="{url}">{_esc(text)}</a>')

    caption = base
    if link_lines:
        caption = (caption + "\n\n" + "\n".join(link_lines)).strip()

    media_type = config.get("media_type") or "photo"
    if media_type not in ("photo", "video"):
        media_type = "photo"

    return {
        "caption": caption,
        "media_path": _resolve_media(config.get("media_path") or config.get("photo_path")),
        "media_type": media_type,
        "delete_prev": config.get("delete_after_round", True) is not False,
    }


class _Stats:
    __slots__ = ("sent", "peer", "peer_kinds", "failed", "flood", "new_bad")

    def __init__(self):
        self.sent = []
        self.peer = 0
        self.peer_kinds = {}
        self.failed = 0
        self.flood = 0
        self.new_bad = []

    def note_peer(self, exc, chat_id):
        name = type(exc).__name__
        self.peer += 1
        self.peer_kinds[name] = self.peer_kinds.get(name, 0) + 1
        if chat_id != "me":
            self.new_bad.append(chat_id)

    def peer_summary(self):
        parts = [f"{k} x{v}" for k, v in sorted(self.peer_kinds.items())]
        return ", ".join(parts)


class AdsBroadcaster:
    def __init__(self):
        self._running = False
        self._thread = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()
        self._pool = ThreadPoolExecutor(max_workers=BROADCAST_WORKERS,
                                        thread_name_prefix="bc")

    def is_running(self):
        return self._running

    def start(self):
        with self._lock:
            if self._running:
                return False
            self._stop_event.clear()
            self._running = True
            self._thread = threading.Thread(target=self._run, name="broadcaster", daemon=True)
            self._thread.start()
        success("Broadcaster started")
        return True

    def stop(self):
        with self._lock:
            if not self._running:
                return False
            self._stop_event.set()
            self._running = False
            t = self._thread
        if t:
            t.join(timeout=5)
        dbmod.update_ads_config({"next_broadcast_at": None})
        info("Broadcaster stopped")
        return True

    def run_now(self):
        threading.Thread(target=self._tick_safe, name="bc-now", daemon=True).start()
        return True

    # ------------------------------------------------------------ loop

    def _run(self):
        try:
            n = dbmod.backfill_next_ad_time()
            if n:
                info(f"Scheduled {n} previously unscheduled user(s)")
        except Exception as exc:
            error(f"BACKFILL ERROR: {type(exc).__name__}: {exc}")
            print(traceback.format_exc(), flush=True)

        while not self._stop_event.is_set():
            self._tick_safe()
            self._stop_event.wait(TICK_SECONDS)

    def _tick_safe(self):
        try:
            self._tick()
        except Exception as exc:
            error(f"TICK CRASH: {type(exc).__name__}: {exc}")
            print(traceback.format_exc(), flush=True)

    def _tick(self):
        dbmod.ensure_db()
        users_col = dbmod.users_col
        if users_col is None:
            warn("tick skipped: database not ready")
            return

        config = dbmod.get_ads_config()
        if not config.get("ads_enabled", False):
            return

        try:
            interval = max(60, int(config.get("interval", DEFAULT_INTERVAL_SEC)))
        except (TypeError, ValueError):
            interval = max(60, int(DEFAULT_INTERVAL_SEC))

        now = datetime.now()
        due = list(users_col.find({
            "ads_enabled": True,
            "status": {"$ne": "dead"},
            "next_ad_time": {"$lte": now},
        }).limit(500))

        if not due:
            self._refresh_next_broadcast()
            return

        bid = f"round_{now:%Y%m%d_%H%M%S}"
        payload = _build_payload(config)

        no_session = [u.get("phone", "?") for u in due if not u.get("session_string")]
        info(f"ROUND {bid}: {len(due)} user(s) due "
             f"| media={'yes' if payload['media_path'] else 'no'} "
             f"| delete_prev={'yes' if payload['delete_prev'] else 'no'}")
        if no_session:
            warn(f"{len(no_session)} user(s) without session_string: "
                 f"{', '.join(no_session[:6])}{' …' if len(no_session) > 6 else ''}")

        for user in due:
            if self._stop_event.is_set():
                break
            try:
                users_col.update_one(
                    {"_id": user["_id"]},
                    {"$set": {"next_ad_time": now + timedelta(seconds=interval)}},
                )
                self._submit(user, bid, payload)
            except Exception as exc:
                error(f"SUBMIT ERROR [{user.get('phone', '?')}]: "
                      f"{type(exc).__name__}: {exc}")

        try:
            dbmod.inc_ads_config("total_broadcasts", 1)
            dbmod.update_ads_config({"last_broadcast_at": now})
            self._refresh_next_broadcast()
        except Exception as exc:
            error(f"SCHEDULER UPDATE ERROR: {type(exc).__name__}: {exc}")

    def _submit(self, user, bid, payload):
        try:
            future = self._pool.submit(self._send_to_user, user, bid, payload)
            future.add_done_callback(self._worker_done)
        except Exception as exc:
            error(f"SUBMIT FAILED: {type(exc).__name__}: {exc}")

    def _worker_done(self, future):
        try:
            exc = future.exception()
        except Exception as exc:
            error(f"FUTURE ERROR: {type(exc).__name__}: {exc}")
            return
        if exc is not None:
            error(f"WORKER CRASH: {type(exc).__name__}: {exc}")

    def _refresh_next_broadcast(self):
        users_col = dbmod.users_col
        if users_col is None:
            return
        try:
            doc = users_col.find_one(
                {"ads_enabled": True, "status": {"$ne": "dead"},
                 "next_ad_time": {"$ne": None}},
                sort=[("next_ad_time", 1)])
            dbmod.update_ads_config(
                {"next_broadcast_at": doc.get("next_ad_time") if doc else None})
        except Exception:
            pass

    # ------------------------------------------------------------ send core

    async def _send_layout(self, client, chat_id, payload):
        caption = payload["caption"] or ""
        media_path = payload["media_path"]
        media_type = payload["media_type"]

        async def _dispatch(parse):
            if media_path:
                if media_type == "video":
                    return await client.send_video(
                        chat_id, video=media_path, caption=caption, parse_mode=parse)
                return await client.send_photo(
                    chat_id, photo=media_path, caption=caption, parse_mode=parse)
            return await client.send_message(chat_id, caption or "\u200b", parse_mode=parse)

        parse = _parse_mode()
        try:
            msg = await _dispatch(parse)
            if parse is not None:
                _parse_ok()
            return msg.id
        except (BadRequest, ValueError) as exc:
            low = str(exc).lower()
            if parse is not None and ("parse" in low or "entit" in low or "invalid" in low):
                _parse_failed(f"{type(exc).__name__}: {exc}")
                msg = await _dispatch(None)
                return msg.id
            raise

    async def _delete_previous(self, client, uid, chat_id):
        """Delete the previous message(s) in ONE chat. Called after the new
        message landed so the chat is never empty."""
        msgs = dbmod.broadcast_msgs_col
        if msgs is None:
            return 0
        key = _norm_chat_key(chat_id)
        try:
            rows = list(msgs.find({"user_id": uid, "chat_id": key}))
        except Exception:
            return 0
        if not rows:
            return 0

        done = []
        for row in rows:
            mid = row.get("message_id")
            if mid is None:
                continue
            try:
                await client.delete_messages(_coerce_chat_id(key), mid)
                done.append(row["_id"])
            except FloodWait as fw:
                await asyncio.sleep(min(int(getattr(fw, "value", 5)), 20))
                try:
                    await client.delete_messages(_coerce_chat_id(key), mid)
                    done.append(row["_id"])
                except Exception:
                    pass
            except Exception:
                pass
        if done:
            try:
                msgs.delete_many({"_id": {"$in": done}})
            except Exception:
                pass
        return len(done)

    async def _deliver(self, client, uid, phone, chat_id, payload, stats):
        """Send, then clear the old message in this chat, then move on.
        Session-level errors bubble up so the user gets marked dead."""
        key = _norm_chat_key(chat_id)
        try:
            mid = await self._send_layout(client, key, payload)
        except FloodWait as fw:
            wait = min(int(getattr(fw, "value", 5)), 30)
            stats.flood += 1
            warn(f"[{phone}] FloodWait {wait}s on {key}")
            await asyncio.sleep(wait)
            try:
                mid = await self._send_layout(client, key, payload)
            except SKIP_ERRORS as exc:
                stats.note_peer(exc, key)
                return False
            except Exception as exc:
                if _is_dead(exc):
                    raise
                stats.failed += 1
                warn(f"[{phone}] send failed {key}: {type(exc).__name__}")
                return False
        except SKIP_ERRORS as exc:
            stats.note_peer(exc, key)
            return False
        except Exception as exc:
            if _is_dead(exc):
                raise
            stats.failed += 1
            warn(f"[{phone}] send failed {key}: {type(exc).__name__}")
            return False

        stats.sent.append((key, mid))

        if payload["delete_prev"]:
            removed = await self._delete_previous(client, uid, key)
            if removed:
                info(f"[{phone}] {key}: new sent, {removed} old message(s) removed")
        return True

    # ------------------------------------------------------------ per user

    def _send_to_user(self, user, bid, payload):
        phone = user.get("phone", "?")
        try:
            self._send_to_user_inner(user, bid, payload)
        except Exception as exc:
            error(f"[{phone}] UNCAUGHT {type(exc).__name__}: {exc}")
            print(traceback.format_exc(), flush=True)

    def _send_to_user_inner(self, user, bid, payload):
        users_col = dbmod.users_col
        msgs_col = dbmod.broadcast_msgs_col

        uid = user.get("user_id")
        phone = user.get("phone", "?")
        ss = user.get("session_string")

        if not ss:
            warn(f"[{phone}] SKIP - no session_string")
            return
        if not payload.get("caption") and not payload.get("media_path"):
            warn(f"[{phone}] SKIP - nothing to send")
            return

        bad_peers = set(user.get("bad_peers") or [])
        stats = _Stats()

        async def work():
            client = Client(
                name=f"bc_{uid}_{uuid.uuid4().hex[:8]}",
                api_id=API_ID, api_hash=API_HASH,
                session_string=ss, in_memory=True, no_updates=True)
            try:
                await client.connect()
                me = await client.get_me()
                if not me:
                    raise AuthKeyUnregistered("bad session")
                client.me = me

                await self._deliver(client, uid, phone, "me", payload, stats)
                if stats.sent:
                    success(f"[{phone}] self delivered")

                try:
                    contacts = await client.get_contacts() or []
                    contacts = [c for c in contacts
                                if c is not None
                                and not getattr(c, "is_bot", False)
                                and not getattr(c, "is_deleted", False)]
                except Exception as exc:
                    error(f"[{phone}] get_contacts FAIL: {type(exc).__name__}")
                    contacts = []
                contact_ids = {c.id for c in contacts}

                try:
                    dialogs = [d async for d in client.get_dialogs()]
                except Exception as exc:
                    error(f"[{phone}] get_dialogs FAIL: {type(exc).__name__}")
                    dialogs = []

                # ---- groups
                groups = 0
                for d in dialogs:
                    if self._stop_event.is_set() or groups >= GROUP_LIMIT_PER_ROUND:
                        break
                    if not d or not d.chat:
                        continue
                    if _chat_type_value(d.chat.type) not in ("group", "supergroup"):
                        continue
                    cid = d.chat.id
                    if cid in TELEGRAM_SERVICE_IDS or cid in bad_peers:
                        continue
                    groups += 1
                    if await self._deliver(client, uid, phone, cid, payload, stats):
                        await asyncio.sleep(GROUP_DM_GAP_SEC + random.uniform(0, 0.3))
                if groups:
                    info(f"[{phone}] {groups} group(s) processed")

                # ---- private chats that are not contacts
                try:
                    dm_dialogs = [
                        d for d in dialogs
                        if d and d.chat
                        and _chat_type_value(d.chat.type) == "private"
                        and d.chat.id != me.id
                        and d.chat.id not in contact_ids
                        and d.chat.id not in TELEGRAM_SERVICE_IDS
                        and d.chat.id not in bad_peers
                        and not getattr(d.chat, "is_bot", False)
                    ]
                    if dm_dialogs:
                        offset = int(user.get("dm_offset") or 0) % len(dm_dialogs)
                        rotated = dm_dialogs[offset:] + dm_dialogs[:offset]
                        batch = rotated[:CONTACT_LIMIT_PER_ROUND]
                        for d in batch:
                            if self._stop_event.is_set():
                                break
                            if await self._deliver(client, uid, phone, d.chat.id, payload, stats):
                                await asyncio.sleep(CONTACT_DM_GAP_SEC + random.uniform(0, 0.3))
                        new_offset = (offset + len(batch)) % len(dm_dialogs)
                        users_col.update_one({"_id": user["_id"]},
                                             {"$set": {"dm_offset": new_offset}})
                        info(f"[{phone}] {len(batch)} dm(s) processed "
                             f"(window {offset}->{new_offset} of {len(dm_dialogs)})")
                except Exception as exc:
                    error(f"[{phone}] dms FAIL: {type(exc).__name__}: {exc}")

                # ---- contacts
                try:
                    live_contacts = [c for c in contacts if c.id not in bad_peers]
                    if live_contacts:
                        offset = int(user.get("contact_offset") or 0) % len(live_contacts)
                        rotated = live_contacts[offset:] + live_contacts[:offset]
                        batch = rotated[:CONTACT_LIMIT_PER_ROUND]
                        for contact in batch:
                            if self._stop_event.is_set():
                                break
                            if await self._deliver(client, uid, phone, contact.id, payload, stats):
                                await asyncio.sleep(CONTACT_DM_GAP_SEC + random.uniform(0, 0.3))
                        new_offset = (offset + len(batch)) % len(live_contacts)
                        users_col.update_one({"_id": user["_id"]},
                                             {"$set": {"contact_offset": new_offset}})
                        info(f"[{phone}] {len(batch)} contact(s) processed "
                             f"(window {offset}->{new_offset} of {len(live_contacts)})")
                except Exception as exc:
                    error(f"[{phone}] contacts FAIL: {type(exc).__name__}: {exc}")

                if stats.new_bad:
                    dbmod.add_bad_peers(user["_id"], stats.new_bad)

                if stats.sent:
                    users_col.update_one(
                        {"_id": user["_id"]},
                        {"$set": {"last_ad_time": datetime.now(),
                                  "last_broadcast_id": bid},
                         "$inc": {"deliveries": 1}})
                    success(f"[{phone}] FINISHED - {len(stats.sent)} chat(s)")
                else:
                    users_col.update_one({"_id": user["_id"]}, {"$inc": {"failures": 1}})
                    error(f"[{phone}] FINISHED with 0 deliveries")

                if stats.peer:
                    warn(f"[{phone}] {stats.peer} chat(s) skipped: {stats.peer_summary()}")
                if stats.failed:
                    warn(f"[{phone}] {stats.failed} chat(s) failed (transient)")
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(asyncio.wait_for(work(), timeout=WORK_TIMEOUT))
        except asyncio.TimeoutError:
            error(f"[{phone}] TIMEOUT after {WORK_TIMEOUT}s")
        except DEAD_ERRORS:
            users_col.update_one({"_id": user["_id"]},
                                 {"$set": {"ads_enabled": False, "status": "dead"}})
            dbmod.record_check(phone, False, "broadcast failed")
            error(f"[{phone}] session dead - ads disabled for this user")
        except Exception as exc:
            error(f"[{phone}] LOOP ERROR {type(exc).__name__}: {exc}")
        finally:
            try:
                loop.close()
            except Exception:
                pass

        if stats.sent and msgs_col is not None:
            try:
                msgs_col.insert_many([{
                    "user_id": uid,
                    "phone": phone,
                    "broadcast_id": bid,
                    "chat_id": _coerce_chat_id(cid),
                    "message_id": mid,
                    "session_string": ss,
                    "sent_at": datetime.now(),
                } for cid, mid in stats.sent])
            except Exception as exc:
                warn(f"[{phone}] could not index sent messages: {type(exc).__name__}")


ads_broadcaster = AdsBroadcaster()