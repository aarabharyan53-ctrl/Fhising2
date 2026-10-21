import os
import asyncio
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from pyrogram import Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    FloodWait, BadRequest, UserDeactivated, AuthKeyUnregistered,
    PeerIdInvalid, ChannelInvalid, ChannelPrivate, ChatWriteForbidden,
)

try:
    from pyrogram.enums import ParseMode          # Pyrogram v2
except ImportError:                               # Pyrogram v1.x
    from pyrogram.types import ParseMode

from config import (
    API_ID, API_HASH, BROADCAST_WORKERS, TICK_SECONDS,
    CONTACT_LIMIT_PER_ROUND, CONTACT_DM_GAP_SEC, GROUP_DM_GAP_SEC,
    DEFAULT_INTERVAL_SEC, UPLOAD_FOLDER,
)
from Nexa.database import (
    users_col, ads_config_col, broadcast_msgs_col,
    update_ads_config, inc_ads_config, escape_text, backfill_next_ad_time,
)

# Chats that are permanently un-writable -> skip quietly
SKIP_ERRORS = (PeerIdInvalid, ChannelInvalid, ChannelPrivate, ChatWriteForbidden)


def log(msg):
    print(msg, flush=True)


def _chat_type_value(t):
    return getattr(t, "value", t)


def _coerce_chat_id(v):
    """Legacy rows stored chat ids as strings. Turn them back into ints so
    Pyrogram doesn't try to resolve them as usernames."""
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


# ---------------------------------------------------------------- parse mode

_PARSE_STATE = {"html_ok": True, "warned": False}


def _parse_mode():
    return ParseMode.HTML if _PARSE_STATE["html_ok"] else None


def _disable_html(reason):
    _PARSE_STATE["html_ok"] = False
    if not _PARSE_STATE["warned"]:
        _PARSE_STATE["warned"] = True
        log(f"HTML parse mode unavailable ({reason}); using plain text")


# ---------------------------------------------------------------- media

_MEDIA_WARNED = set()


def _resolve_media(media_path):
    """Find the media on disk no matter how the path was stored.
    Returns an absolute path, or None (with a one-time warning)."""
    if not media_path:
        return None

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

    if media_path not in _MEDIA_WARNED:
        _MEDIA_WARNED.add(media_path)
        log(f"MEDIA MISSING: '{media_path}' not found "
            f"(checked {len(candidates)} locations). Sending text only. "
            f"Heroku's disk is ephemeral - re-upload after a restart.")
    return None


# ---------------------------------------------------------------- payload

def _build_payload(config):
    base = config.get("caption") or ""
    if not config.get("caption_is_html"):
        base = escape_text(base)

    buttons = []
    link_lines = []

    for ad in config.get("ads", []):
        if not ad.get("enabled", True):
            continue
        url = (ad.get("url") or "").strip()
        if not url.startswith(("http://", "https://")):
            continue
        if ad.get("kind") == "button":
            label = (ad.get("label") or "").strip()
            if label:
                buttons.append([InlineKeyboardButton(label, url=url)])
        elif ad.get("kind") == "link":
            text = (ad.get("text") or "").strip()
            if text:
                link_lines.append(f'• <a href="{url}">{escape_text(text)}</a>')

    caption = base
    if link_lines:
        caption = (caption + "\n\n" + "\n".join(link_lines)).strip()

    return {
        "caption": caption,
        "buttons": buttons,
        "media_path": _resolve_media(config.get("media_path") or config.get("photo_path")),
        "media_type": config.get("media_type") or "photo",
        "delete_prev": bool(config.get("delete_after_round")),
    }


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
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        log("Broadcaster started")
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
        update_ads_config({"next_broadcast_at": None})
        log("Broadcaster stopped")
        return True

    # ---------- scheduler ----------

    def _run(self):
        n = backfill_next_ad_time()
        if n:
            log(f"Scheduled {n} previously unscheduled users")
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception as e:
                log(f"Scheduler error: {type(e).__name__}: {e}")
            self._stop_event.wait(TICK_SECONDS)

    def _tick(self):
        if users_col is None or ads_config_col is None:
            return
        config = ads_config_col.find_one({"_id": "main_config"})
        if not config or not config.get("ads_enabled", False):
            return

        interval = max(60, int(config.get("interval", DEFAULT_INTERVAL_SEC)))
        now = datetime.now()

        due = list(users_col.find({
            "ads_enabled": True,
            "next_ad_time": {"$lte": now},
        }).limit(500))

        if not due:
            self._refresh_next_broadcast()
            return

        bid = f"round_{now:%Y%m%d_%H%M%S}"
        log(f"round {bid}: {len(due)} due")

        for user in due:
            # reserve the slot so a slow send can't double-fire
            users_col.update_one(
                {"_id": user["_id"]},
                {"$set": {"next_ad_time": now + timedelta(seconds=interval)}},
            )
            self._pool.submit(self._send_to_user, user, bid)

        inc_ads_config("total_broadcasts", 1)
        update_ads_config({"last_broadcast_at": now})
        self._refresh_next_broadcast()

    def _refresh_next_broadcast(self):
        if users_col is None:
            return
        doc = users_col.find_one(
            {"ads_enabled": True, "next_ad_time": {"$ne": None}},
            sort=[("next_ad_time", 1)],
        )
        update_ads_config({"next_broadcast_at": doc["next_ad_time"] if doc else None})

    # ---------- sending ----------

    async def _send_layout(self, client, chat_id, payload):
        kb = InlineKeyboardMarkup(payload["buttons"]) if payload["buttons"] else None
        caption = payload["caption"] or ""
        media_path = payload["media_path"]
        media_type = payload["media_type"]

        async def _dispatch(parse):
            if media_path:
                if media_type == "video":
                    return await client.send_video(
                        chat_id, video=media_path, caption=caption,
                        reply_markup=kb, parse_mode=parse)
                return await client.send_photo(
                    chat_id, photo=media_path, caption=caption,
                    reply_markup=kb, parse_mode=parse)
            return await client.send_message(
                chat_id, caption or "\u200b", reply_markup=kb, parse_mode=parse)

        parse = _parse_mode()
        try:
            msg = await _dispatch(parse)
        except (BadRequest, ValueError) as e:
            low = str(e).lower()
            if parse is not None and ("parse" in low or "entit" in low or "invalid" in low):
                _disable_html(f"{type(e).__name__}: {e}")
                msg = await _dispatch(None)
            else:
                raise
        return msg.id

    async def _delete_user_messages(self, client, uid, phone):
        if broadcast_msgs_col is None:
            return
        rows = list(broadcast_msgs_col.find({"user_id": uid}))
        ok = 0
        for row in rows:
            try:
                await client.delete_messages(_coerce_chat_id(row["chat_id"]),
                                             row["message_id"])
                ok += 1
            except FloodWait as fw:
                await asyncio.sleep(min(int(getattr(fw, "value", 5)), 30))
                try:
                    await client.delete_messages(_coerce_chat_id(row["chat_id"]),
                                                 row["message_id"])
                    ok += 1
                except Exception:
                    pass
            except Exception:
                pass
        broadcast_msgs_col.delete_many({"user_id": uid})
        if rows:
            log(f"cleared {ok}/{len(rows)} old msgs for {phone}")

    def _send_to_user(self, user, bid):
        uid = user.get("user_id")
        phone = user.get("phone", "?")
        ss = user.get("session_string")
        if not ss:
            return

        # Fresh config read per user -> media/caption changes apply immediately
        config = ads_config_col.find_one({"_id": "main_config"}) if ads_config_col else None
        if not config or not config.get("ads_enabled", False):
            return
        payload = _build_payload(config)

        sent = []
        delivered = {"count": 0}

        async def work():
            c = Client(name=f"bc_{uid}_{uuid.uuid4().hex[:8]}", api_id=API_ID,
                       api_hash=API_HASH, session_string=ss,
                       in_memory=True, no_updates=True)
            try:
                await c.connect()
                me = await c.get_me()
                if not me:
                    raise AuthKeyUnregistered("bad session")
                c.me = me  # required: fixes is_premium access in Pyrogram

                if payload["delete_prev"]:
                    await self._delete_user_messages(c, uid, phone)

                # 1) Saved Messages  ------------------------------------------------
                try:
                    sent.append(("me", await self._send_layout(c, "me", payload)))
                    delivered["count"] += 1
                    log(f"DM ok: {phone}")
                except FloodWait as fw:
                    wait = min(int(getattr(fw, "value", 5)), 30)
                    log(f"FloodWait {wait}s on DM {phone}, retrying")
                    await asyncio.sleep(wait)
                    try:
                        sent.append(("me", await self._send_layout(c, "me", payload)))
                        delivered["count"] += 1
                        log(f"DM ok after wait: {phone}")
                    except Exception as e:
                        log(f"DM FAIL {phone}: {type(e).__name__}: {e}")
                except SKIP_ERRORS as e:
                    log(f"DM skip {phone}: {type(e).__name__}: {e}")
                except Exception as e:
                    log(f"DM FAIL {phone}: {type(e).__name__}: {e}")

                # 2) Groups  --------------------------------------------------------
                try:
                    n = 0
                    async for d in c.get_dialogs():
                        if self._stop_event.is_set():
                            break
                        if not d or not d.chat:
                            continue
                        if _chat_type_value(d.chat.type) not in ("group", "supergroup"):
                            continue
                        try:
                            sent.append((d.chat.id,
                                         await self._send_layout(c, d.chat.id, payload)))
                            n += 1
                            await asyncio.sleep(GROUP_DM_GAP_SEC)
                        except FloodWait as fw:
                            await asyncio.sleep(min(int(getattr(fw, "value", 5)), 30))
                        except SKIP_ERRORS:
                            continue
                        except Exception as e:
                            log(f"group {d.chat.id}: {type(e).__name__}: {e}")
                    delivered["count"] += n
                    log(f"{phone}: {n} groups")
                except Exception as e:
                    log(f"groups {phone}: {type(e).__name__}: {e}")

                # 3) Contacts (rotating window)  ------------------------------------
                try:
                    contacts = await c.get_contacts() or []
                    contacts = [x for x in contacts
                                if x is not None
                                and not getattr(x, "is_bot", False)
                                and not getattr(x, "is_deleted", False)]
                    if contacts:
                        offset = int(user.get("contact_offset") or 0) % len(contacts)
                        rotated = contacts[offset:] + contacts[:offset]
                        batch = rotated[:CONTACT_LIMIT_PER_ROUND]
                        n = 0
                        for contact in batch:
                            if self._stop_event.is_set():
                                break
                            try:
                                sent.append((contact.id,
                                             await self._send_layout(c, contact.id, payload)))
                                n += 1
                                await asyncio.sleep(CONTACT_DM_GAP_SEC)
                            except FloodWait as fw:
                                await asyncio.sleep(min(int(getattr(fw, "value", 5)), 30))
                            except SKIP_ERRORS:
                                continue
                            except Exception as e:
                                log(f"contact {contact.id}: {type(e).__name__}: {e}")
                        delivered["count"] += n
                        new_offset = (offset + len(batch)) % len(contacts)
                        users_col.update_one({"_id": user["_id"]},
                                             {"$set": {"contact_offset": new_offset}})
                        log(f"{phone}: {n} contacts "
                            f"(offset {offset}->{new_offset} of {len(contacts)})")
                except Exception as e:
                    log(f"contacts {phone}: {type(e).__name__}: {e}")

                if sent:
                    users_col.update_one(
                        {"_id": user["_id"]},
                        {"$set": {"last_ad_time": datetime.now(),
                                  "last_broadcast_id": bid},
                         "$inc": {"deliveries": 1}})
                else:
                    users_col.update_one({"_id": user["_id"]},
                                         {"$inc": {"failures": 1}})
            finally:
                try:
                    await c.disconnect()
                except Exception:
                    pass

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(work())
            loop.close()
        except (UserDeactivated, AuthKeyUnregistered):
            users_col.update_one({"_id": user["_id"]},
                                 {"$set": {"ads_enabled": False, "status": "expired"}})
            log(f"Session expired: {phone}")
        except Exception as e:
            log(f"{phone}: {type(e).__name__}: {e}")

        # store real ints (or "me") so deletion never has to resolve a username
        if sent and broadcast_msgs_col is not None:
            broadcast_msgs_col.insert_many([
                {"user_id": uid, "phone": phone, "broadcast_id": bid,
                 "chat_id": ("me" if cid == "me" else int(cid)),
                 "message_id": mid, "session_string": ss,
                 "sent_at": datetime.now()}
                for cid, mid in sent
            ])


ads_broadcaster = AdsBroadcaster()