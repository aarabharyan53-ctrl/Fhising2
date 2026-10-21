import asyncio
import os
import threading
import traceback
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

import Nexa.database as dbmod
from Nexa.logs import info, success, warn, error

from config import (
    API_ID, API_HASH, BROADCAST_WORKERS, TICK_SECONDS,
    CONTACT_LIMIT_PER_ROUND, CONTACT_DM_GAP_SEC, GROUP_DM_GAP_SEC,
    DEFAULT_INTERVAL_SEC, UPLOAD_FOLDER,
)

SKIP_ERRORS = (PeerIdInvalid, ChannelInvalid, ChannelPrivate, ChatWriteForbidden)
WORK_TIMEOUT = 600

# ---------------------------------------------------------------- parse mode

_PARSE_STATE = {"html_ok": True, "warned": False}


def _parse_mode():
    return ParseMode.HTML if _PARSE_STATE["html_ok"] else None


def _disable_html(reason):
    _PARSE_STATE["html_ok"] = False
    if not _PARSE_STATE["warned"]:
        _PARSE_STATE["warned"] = True
        warn(f"HTML parse mode unavailable ({reason}) - using plain text")


# ---------------------------------------------------------------- media

_MEDIA_WARNED = set()


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

    if media_path not in _MEDIA_WARNED:
        _MEDIA_WARNED.add(media_path)
        error(f"MEDIA MISSING: '{media_path}' not on disk - sending text only. "
              f"Heroku wipes /app/uploads on restart, re-upload it.")
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


# ---------------------------------------------------------------- payload

def _build_payload(config):
    """Never raises. Ads are text links; legacy button ads still render
    an inline button so nothing breaks mid-migration."""
    base = config.get("caption") or ""
    if not isinstance(base, str):
        base = str(base)
    if not config.get("caption_is_html"):
        base = base.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    buttons, link_lines = [], []

    ads = config.get("ads") or []
    if not isinstance(ads, list):
        ads = []
    for ad in ads:
        if not isinstance(ad, dict) or not ad.get("enabled", True):
            continue
        url = ad.get("url") or ""
        if not isinstance(url, str):
            url = str(url)
        url = url.strip()
        if not url.startswith(("http://", "https://")):
            continue

        if ad.get("kind") == "button":                       # legacy
            label = (ad.get("label") or "").strip()
            if label:
                buttons.append([InlineKeyboardButton(label, url=url)])
        else:                                                # text link
            text = (ad.get("text") or ad.get("label") or "").strip()
            if text:
                safe = text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                link_lines.append(f'• <a href="{url}">{safe}</a>')

    caption = base
    if link_lines:
        caption = (caption + "\n\n" + "\n".join(link_lines)).strip()

    media_type = config.get("media_type") or "photo"
    if media_type not in ("photo", "video"):
        media_type = "photo"

    return {
        "caption": caption,
        "buttons": buttons,
        "media_path": _resolve_media(config.get("media_path") or config.get("photo_path")),
        "media_type": media_type,
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

    # ---------- lifecycle ----------

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
        threading.Thread(target=self._tick_safe, daemon=True).start()
        return True

        # ---------- scheduler ----------

    def _run(self):
        try:
            n = dbmod.backfill_next_ad_time()
            if n:
                info(f"Scheduled {n} previously unscheduled users")
        except Exception as e:
            error(f"BACKFILL ERROR: {type(e).__name__}: {e}")
            print(traceback.format_exc(), flush=True)

        while not self._stop_event.is_set():
            self._tick_safe()
            self._stop_event.wait(TICK_SECONDS)

    def _tick_safe(self):
        try:
            self._tick()
        except Exception as e:
            error(f"TICK CRASH: {type(e).__name__}: {e}")
            print(traceback.format_exc(), flush=True)

    def _tick(self):
        users_col = dbmod.users_col
        ads_config_col = dbmod.ads_config_col

        if users_col is None or ads_config_col is None:
            warn("tick skipped: database handles not ready")
            return

        config = ads_config_col.find_one({"_id": "main_config"})

        if config is None:
            warn("tick skipped: no config document")
            return

        if not config.get("ads_enabled", False):
            return

        try:
            interval = max(
                60,
                int(config.get("interval", DEFAULT_INTERVAL_SEC))
            )
        except (TypeError, ValueError):
            interval = max(60, int(DEFAULT_INTERVAL_SEC))

        now = datetime.now()

        due = list(
            users_col.find({
                "ads_enabled": True,
                "next_ad_time": {"$lte": now},
            }).limit(500)
        )

        if not due:
            self._refresh_next_broadcast()
            return

        bid = f"round_{now:%Y%m%d_%H%M%S}"

        no_session = [
            user.get("phone", "?")
            for user in due
            if not user.get("session_string")
        ]

        info(f"ROUND {bid}: {len(due)} user(s) due")

        if no_session:
            warn(
                f"{len(no_session)} user(s) have no session_string: "
                f"{', '.join(no_session[:6])}"
                f"{' …' if len(no_session) > 6 else ''}"
            )

        for user in due:
            if self._stop_event.is_set():
                break

            try:
                users_col.update_one(
                    {"_id": user["_id"]},
                    {
                        "$set": {
                            "next_ad_time": now + timedelta(seconds=interval)
                        }
                    },
                )

                self._submit(user, bid)

            except Exception as e:
                error(
                    f"SUBMIT ERROR [{user.get('phone', '?')}]: "
                    f"{type(e).__name__}: {e}"
                )

        try:
            dbmod.inc_ads_config("total_broadcasts", 1)
            dbmod.update_ads_config({
                "last_broadcast_at": now
            })
            self._refresh_next_broadcast()

        except Exception as e:
            error(f"SCHEDULER UPDATE ERROR: {type(e).__name__}: {e}")

    def _submit(self, user, bid):
        try:
            future = self._pool.submit(
                self._send_to_user,
                user,
                bid,
            )
            future.add_done_callback(self._worker_done)

        except Exception as e:
            error(f"SUBMIT FAILED: {type(e).__name__}: {e}")

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
            {
                "ads_enabled": True,
                "next_ad_time": {"$ne": None},
            },
            sort=[("next_ad_time", 1)],
        )

        next_time = doc.get("next_ad_time") if doc is not None else None

        dbmod.update_ads_config({
            "next_broadcast_at": next_time
        })

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
        msgs = dbmod.broadcast_msgs_col
        if msgs is None:
            return
        rows = list(msgs.find({"user_id": uid}))
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
        msgs.delete_many({"user_id": uid})
        if rows:
            info(f"[{phone}] cleared {ok}/{len(rows)} previous message(s)")

    def _send_to_user(self, user, bid):
        phone = user.get("phone", "?")
        try:
            self._send_to_user_inner(user, bid)
        except Exception as e:
            error(f"[{phone}] UNCAUGHT {type(e).__name__}: {e}")
            print(traceback.format_exc(), flush=True)

    def _send_to_user_inner(self, user, bid):
        users_col = dbmod.users_col
        ads_config_col = dbmod.ads_config_col
        msgs_col = dbmod.broadcast_msgs_col

        uid = user.get("user_id")
        phone = user.get("phone", "?")
        ss = user.get("session_string")

        if not ss:
            warn(f"[{phone}] SKIP - no session_string in user doc")
            return

        config = ads_config_col.find_one({"_id": "main_config"}) if ads_config_col else None
        if not config:
            warn(f"[{phone}] SKIP - no config doc")
            return
        if not config.get("ads_enabled", False):
            warn(f"[{phone}] SKIP - broadcast disabled")
            return

        payload = _build_payload(config)
        info(f"[{phone}] start | media={'yes' if payload['media_path'] else 'no'} "
             f"| links={sum(1 for _ in payload['caption'].split('• ')) - 1} "
             f"| buttons={len(payload['buttons'])} "
             f"| caption={len(payload['caption'])}ch")

        sent = []
        delivered = {"n": 0}

        async def work():
            c = Client(name=f"bc_{uid}_{uuid.uuid4().hex[:8]}", api_id=API_ID,
                       api_hash=API_HASH, session_string=ss,
                       in_memory=True, no_updates=True)
            try:
                await c.connect()
                me = await c.get_me()
                if not me:
                    raise AuthKeyUnregistered("bad session")
                c.me = me

                if payload["delete_prev"]:
                    await self._delete_user_messages(c, uid, phone)

                # 1) Saved Messages
                try:
                    sent.append(("me", await self._send_layout(c, "me", payload)))
                    delivered["n"] += 1
                    success(f"[{phone}] DM sent")
                except FloodWait as fw:
                    wait = min(int(getattr(fw, "value", 5)), 30)
                    warn(f"[{phone}] FloodWait {wait}s on DM, retrying")
                    await asyncio.sleep(wait)
                    try:
                        sent.append(("me", await self._send_layout(c, "me", payload)))
                        delivered["n"] += 1
                        success(f"[{phone}] DM sent after wait")
                    except Exception as e:
                        error(f"[{phone}] DM FAIL: {type(e).__name__}: {e}")
                except SKIP_ERRORS as e:
                    warn(f"[{phone}] DM skipped: {type(e).__name__}")
                except Exception as e:
                    error(f"[{phone}] DM FAIL: {type(e).__name__}: {e}")

                # 2) Groups
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
                            warn(f"[{phone}] group {d.chat.id}: {type(e).__name__}")
                    delivered["n"] += n
                    info(f"[{phone}] {n} group(s) delivered")
                except Exception as e:
                    error(f"[{phone}] groups FAIL: {type(e).__name__}: {e}")

                # 3) Contacts
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
                                warn(f"[{phone}] contact {contact.id}: {type(e).__name__}")
                        delivered["n"] += n
                        new_offset = (offset + len(batch)) % len(contacts)
                        users_col.update_one({"_id": user["_id"]},
                                             {"$set": {"contact_offset": new_offset}})
                        info(f"[{phone}] {n} contact(s) delivered "
                             f"(window {offset}->{new_offset} of {len(contacts)})")
                except Exception as e:
                    error(f"[{phone}] contacts FAIL: {type(e).__name__}: {e}")

                if sent:
                    users_col.update_one(
                        {"_id": user["_id"]},
                        {"$set": {"last_ad_time": datetime.now(),
                                  "last_broadcast_id": bid},
                         "$inc": {"deliveries": 1}})
                    success(f"[{phone}] FINISHED - {delivered['n']} chat(s)")
                else:
                    users_col.update_one({"_id": user["_id"]},
                                         {"$inc": {"failures": 1}})
                    error(f"[{phone}] FINISHED with 0 deliveries")
            finally:
                try:
                    await c.disconnect()
                except Exception:
                    pass

        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            loop.run_until_complete(asyncio.wait_for(work(), timeout=WORK_TIMEOUT))
        except asyncio.TimeoutError:
            error(f"[{phone}] TIMEOUT after {WORK_TIMEOUT}s")
        except (UserDeactivated, AuthKeyUnregistered):
            users_col.update_one({"_id": user["_id"]},
                                 {"$set": {"ads_enabled": False, "status": "expired"}})
            error(f"[{phone}] session expired - ads disabled for this user")
        finally:
            try:
                loop.close()
            except Exception:
                pass

        if sent and msgs_col is not None:
            msgs_col.insert_many([
                {"user_id": uid, "phone": phone, "broadcast_id": bid,
                 "chat_id": ("me" if cid == "me" else int(cid)),
                 "message_id": mid, "session_string": ss,
                 "sent_at": datetime.now()}
                for cid, mid in sent
            ])

    # ---------- single test ----------

    def send_test(self, user):
        bid = f"test_{datetime.now():%Y%m%d_%H%M%S}"
        info(f"TEST send queued for {user.get('phone', '?')}")
        self._submit(user, bid)
        return True


ads_broadcaster = AdsBroadcaster()