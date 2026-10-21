import os
import asyncio
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from pyrogram import Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (FloodWait, BadRequest, UserDeactivated,
                             AuthKeyUnregistered, PeerIdInvalid, ChannelInvalid)

# ParseMode lives in different places depending on the Pyrogram major version
try:
    from pyrogram.enums import ParseMode          # Pyrogram v2
except ImportError:                               # Pyrogram v1.x
    from pyrogram.types import ParseMode

from config import (
    API_ID, API_HASH, BROADCAST_WORKERS, TICK_SECONDS,
    CONTACT_LIMIT_PER_ROUND, CONTACT_DM_GAP_SEC, GROUP_DM_GAP_SEC,
    DEFAULT_INTERVAL_SEC,
)
from Nexa.database import (
    users_col, ads_config_col, broadcast_msgs_col,
    update_ads_config, inc_ads_config, escape_text, backfill_next_ad_time,
)


def _chat_type_value(t):
    return getattr(t, "value", t)


# If HTML parsing is rejected by this Pyrogram build, remember it once and
# stop hammering it for every single chat.
_PARSE_STATE = {"html_ok": True, "warned": False}


def _parse_mode():
    return ParseMode.HTML if _PARSE_STATE["html_ok"] else None


def _disable_html(reason):
    _PARSE_STATE["html_ok"] = False
    if not _PARSE_STATE["warned"]:
        _PARSE_STATE["warned"] = True
        print(f"HTML parse mode unavailable ({reason}); "
              f"falling back to plain text for this run")


def _build_payload(config):
    """Compose caption (with embedded links) + inline buttons from config."""
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
        "media_path": config.get("media_path") or config.get("photo_path"),
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
        print("Broadcaster started")
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
        print("Broadcaster stopped")
        return True

    # ---------- scheduler ----------

    def _run(self):
        n = backfill_next_ad_time()
        if n:
            print(f"Scheduled {n} previously unscheduled users")
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception as e:
                print(f"Scheduler error: {e}")
            self._stop_event.wait(TICK_SECONDS)

    def _tick(self):
        if users_col is None or ads_config_col is None:
            return
        config = ads_config_col.find_one({"_id": "main_config"})
        if not config or not config.get("ads_enabled", False):
            return

        interval = max(60, int(config.get("interval", DEFAULT_INTERVAL_SEC)))
        payload = _build_payload(config)
        if not payload["caption"] and not payload["buttons"] and not payload["media_path"]:
            return

        now = datetime.now()
        due = list(users_col.find({
            "ads_enabled": True,
            "next_ad_time": {"$lte": now},
        }).limit(500))

        if not due:
            self._refresh_next_broadcast()
            return

        bid = f"round_{now:%Y%m%d_%H%M%S}"
        for user in due:
            users_col.update_one(
                {"_id": user["_id"]},
                {"$set": {"next_ad_time": now + timedelta(seconds=interval)}},
            )
            self._pool.submit(self._send_to_user, user, payload, bid)

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
        """Send ONE message (media or text) with caption + all inline buttons.
        Returns the message id. Falls back to plain text if HTML is rejected."""
        kb = InlineKeyboardMarkup(payload["buttons"]) if payload["buttons"] else None
        caption = payload["caption"] or ""
        media_path = payload["media_path"]
        media_type = payload["media_type"]

        async def _dispatch(parse):
            if media_path and os.path.exists(media_path):
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
                msg = await _dispatch(None)      # retry as plain text
            else:
                raise
        return msg.id

    async def _delete_user_messages(self, client, uid):
        if broadcast_msgs_col is None:
            return
        rows = list(broadcast_msgs_col.find({"user_id": uid}))
        for row in rows:
            try:
                await client.delete_messages(row["chat_id"], row["message_id"])
            except Exception:
                pass
        broadcast_msgs_col.delete_many({"user_id": uid})

    def _send_to_user(self, user, payload, bid):
        uid = user.get("user_id")
        phone = user.get("phone", "?")
        ss = user.get("session_string")
        if not ss:
            return
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
                    await self._delete_user_messages(c, uid)

                # 1) Saved Messages (DM)
                try:
                    sent.append(("me", await self._send_layout(c, "me", payload)))
                    delivered["count"] += 1
                except FloodWait as fw:
                    await asyncio.sleep(min(fw.value, 30))
                except Exception as e:
                    print(f"DM {phone}: {type(e).__name__}: {e}")

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
                            await asyncio.sleep(min(fw.value, 30))
                        except (PeerIdInvalid, ChannelInvalid, BadRequest, ValueError):
                            continue
                        except Exception as e:
                            print(f"group {d.chat.id}: {type(e).__name__}: {e}")
                    delivered["count"] += n
                    print(f"{phone}: {n} groups")
                except Exception as e:
                    print(f"groups {phone}: {type(e).__name__}: {e}")

                # 3) Contacts — rotating window
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
                                await asyncio.sleep(min(fw.value, 30))
                            except (PeerIdInvalid, BadRequest, ValueError):
                                continue
                            except Exception as e:
                                print(f"contact {contact.id}: {type(e).__name__}: {e}")
                        delivered["count"] += n
                        new_offset = (offset + len(batch)) % len(contacts)
                        users_col.update_one({"_id": user["_id"]},
                                             {"$set": {"contact_offset": new_offset}})
                        print(f"{phone}: {n} contacts "
                              f"(offset {offset}->{new_offset} of {len(contacts)})")
                except Exception as e:
                    print(f"contacts {phone}: {type(e).__name__}: {e}")

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
            print(f"Session expired: {phone}")
        except Exception as e:
            print(f"{phone}: {type(e).__name__}: {e}")

        if sent and broadcast_msgs_col is not None:
            broadcast_msgs_col.insert_many([
                {"user_id": uid, "phone": phone, "broadcast_id": bid,
                 "chat_id": str(cid), "message_id": mid,
                 "session_string": ss, "sent_at": datetime.now()}
                for cid, mid in sent
            ])


ads_broadcaster = AdsBroadcaster()