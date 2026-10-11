import os
import asyncio
import threading
import uuid
from datetime import datetime
from pyrogram import Client
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pyrogram.errors import (
    FloodWait, BadRequest, UserDeactivated, AuthKeyUnregistered,
    MsgIdInvalid, PeerIdInvalid, ChannelInvalid
)
from config import API_ID, API_HASH
from Nexa.database import users_col, ads_config_col, broadcast_msgs_col


def _chat_type_value(chat_type):
    """Pyrogram ChatType can be an enum or a str — normalize to a plain string."""
    return getattr(chat_type, "value", chat_type)


def _clean_ads(config):
    """Keep only ads that have a label and a valid URL."""
    out = []
    for ad in config.get("ads", []):
        label = (ad.get("label") or "").strip()
        url = (ad.get("url") or "").strip()
        if label and url.startswith(("http://", "https://")):
            out.append({
                "text": (ad.get("text") or "").strip(),
                "label": label,
                "url": url
            })
    return out


class AdsBroadcaster:
    def __init__(self):
        self._running = False
        self._thread = None
        self._stop_event = threading.Event()
        self._round_count = 0
        self._lock = threading.Lock()

    def is_running(self):
        return self._running

    def start(self):
        with self._lock:
            if self._running:
                return False  # prevent duplicate jobs
            self._stop_event.clear()
            self._running = True
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        print("📢 Ads Broadcaster started")
        return True

    def stop(self):
        with self._lock:
            if not self._running:
                return False
            self._stop_event.set()
            self._running = False
            thread = self._thread
        if thread:
            thread.join(timeout=5)
        print("📢 Ads Broadcaster stopped (future rounds cancelled)")
        return True

    def _get_interval(self):
        if ads_config_col is not None:
            config = ads_config_col.find_one({"_id": "main_config"})
            if config:
                return max(1, int(config.get("interval", 600)))
        return 600

    def _wait(self, seconds):
        """Wait up to `seconds`, waking every second. Returns True if stopped.
        Re-reads the interval so live changes take effect."""
        elapsed = 0
        while not self._stop_event.is_set():
            if elapsed >= self._get_interval():
                return False
            self._stop_event.wait(1)
            elapsed += 1
        return True

    def _run(self):
        interval = self._get_interval()
        print(f"⏳ First broadcast in {interval // 60} min {interval % 60}s...")
        while not self._stop_event.is_set():
            # FIRST wait, THEN broadcast -> initial delay is respected
            if self._wait(self._get_interval()):
                break
            if self._stop_event.is_set():
                break
            try:
                self._run_round()
            except Exception as e:
                print(f"Broadcast round error: {e}")
            nxt = self._get_interval()
            print(f"⏳ Round complete. Next in {nxt // 60} min...")

    # ---------- round ----------

    def _run_round(self):
        self._round_count += 1
        broadcast_id = f"round_{self._round_count}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        print(f"\n{'=' * 60}\n🚀 BROADCAST ROUND #{self._round_count} ({broadcast_id})\n{'=' * 60}")

        self._delete_previous_messages()

        if users_col is None or ads_config_col is None:
            print("❌ DB not connected")
            return

        config = ads_config_col.find_one({"_id": "main_config"})
        if not config or not config.get("ads_enabled", False):
            print("❌ Ads disabled")
            return

        caption = (config.get("caption") or "").strip()
        ads = _clean_ads(config)
        photo_path = config.get("photo_path")

        if not caption and not ads and not photo_path:
            print("📭 Nothing to send (no caption, ads, or image)")
            return

        raw_users = list(users_col.find({"ads_enabled": True}))

        users_to_send = []
        for user in raw_users:
            if self._verify_user_session(user):
                users_to_send.append(user)
            else:
                users_col.update_one(
                    {"_id": user["_id"]},
                    {"$set": {"ads_enabled": False, "status": "expired"}}
                )
                print(f"❌ Session expired for {user.get('phone', 'Unknown')}, disabled")

        if not users_to_send:
            print("📭 No valid users to broadcast")
            return

        print(f"📨 Broadcasting to {len(users_to_send)} valid users...")
        for user in users_to_send:
            if self._stop_event.is_set():
                print("⏹️ Broadcast stopped")
                break
            threading.Thread(
                target=self._send_to_user,
                args=(user, caption, ads, photo_path, broadcast_id),
                daemon=True
            ).start()

    def _verify_user_session(self, user):
        user_id = user.get("user_id")
        phone = user.get("phone", "Unknown")
        session_string = user.get("session_string")

        if not session_string:
            return False

        async def check():
            client = Client(
                name=f"check_{user_id}_{uuid.uuid4().hex[:6]}",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=session_string,
                in_memory=True,
                no_updates=True
            )
            try:
                await client.connect()
                me = await client.get_me()
                return me is not None
            except (AuthKeyUnregistered, UserDeactivated):
                return False
            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(check())
            loop.close()
            return result
        except Exception as e:
            print(f"⚠️ Session check failed for {phone}: {e}")
            return False

    def _delete_previous_messages(self):
        if broadcast_msgs_col is None:
            return

        old_msgs = list(broadcast_msgs_col.find())
        if not old_msgs:
            print("📝 No old messages to delete")
            return

        print(f"🗑️ Deleting {len(old_msgs)} old broadcast messages...")
        deleted_count = 0

        for msg_data in old_msgs:
            session_string = msg_data.get("session_string")
            chat_id = msg_data.get("chat_id")
            message_id = msg_data.get("message_id")

            if not all([session_string, chat_id, message_id]):
                broadcast_msgs_col.delete_one({"_id": msg_data["_id"]})
                continue

            async def delete_msg():
                client = Client(
                    name=f"del_{uuid.uuid4().hex[:8]}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    session_string=session_string,
                    in_memory=True,
                    no_updates=True
                )
                try:
                    await client.connect()
                    await client.delete_messages(chat_id, message_id)
                    return True
                except Exception:
                    return False
                finally:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                success = loop.run_until_complete(delete_msg())
                loop.close()
                if success:
                    deleted_count += 1
                    broadcast_msgs_col.delete_one({"_id": msg_data["_id"]})
            except Exception as e:
                print(f"⚠️ Failed to delete message: {e}")

        print(f"✅ Deleted {deleted_count} old messages")

    async def _send_layout(self, client, chat_id, caption, ads, photo_path):
        """Send the main message, then each ad as its own message with the
        button directly below its own text. Returns list of message ids."""
        sent = []

        # 1) Main message (photo + caption, or text)
        if photo_path and os.path.exists(photo_path):
            m = await client.send_photo(chat_id, photo=photo_path, caption=caption or "")
        else:
            m = await client.send_message(chat_id, caption or " ")
        sent.append(m.id)

        # 2) Each ad section beneath the main message
        for ad in ads:
            text = ad["text"] or ad["label"]
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(ad["label"], url=ad["url"])]])
            m = await client.send_message(chat_id, text, reply_markup=kb)
            sent.append(m.id)

        return sent

    def _send_to_user(self, user, caption, ads, photo_path, broadcast_id):
        user_id = user.get("user_id")
        phone = user.get("phone", "Unknown")
        session_string = user.get("session_string")

        if not session_string:
            print(f"⚠️ No session string for {phone}")
            return

        sent_messages = []  # list of (chat_id, message_id)

        try:
            async def send_ad():
                client = Client(
                    name=f"temp_{user_id}_{uuid.uuid4().hex[:8]}",
                    api_id=API_ID,
                    api_hash=API_HASH,
                    session_string=session_string,
                    in_memory=True,
                    no_updates=True
                )

                try:
                    await client.connect()

                    me = await client.get_me()
                    if not me:
                        raise AuthKeyUnregistered("Invalid session")

                    # 1. Send to Saved Messages (DM)
                    try:
                        ids = await self._send_layout(client, "me", caption, ads, photo_path)
                        for mid in ids:
                            sent_messages.append(("me", mid))
                        print(f"✅ DM sent to {phone}")
                    except Exception as e:
                        print(f"❌ DM failed for {phone}: {e}")

                    # 2. Send to Joined Groups
                    try:
                        group_count = 0

                        async for dialog in client.get_dialogs():
                            if self._stop_event.is_set():
                                break

                            if dialog is None or dialog.chat is None:
                                continue

                            # enum-safe check (fixes "0 groups")
                            if _chat_type_value(dialog.chat.type) not in ("group", "supergroup"):
                                continue

                            chat_id = dialog.chat.id
                            try:
                                ids = await self._send_layout(client, chat_id, caption, ads, photo_path)
                                for mid in ids:
                                    sent_messages.append((chat_id, mid))
                                group_count += 1
                                await asyncio.sleep(3)

                            except (PeerIdInvalid, ChannelInvalid, ValueError) as e:
                                print(f"⚠️ Skipping bad peer {chat_id} for {phone}: {e}")
                                continue
                            except FloodWait as fw:
                                print(f"⏳ FloodWait in group for {phone}: {fw.value}s")
                                await asyncio.sleep(min(fw.value, 30))
                            except BadRequest as e:
                                print(f"⚠️ BadRequest in group for {phone}: {e}")
                                continue
                            except Exception as e:
                                print(f"⚠️ Group send failed for {phone} in {chat_id}: {e}")
                                continue

                        print(f"✅ Sent to {group_count} groups for {phone}")

                    except Exception as e:
                        print(f"❌ Groups failed for {phone}: {e}")

                    if users_col is not None and sent_messages:
                        users_col.update_one(
                            {"_id": user["_id"]},
                            {
                                "$set": {
                                    "last_ad_time": datetime.now(),
                                    "last_broadcast_id": broadcast_id
                                },
                                "$inc": {"total_ads_sent": len(sent_messages)}
                            }
                        )

                except UserDeactivated:
                    raise
                except AuthKeyUnregistered:
                    raise
                except Exception as e:
                    print(f"❌ Error for {phone}: {e}")
                    raise
                finally:
                    try:
                        await client.disconnect()
                    except Exception:
                        pass

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(send_ad())
            loop.close()

            if broadcast_msgs_col is not None and sent_messages:
                for chat_id, mid in sent_messages:
                    broadcast_msgs_col.insert_one({
                        "user_id": user_id,
                        "phone": phone,
                        "broadcast_id": broadcast_id,
                        "chat_id": str(chat_id),
                        "message_id": mid,
                        "session_string": session_string,
                        "sent_at": datetime.now()
                    })

        except UserDeactivated:
            if users_col is not None:
                users_col.update_one(
                    {"_id": user["_id"]},
                    {"$set": {"ads_enabled": False, "status": "deactivated"}}
                )
            print(f"❌ User {phone} deactivated")
        except AuthKeyUnregistered:
            if users_col is not None:
                users_col.update_one(
                    {"_id": user["_id"]},
                    {"$set": {"ads_enabled": False, "status": "expired"}}
                )
            print(f"❌ Session expired for {phone}")
        except FloodWait as e:
            print(f"⏳ Flood wait for {phone}: {e.value}s")
        except PeerIdInvalid as e:
            print(f"❌ Peer ID invalid for {phone}: {e}")
        except Exception as e:
            print(f"❌ Failed to send to {phone}: {e}")


ads_broadcaster = AdsBroadcaster()