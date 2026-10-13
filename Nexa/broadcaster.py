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
from config import API_ID, API_HASH, BROADCAST_WORKERS
from Nexa.database import (users_col, ads_config_col, broadcast_msgs_col,
                           update_ads_config, inc_ads_config)


def _chat_type_value(t):
    return getattr(t, "value", t)


def _clean_ads(config):
    out = []
    for ad in config.get("ads", []):
        label = (ad.get("label") or "").strip()
        url = (ad.get("url") or "").strip()
        if label and url.startswith(("http://", "https://")):
            out.append({"text": (ad.get("text") or "").strip(),
                        "label": label, "url": url})
    return out


class AdsBroadcaster:
    def __init__(self):
        self._running = False
        self._thread = None
        self._stop_event = threading.Event()
        self._round_count = 0
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
        print("📢 Broadcaster started")
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
        print("📢 Broadcaster stopped")
        return True

    def _get_interval(self):
        if ads_config_col is not None:
            c = ads_config_col.find_one({"_id": "main_config"})
            if c:
                return max(1, int(c.get("interval", 600)))
        return 600

    def _wait(self, seconds):
        elapsed = 0
        while not self._stop_event.is_set():
            if elapsed >= self._get_interval():
                return False
            self._stop_event.wait(1)
            elapsed += 1
        return True

    def _run(self):
        while not self._stop_event.is_set():
            secs = self._get_interval()
            nxt = datetime.now() + timedelta(seconds=secs)
            update_ads_config({"next_broadcast_at": nxt})
            print(f"⏳ Next broadcast at {nxt:%H:%M:%S}")
            if self._wait(secs):
                break
            if self._stop_event.is_set():
                break
            try:
                self._run_round()
            except Exception as e:
                print(f"Round error: {e}")

    def _run_round(self):
        self._round_count += 1
        bid = f"round_{self._round_count}_{datetime.now():%Y%m%d_%H%M%S}"
        print(f"\n{'='*50}\n🚀 ROUND #{self._round_count} ({bid})\n{'='*50}")
        self._delete_previous_messages()

        if users_col is None or ads_config_col is None:
            return
        config = ads_config_col.find_one({"_id": "main_config"})
        if not config or not config.get("ads_enabled", False):
            return

        caption = (config.get("caption") or "").strip()
        ads = _clean_ads(config)
        photo = config.get("photo_path")
        if not caption and not ads and not photo:
            print("📭 nothing to send")
            return

        users = list(users_col.find({"ads_enabled": True}))
        valid = [u for u in users if self._verify_user_session(u)]
        for u in users:
            if u not in valid:
                users_col.update_one({"_id": u["_id"]},
                                     {"$set": {"ads_enabled": False, "status": "expired"}})

        if not valid:
            print("📭 no valid users")
            return

        print(f"📨 sending to {len(valid)} users")
        for u in valid:
            if self._stop_event.is_set():
                break
            self._pool.submit(self._send_to_user, u, caption, ads, photo, bid)

        inc_ads_config("total_broadcasts", 1)
        update_ads_config({"last_broadcast_at": datetime.now()})

    def _verify_user_session(self, user):
        ss = user.get("session_string")
        if not ss:
            return False

        async def check():
            c = Client(name=f"chk_{uuid.uuid4().hex[:8]}", api_id=API_ID,
                       api_hash=API_HASH, session_string=ss,
                       in_memory=True, no_updates=True)
            try:
                await c.connect()
                return (await c.get_me()) is not None
            except (AuthKeyUnregistered, UserDeactivated):
                return False
            except Exception:
                return True          # M8: transient → keep
            finally:
                try:
                    await c.disconnect()
                except Exception:
                    pass

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            r = loop.run_until_complete(check())
            loop.close()
            return r
        except Exception:
            return True

    def _delete_previous_messages(self):
        if broadcast_msgs_col is None:
            return
        old = list(broadcast_msgs_col.find())
        if not old:
            return
        print(f"🗑️ deleting {len(old)} old messages")

        async def wipe():
            c = Client(name=f"del_{uuid.uuid4().hex[:8]}", api_id=API_ID,
                       api_hash=API_HASH, session_string=old[0]["session_string"],
                       in_memory=True, no_updates=True)
            try:
                await c.connect()
                c.me = await c.get_me()
                return True
            finally:
                pass

        for m in old:
            async def one(m=m):
                c = Client(name=f"del_{uuid.uuid4().hex[:8]}", api_id=API_ID,
                           api_hash=API_HASH, session_string=m["session_string"],
                           in_memory=True, no_updates=True)
                try:
                    await c.connect()
                    c.me = await c.get_me()
                    await c.delete_messages(m["chat_id"], m["message_id"])
                    return True
                except Exception:
                    return False
                finally:
                    try:
                        await c.disconnect()
                    except Exception:
                        pass

            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                ok = loop.run_until_complete(one())
                loop.close()
                if ok:
                    broadcast_msgs_col.delete_one({"_id": m["_id"]})
            except Exception as e:
                print(f"⚠️ delete failed: {e}")

    async def _send_layout(self, client, chat_id, caption, ads, photo):
        sent = []
        if photo and os.path.exists(photo):
            m = await client.send_photo(chat_id, photo=photo, caption=caption or "")
        else:
            m = await client.send_message(chat_id, caption or " ")
        sent.append(m.id)
        for ad in ads:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton(ad["label"], url=ad["url"])]])
            m = await client.send_message(chat_id, ad["text"] or ad["label"], reply_markup=kb)
            sent.append(m.id)
        return sent

    def _send_to_user(self, user, caption, ads, photo, bid):
        uid, phone, ss = user.get("user_id"), user.get("phone", "?"), user.get("session_string")
        if not ss:
            return
        sent = []

        async def work():
            c = Client(name=f"bc_{uid}_{uuid.uuid4().hex[:8]}", api_id=API_ID,
                       api_hash=API_HASH, session_string=ss,
                       in_memory=True, no_updates=True)
            try:
                await c.connect()
                me = await c.get_me()
                if not me:
                    raise AuthKeyUnregistered("bad session")
                c.me = me  # fixes is_premium crash

                try:
                    for mid in await self._send_layout(c, "me", caption, ads, photo):
                        sent.append(("me", mid))
                except Exception as e:
                    print(f"❌ DM {phone}: {e}")

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
                            for mid in await self._send_layout(c, d.chat.id, caption, ads, photo):
                                sent.append((d.chat.id, mid))
                            n += 1
                            await asyncio.sleep(3)
                        except (PeerIdInvalid, ChannelInvalid, ValueError):
                            continue
                        except FloodWait as fw:
                            await asyncio.sleep(min(fw.value, 30))
                        except Exception as e:
                            print(f"⚠️ group {d.chat.id}: {e}")
                    print(f"✅ {phone}: {n} groups")
                except Exception as e:
                    print(f"❌ groups {phone}: {e}")

                if sent:
                    users_col.update_one(
                        {"_id": user["_id"]},
                        {"$set": {"last_ad_time": datetime.now(), "last_broadcast_id": bid},
                         "$inc": {"deliveries": 1}})
                else:
                    users_col.update_one({"_id": user["_id"]}, {"$inc": {"failures": 1}})
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
        except Exception as e:
            print(f"❌ {phone}: {e}")

        if broadcast_msgs_col is not None and sent:
            broadcast_msgs_col.insert_many([
                {"user_id": uid, "phone": phone, "broadcast_id": bid,
                 "chat_id": str(cid), "message_id": mid,
                 "session_string": ss, "sent_at": datetime.now()}
                for cid, mid in sent])


ads_broadcaster = AdsBroadcaster()