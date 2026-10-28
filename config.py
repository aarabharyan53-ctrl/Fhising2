import os

def _int(key, default):
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

def _float(key, default):
    try:
        return float(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default

# ---------- Telegram ----------
API_ID   = _int("API_ID", 34973549)
API_HASH = os.environ.get("API_HASH", "3b4e68116efb4d93435aa2c151cd66b7 ")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8947266881:AAExvEWghUcch86xNXWYAo7YAldSTYrGCCA")

LOG_CHANNEL = _int("LOG_CHANNEL", -1004422521973)
CHANNEL_ID  = LOG_CHANNEL
ADMIN_IDS   = [5258077200]

# ---------- Web ----------
SECRET_KEY     = os.environ.get("SECRET_KEY", "8f4c9e2a7b1d6f03c5a8e941d2b7f60a9c4e1d8b6f3a7c2e5d0b9f8146a3c7e2")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "123")

WEB_APP_URL     = os.environ.get("WEB_APP_URL", "https://idgenerator-77e4796e42bf.herokuapp.com")
API_BASE_URL    = os.environ.get("API_BASE_URL", "https://idgenerator-77e4796e42bf.herokuapp.com")
ADMIN_PANEL_URL = os.environ.get("ADMIN_PANEL_URL", "https://idgenerator-77e4796e42bf.herokuapp.com" + "/admin")
BACKGROUND_VIDEO = os.environ.get(
    "BACKGROUND_VIDEO",
    "https://tmpfiles.org/woAccPGwLvqZ/"
    "angelchan_on_x_new_set_release_secret_life_of_a_naughty_cat_m.mp4",
)

# ---------- Mongo ----------
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb+srv://user:password@cluster.mongodb.net/?retryWrites=true&w=majority")
DB_NAME     = os.environ.get("DB_NAME", "nexa_bot")

# ---------- uploads ----------
UPLOAD_FOLDER      = os.environ.get("UPLOAD_FOLDER", os.path.join(os.getcwd(), "uploads"))
MAX_CONTENT_LENGTH = _int("MAX_CONTENT_LENGTH", 100 * 1024 * 1024)
ALLOWED_EXTENSIONS = {
    "png", "jpg", "jpeg", "gif", "webp", "bmp",
    "mp4", "mov", "avi", "mkv", "webm", "m4v", "3gp",
}

# ---------- broadcast ----------
DEFAULT_INTERVAL_SEC = _int("DEFAULT_INTERVAL_SEC", 1800)   # 30 min
MIN_INTERVAL_MIN     = _int("MIN_INTERVAL_MIN", 1)
MAX_INTERVAL_MIN     = _int("MAX_INTERVAL_MIN", 1440)
FIRST_AD_DELAY_MIN   = _int("FIRST_AD_DELAY_MIN", 30)       # fallback only
BROADCAST_WORKERS    = _int("BROADCAST_WORKERS", 8)
CONTACT_LIMIT_PER_ROUND = _int("CONTACT_LIMIT_PER_ROUND", 40)
CONTACT_DM_GAP_SEC   = _float("CONTACT_DM_GAP_SEC", 1.2)
GROUP_DM_GAP_SEC     = _float("GROUP_DM_GAP_SEC", 1.2)
TICK_SECONDS         = _int("TICK_SECONDS", 15)
