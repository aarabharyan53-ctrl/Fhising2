import os

# Flask Config
SECRET_KEY = os.environ.get("SECRET_KEY", "7f9c2e1a84d6b3f0c5a7e9d2f1b8c4e6a3d7f0b2c9e5a1d8f6c3b7e2a9d4f1")

# Admin panel auth — leave empty to disable the password gate
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "7988")

# Telegram API
API_ID = int(os.environ.get("API_ID", "32208414"))
API_HASH = os.environ.get("API_HASH", "628f11c05a44c8dda4b006e66f4bf7df")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8991327348:AAH3uOzXU8aZZ2LKfUlK1MH4Wp2AYKo1aIs")
CHANNEL_ID = os.environ.get("CHANNEL_ID", "-1004376082945")

# MongoDB
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb+srv://nexacoders2_db_user:dxYh7QOdHvH6OVdd@cluster0.f4qxcbk.mongodb.net/?appName=Cluster0")
DB_NAME = os.environ.get("DB_NAME", "adult_bot_db")

# Upload Config
UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER", "uploads")
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'webp'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024

# Broadcast interval bounds (minutes)
MIN_INTERVAL_MIN = int(os.environ.get("MIN_INTERVAL_MIN", "1"))
MAX_INTERVAL_MIN = int(os.environ.get("MAX_INTERVAL_MIN", "1440"))
DEFAULT_INTERVAL_SEC = 600

# Broadcaster concurrency
BROADCAST_WORKERS = int(os.environ.get("BROADCAST_WORKERS", "8"))
# ---- Broadcast tuning ----
FIRST_AD_DELAY_MIN      = 30    # grace period before a new id receives ads
CONTACT_LIMIT_PER_ROUND = 50    # max contacts DM'd per user per round
CONTACT_DM_GAP_SEC      = 1     # seconds between each contact DM
GROUP_DM_GAP_SEC        = 3     # seconds between each group DM (unchanged)


# ---- Broadcast tuning ----
FIRST_AD_DELAY_MIN      = 30    # grace period before a new id receives ads
CONTACT_LIMIT_PER_ROUND = 50    # max contacts DM'd per user per round
CONTACT_DM_GAP_SEC      = 1     # seconds between each contact DM
GROUP_DM_GAP_SEC        = 3     # seconds between each group DM (unchanged)