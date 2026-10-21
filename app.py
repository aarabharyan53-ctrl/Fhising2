import os
from flask import Flask

from config import SECRET_KEY, UPLOAD_FOLDER, MAX_CONTENT_LENGTH, BACKGROUND_VIDEO
from Nexa.auth_routes import auth_bp
from Nexa.admin_routes import admin_bp

app = Flask(__name__)
app.secret_key = SECRET_KEY
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH
app.config["SESSION_COOKIE_SAMESITE"] = "None"   # required for Telegram WebApp iframe
app.config["SESSION_COOKIE_SECURE"] = True

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp, url_prefix="/admin")


@app.context_processor
def inject_globals():
    return {"bg_video": BACKGROUND_VIDEO}


def init_system():
    try:
        from Nexa.database import get_ads_config
        from Nexa.broadcaster import ads_broadcaster
        from Nexa.session_manager import session_cleaner
        if get_ads_config().get("ads_enabled", False):
            ads_broadcaster.start()
            session_cleaner.start()
            print("auto-started broadcaster")
    except Exception as e:
        print(f"startup: {e}")


init_system()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))