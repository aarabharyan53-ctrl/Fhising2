import os

from flask import Flask, jsonify, request

from config import SECRET_KEY, UPLOAD_FOLDER, MAX_CONTENT_LENGTH, BACKGROUND_VIDEO
from Nexa.auth_routes import auth_bp
from Nexa.admin_routes import admin_bp

try:
    from werkzeug.middleware.proxy_fix import ProxyFix
except Exception:
    ProxyFix = None

app = Flask(__name__)
app.secret_key = SECRET_KEY or os.urandom(32).hex()
app.config.update(
    UPLOAD_FOLDER=UPLOAD_FOLDER,
    MAX_CONTENT_LENGTH=MAX_CONTENT_LENGTH,
    SESSION_COOKIE_SAMESITE="None",   # Telegram WebApp iframe ke liye
    SESSION_COOKIE_SECURE=True,       # https par hi chalega
    SESSION_COOKIE_HTTPONLY=True,
    PREFERRED_URL_SCHEME="https",
    JSON_SORT_KEYS=False,
)

if ProxyFix:
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app.register_blueprint(auth_bp)
app.register_blueprint(admin_bp, url_prefix="/admin")


@app.context_processor
def inject_globals():
    return {"bg_video": BACKGROUND_VIDEO}


@app.route("/healthz")
def healthz():
    return jsonify({"ok": True})


@app.route("/favicon.ico")
def favicon():
    return ("", 204)


@app.errorhandler(404)
def _404(e):
    if request.path.startswith("/admin/api/"):
        return jsonify({"success": False, "error": "not found"}), 404
    return "404", 404


@app.errorhandler(500)
def _500(e):
    if request.path.startswith("/admin/api/"):
        return jsonify({"success": False, "error": "server error"}), 500
    return "500 - check logs", 500


def init_system():
    try:
        from Nexa.database import get_ads_config
        from Nexa.broadcaster import ads_broadcaster
        from Nexa.session_manager import session_cleaner
        if (get_ads_config() or {}).get("ads_enabled", False):
            ads_broadcaster.start()
            session_cleaner.start()
            print("auto-started broadcaster")
    except Exception as e:
        print(f"startup: {e}")


init_system()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))