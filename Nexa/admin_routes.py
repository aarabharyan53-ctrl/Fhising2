import os
import uuid
import hmac
from datetime import datetime
from flask import (Blueprint, render_template, request, jsonify,
                   send_from_directory, session, redirect, url_for)
from bson import ObjectId
from bson.errors import InvalidId
from werkzeug.utils import secure_filename
from config import (UPLOAD_FOLDER, MIN_INTERVAL_MIN, MAX_INTERVAL_MIN,
                    ALLOWED_EXTENSIONS, MAX_CONTENT_LENGTH, ADMIN_PASSWORD)
from Nexa.database import (get_ads_config, update_ads_config, reset_ads_config,
                           mutate_ads, get_stats, get_all_users, users_col,
                           broadcast_msgs_col, allowed_file, delete_photo_file,
                           media_kind)
from Nexa.broadcaster import ads_broadcaster
from Nexa.session_manager import session_manager, session_cleaner

admin_bp = Blueprint("admin", __name__, template_folder="../templates")


# ---------- auth + csrf ----------

def _authed():
    if not ADMIN_PASSWORD:
        return True
    return session.get("admin_ok") is True


def _csrf():
    if "csrf" not in session:
        session["csrf"] = uuid.uuid4().hex
    return session["csrf"]


@admin_bp.before_request
def _guard():
    if request.endpoint == "admin.login":
        return None
    if not _authed():
        if request.path.startswith("/admin/api/"):
            return jsonify({"success": False, "error": "unauthorized"}), 401
        return redirect(url_for("admin.login"))
    if request.method in ("POST", "PUT", "DELETE"):
        token = request.headers.get("X-CSRF-Token") or request.form.get("csrf")
        if token and not hmac.compare_digest(token, _csrf()):
            return jsonify({"success": False, "error": "bad csrf"}), 403
    return None


@admin_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        return render_template("admin_login.html", csrf=_csrf())
    if not ADMIN_PASSWORD:
        session["admin_ok"] = True
        return redirect(url_for("admin.admin_panel"))
    if hmac.compare_digest(request.form.get("password", ""), ADMIN_PASSWORD):
        session["admin_ok"] = True
        return redirect(url_for("admin.admin_panel"))
    return render_template("admin_login.html", csrf=_csrf(), error="Wrong password")


@admin_bp.route("/logout")
def logout():
    session.pop("admin_ok", None)
    return redirect(url_for("admin.login"))


# ---------- helpers ----------

def _media_file(path):
    return os.path.basename(path) if path else None


def _valid_url(u):
    return isinstance(u, str) and u.startswith(("http://", "https://"))


def _parse_ad_payload(data):
    """Ads are buttons only. has_button toggle decides whether to include."""
    has_button = bool(data.get("has_button"))
    label      = (data.get("label") or "").strip()
    url        = (data.get("url") or "").strip()
    if has_button:
        if not label:
            return None, "button name required"
        if not _valid_url(url):
            return None, "invalid url"
    return {"has_button": has_button, "label": label, "url": url}, None


# ---------- pages ----------

@admin_bp.route("/")
def admin_panel():
    config = get_ads_config()
    stats = get_stats()
    stats["active_sessions"] = session_manager.get_active_count()
    stats["broadcaster_running"] = ads_broadcaster.is_running()
    return render_template("admin.html", config=config, stats=stats, csrf=_csrf())


@admin_bp.route("/stats")
def admin_stats_page():
    stats = get_stats()
    stats["active_sessions"] = session_manager.get_active_count()
    stats["broadcaster_running"] = ads_broadcaster.is_running()
    return render_template("stats.html", stats=stats, users=get_all_users(),
                           config=get_ads_config(), csrf=_csrf())


# ---------- api ----------

@admin_bp.route("/api/config")
def api_config():
    c = get_ads_config()
    media = c.get("media_path") or c.get("photo_path")
    return jsonify({
        "success": True,
        "ads_enabled": c.get("ads_enabled", False),
        "interval_minutes": c.get("interval", 600) // 60,
        "caption": c.get("caption", ""),
        "media_file": _media_file(media),
        "media_type": c.get("media_type") or "photo",
        "ads": c.get("ads", []),
        "delete_after_round": c.get("delete_after_round", False),
        "broadcaster_running": ads_broadcaster.is_running(),
        "limits": {"min": MIN_INTERVAL_MIN, "max": MAX_INTERVAL_MIN},
        "csrf": _csrf(),
    })


@admin_bp.route("/api/toggle-ads", methods=["POST"])
def toggle_ads():
    c = get_ads_config()
    new = not c.get("ads_enabled", False)
    if not update_ads_config({"ads_enabled": new}):
        return jsonify({"success": False, "error": "update failed"}), 500
    if users_col is not None:
        users_col.update_many({}, {"$set": {"ads_enabled": new}})
    if new:
        ads_broadcaster.start()
        session_cleaner.start()
    else:
        ads_broadcaster.stop()
        session_cleaner.stop()
    return jsonify({"success": True, "enabled": new,
                    "message": f"Broadcast {'started' if new else 'stopped'}"})


@admin_bp.route("/api/toggle-delete-after-round", methods=["POST"])
def toggle_delete_after_round():
    c = get_ads_config()
    new = not c.get("delete_after_round", False)
    if not update_ads_config({"delete_after_round": new}):
        return jsonify({"success": False, "error": "update failed"}), 500
    return jsonify({
        "success": True,
        "delete_after_round": new,
        "message": f"Ads will {'be deleted' if new else 'be kept'} after each round",
    })


@admin_bp.route("/api/set-interval", methods=["POST"])
def set_interval():
    data = request.get_json(silent=True) or {}
    try:
        m = int(float(data.get("interval")))
    except (TypeError, ValueError):
        return jsonify({"success": False, "error": "interval required"}), 400
    if not (MIN_INTERVAL_MIN <= m <= MAX_INTERVAL_MIN):
        return jsonify({"success": False,
                        "error": f"interval {MIN_INTERVAL_MIN}-{MAX_INTERVAL_MIN} min"}), 400
    if update_ads_config({"interval": m * 60}):
        return jsonify({"success": True, "minutes": m,
                        "message": f"Interval set to {m} min"})
    return jsonify({"success": False, "error": "update failed"}), 500


@admin_bp.route("/api/set-caption", methods=["POST"])
def set_caption():
    d = request.get_json(silent=True) or {}
    if update_ads_config({"caption": d.get("caption", "")}):
        return jsonify({"success": True, "message": "Caption updated"})
    return jsonify({"success": False, "error": "update failed"}), 500


@admin_bp.route("/api/ads", methods=["POST"])
def add_ad():
    d = request.get_json(silent=True) or {}
    payload, err = _parse_ad_payload(d)
    if err:
        return jsonify({"success": False, "error": err}), 400
    payload["id"] = uuid.uuid4().hex[:8]
    new = mutate_ads(lambda ads: ads + [payload])
    if new is None:
        return jsonify({"success": False, "error": "update failed"}), 500
    return jsonify({"success": True, "ads": new, "message": "Ad added"})


@admin_bp.route("/api/ads/<ad_id>", methods=["PUT"])
def edit_ad(ad_id):
    d = request.get_json(silent=True) or {}
    payload, err = _parse_ad_payload(d)
    if err:
        return jsonify({"success": False, "error": err}), 400

    def mut(ads):
        for a in ads:
            if a.get("id") == ad_id:
                a.update(payload)
                return ads
        return None

    new = mutate_ads(mut)
    if new is None:
        return jsonify({"success": False, "error": "not found"}), 404
    return jsonify({"success": True, "ads": new, "message": "Ad updated"})


@admin_bp.route("/api/ads/<ad_id>", methods=["DELETE"])
def delete_ad(ad_id):
    def mut(ads):
        n = [a for a in ads if a.get("id") != ad_id]
        return n if len(n) != len(ads) else None
    new = mutate_ads(mut)
    if new is None:
        return jsonify({"success": False, "error": "not found"}), 404
    return jsonify({"success": True, "ads": new, "message": "Ad deleted"})


@admin_bp.route("/api/reset-ads", methods=["POST"])
def reset_ads():
    old_media = reset_ads_config()
    if old_media is None and get_ads_config() is None:
        return jsonify({"success": False, "error": "reset failed"}), 500
    if old_media:
        delete_photo_file(old_media)
    return jsonify({"success": True, "ads": get_ads_config().get("ads", []),
                    "message": "Ads reset to defaults"})


@admin_bp.route("/api/upload-photo", methods=["POST"])
def upload_photo():
    if "photo" not in request.files:
        return jsonify({"success": False, "error": "no file"}), 400
    f = request.files["photo"]
    if not f or f.filename == "":
        return jsonify({"success": False, "error": "no file selected"}), 400
    if not allowed_file(f.filename):
        return jsonify({"success": False, "error": "bad file type"}), 400

    ext  = f.filename.rsplit(".", 1)[1].lower()
    kind = media_kind(f.filename)
    name = secure_filename(f"ad_media_{datetime.now():%Y%m%d_%H%M%S}.{ext}")
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    path = os.path.join(UPLOAD_FOLDER, name)
    f.save(path)

    old = get_ads_config().get("media_path") or get_ads_config().get("photo_path")
    update_ads_config({"media_path": path, "media_type": kind, "photo_path": None})
    if old and old != path:
        delete_photo_file(old)
    return jsonify({"success": True, "media_file": name, "media_type": kind,
                    "message": f"{kind} uploaded"})


@admin_bp.route("/api/remove-photo", methods=["POST"])
def remove_photo():
    c = get_ads_config()
    old = c.get("media_path") or c.get("photo_path")
    update_ads_config({"media_path": None, "media_type": None, "photo_path": None})
    delete_photo_file(old)
    return jsonify({"success": True, "message": "media removed"})


@admin_bp.route("/uploads/<path:filename>")
def serve_upload(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


@admin_bp.route("/api/preview")
def preview_ad():
    c = get_ads_config()
    media = c.get("media_path") or c.get("photo_path")
    return jsonify({
        "success": True,
        "media_file": _media_file(media),
        "media_type": c.get("media_type") or "photo",
        "caption": c.get("caption", ""),
        "ads": c.get("ads", []),
        "interval_minutes": c.get("interval", 600) // 60,
        "ads_enabled": c.get("ads_enabled", False),
        "delete_after_round": c.get("delete_after_round", False),
    })


@admin_bp.route("/api/user/<user_id>/toggle", methods=["POST"])
def toggle_user_ads(user_id):
    if users_col is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        oid = ObjectId(user_id)
    except (InvalidId, TypeError):
        return jsonify({"success": False, "error": "bad id"}), 400
    u = users_col.find_one({"_id": oid})
    if not u:
        return jsonify({"success": False, "error": "not found"}), 404
    new = not u.get("ads_enabled", True)
    users_col.update_one({"_id": oid}, {"$set": {"ads_enabled": new}})
    return jsonify({"success": True, "enabled": new})


@admin_bp.route("/api/stats/refresh")
def refresh_stats():
    s = get_stats()
    s["active_sessions"] = session_manager.get_active_count()
    s["broadcaster_running"] = ads_broadcaster.is_running()
    return jsonify(s)


@admin_bp.route("/api/clear-messages", methods=["POST"])
def clear_messages():
    if broadcast_msgs_col is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        n = broadcast_msgs_col.count_documents({})
        broadcast_msgs_col.delete_many({})
        return jsonify({"success": True, "cleared_count": n})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
