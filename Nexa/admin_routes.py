import os
import uuid
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify, send_from_directory
from bson import ObjectId
from bson.errors import InvalidId
from werkzeug.utils import secure_filename
from config import UPLOAD_FOLDER, MIN_INTERVAL_MIN, MAX_INTERVAL_MIN
from Nexa.database import (
    get_ads_config, update_ads_config, reset_ads_config, get_stats, get_all_users,
    users_col, broadcast_msgs_col, allowed_file
)
from Nexa.broadcaster import ads_broadcaster
from Nexa.session_manager import session_manager, session_cleaner

admin_bp = Blueprint('admin', __name__, template_folder='../templates')


def _photo_filename(path):
    """Return just the filename for a stored photo path, or None."""
    return os.path.basename(path) if path else None


def _valid_url(url):
    return isinstance(url, str) and url.startswith(("http://", "https://"))


@admin_bp.route("/")
def admin_panel():
    config = get_ads_config()
    stats = get_stats()
    stats['active_sessions'] = session_manager.get_active_count()
    stats['broadcaster_running'] = ads_broadcaster.is_running()
    return render_template("admin.html", config=config, stats=stats)


@admin_bp.route("/stats")
def admin_stats_page():
    stats = get_stats()
    users = get_all_users()
    config = get_ads_config()
    stats['active_sessions'] = session_manager.get_active_count()
    return render_template("stats.html", stats=stats, users=users, config=config)


@admin_bp.route("/api/config")
def api_config():
    config = get_ads_config()
    return jsonify({
        "success": True,
        "ads_enabled": config.get("ads_enabled", False),
        "interval": config.get("interval", 600),
        "interval_minutes": config.get("interval", 600) // 60,
        "caption": config.get("caption", ""),
        "photo_file": _photo_filename(config.get("photo_path")),
        "ads": config.get("ads", []),
        "broadcaster_running": ads_broadcaster.is_running(),
        "limits": {"min": MIN_INTERVAL_MIN, "max": MAX_INTERVAL_MIN}
    })


@admin_bp.route("/api/toggle-ads", methods=["POST"])
def toggle_ads():
    config = get_ads_config()
    new_status = not config.get("ads_enabled", False)

    if not update_ads_config({"ads_enabled": new_status}):
        return jsonify({"success": False, "error": "Failed to update config"}), 500

    if users_col is not None:
        users_col.update_many({}, {"$set": {"ads_enabled": new_status}})

    if new_status:
        ads_broadcaster.start()
        session_cleaner.start()
    else:
        ads_broadcaster.stop()
        session_cleaner.stop()

    return jsonify({
        "success": True,
        "enabled": new_status,
        "message": f"Broadcast {'started' if new_status else 'stopped'}"
    })


@admin_bp.route("/api/set-interval", methods=["POST"])
def set_interval():
    data = request.get_json(silent=True) or {}
    try:
        minutes = int(float(data.get("interval")))
    except (ValueError, TypeError):
        return jsonify({"success": False, "error": "Interval required"}), 400

    if minutes < MIN_INTERVAL_MIN or minutes > MAX_INTERVAL_MIN:
        return jsonify({
            "success": False,
            "error": f"Interval must be {MIN_INTERVAL_MIN}-{MAX_INTERVAL_MIN} minutes"
        }), 400

    if update_ads_config({"interval": minutes * 60}):
        return jsonify({
            "success": True,
            "interval": minutes * 60,
            "minutes": minutes,
            "message": f"Interval set to {minutes} minutes"
        })
    return jsonify({"success": False, "error": "Failed to update interval"}), 500


@admin_bp.route("/api/set-caption", methods=["POST"])
def set_caption():
    data = request.get_json(silent=True) or {}
    if update_ads_config({"caption": data.get("caption", "")}):
        return jsonify({"success": True, "message": "Caption updated!"})
    return jsonify({"success": False, "error": "Failed to update caption"}), 500


# ---------------- Advertisement CRUD ----------------

@admin_bp.route("/api/ads", methods=["POST"])
def add_ad():
    data = request.get_json(silent=True) or {}
    label = (data.get("label") or "").strip()
    url = (data.get("url") or "").strip()
    text = (data.get("text") or "").strip()

    if not label:
        return jsonify({"success": False, "error": "Button name is required"}), 400
    if not _valid_url(url):
        return jsonify({"success": False, "error": "URL must start with http:// or https://"}), 400

    config = get_ads_config()
    ads = config.get("ads", [])
    ads.append({"id": uuid.uuid4().hex[:8], "text": text, "label": label, "url": url})
    if update_ads_config({"ads": ads}):
        return jsonify({"success": True, "ads": ads, "message": "Advertisement added"})
    return jsonify({"success": False, "error": "Failed to add"}), 500


@admin_bp.route("/api/ads/<ad_id>", methods=["PUT"])
def edit_ad(ad_id):
    data = request.get_json(silent=True) or {}
    label = (data.get("label") or "").strip()
    url = (data.get("url") or "").strip()
    text = (data.get("text") or "").strip()

    if not label:
        return jsonify({"success": False, "error": "Button name is required"}), 400
    if not _valid_url(url):
        return jsonify({"success": False, "error": "URL must start with http:// or https://"}), 400

    config = get_ads_config()
    ads = config.get("ads", [])
    found = False
    for ad in ads:
        if ad.get("id") == ad_id:
            ad.update({"text": text, "label": label, "url": url})
            found = True
            break

    if not found:
        return jsonify({"success": False, "error": "Advertisement not found"}), 404
    if update_ads_config({"ads": ads}):
        return jsonify({"success": True, "ads": ads, "message": "Advertisement updated"})
    return jsonify({"success": False, "error": "Failed to update"}), 500


@admin_bp.route("/api/ads/<ad_id>", methods=["DELETE"])
def delete_ad(ad_id):
    config = get_ads_config()
    ads = config.get("ads", [])
    new_ads = [a for a in ads if a.get("id") != ad_id]

    if len(new_ads) == len(ads):
        return jsonify({"success": False, "error": "Advertisement not found"}), 404
    if update_ads_config({"ads": new_ads}):
        return jsonify({"success": True, "ads": new_ads, "message": "Advertisement deleted"})
    return jsonify({"success": False, "error": "Failed to delete"}), 500


@admin_bp.route("/api/reset-ads", methods=["POST"])
def reset_ads():
    if reset_ads_config():
        config = get_ads_config()
        return jsonify({
            "success": True,
            "ads": config.get("ads", []),
            "message": "All advertisements reset to defaults"
        })
    return jsonify({"success": False, "error": "Failed to reset"}), 500


# ---------------- Media ----------------

@admin_bp.route("/api/upload-photo", methods=["POST"])
def upload_photo():
    if 'photo' not in request.files:
        return jsonify({"success": False, "error": "No file"}), 400

    file = request.files['photo']
    if file.filename == '':
        return jsonify({"success": False, "error": "No file selected"}), 400
    if not (file and allowed_file(file.filename)):
        return jsonify({"success": False, "error": "Invalid file type"}), 400

    ext = file.filename.rsplit('.', 1)[1].lower()
    filename = secure_filename(f"ad_photo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{ext}")
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    filepath = os.path.join(UPLOAD_FOLDER, filename)
    file.save(filepath)

    update_ads_config({"photo_path": filepath})
    return jsonify({"success": True, "path": filepath, "photo_file": filename,
                    "message": "Photo uploaded!"})


@admin_bp.route("/api/remove-photo", methods=["POST"])
def remove_photo():
    update_ads_config({"photo_path": None})
    return jsonify({"success": True, "message": "Photo removed"})


@admin_bp.route("/uploads/<path:filename>")
def serve_upload(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


# ---------------- Preview / stats ----------------

@admin_bp.route("/api/preview")
def preview_ad():
    config = get_ads_config()
    return jsonify({
        "success": True,
        "photo_file": _photo_filename(config.get("photo_path")),
        "caption": config.get("caption", ""),
        "ads": config.get("ads", []),
        "interval_minutes": config.get("interval", 600) // 60,
        "ads_enabled": config.get("ads_enabled", False)
    })


@admin_bp.route("/api/user/<user_id>/toggle", methods=["POST"])
def toggle_user_ads(user_id):
    if users_col is None:
        return jsonify({"success": False, "error": "DB not connected"}), 500
    try:
        oid = ObjectId(user_id)
    except (InvalidId, TypeError):
        return jsonify({"success": False, "error": "Invalid user ID"}), 400

    user = users_col.find_one({"_id": oid})
    if not user:
        return jsonify({"success": False, "error": "User not found"}), 404

    new_status = not user.get("ads_enabled", True)
    users_col.update_one({"_id": oid}, {"$set": {"ads_enabled": new_status}})
    return jsonify({
        "success": True,
        "enabled": new_status,
        "message": f"User ads {'enabled' if new_status else 'disabled'}"
    })


@admin_bp.route("/api/stats/refresh")
def refresh_stats():
    stats = get_stats()
    stats['active_sessions'] = session_manager.get_active_count()
    stats['broadcaster_running'] = ads_broadcaster.is_running()
    return jsonify(stats)


@admin_bp.route("/api/clear-messages", methods=["POST"])
def clear_messages():
    if broadcast_msgs_col is None:
        return jsonify({"success": False, "error": "DB not connected"}), 500
    try:
        count = broadcast_msgs_col.count_documents({})
        broadcast_msgs_col.delete_many({})
        return jsonify({
            "success": True,
            "message": f"Cleared {count} pending messages",
            "cleared_count": count
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500