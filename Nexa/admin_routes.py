import os
import uuid
import hmac
from datetime import datetime, timedelta

from flask import (Blueprint, render_template, request, jsonify,
                   send_from_directory, session, redirect, url_for)
from bson import ObjectId
from bson.errors import InvalidId
from werkzeug.utils import secure_filename

import Nexa.database as dbmod
from config import (UPLOAD_FOLDER, MIN_INTERVAL_MIN, MAX_INTERVAL_MIN,
                    ADMIN_PASSWORD, BACKGROUND_VIDEO)
from Nexa.database import (get_ads_config, update_ads_config, reset_ads_config,
                           mutate_ads, get_stats, get_all_users, allowed_file,
                           delete_photo_file, media_kind, get_videos, update_videos,
                           reschedule_all_after_interval_change)
from Nexa.broadcaster import ads_broadcaster
from Nexa.session_manager import session_manager, session_cleaner
from Nexa.logs import (get_logs, clear_logs, get_counters, reset_counters,
                       last_error, info, warn, error)

admin_bp = Blueprint("admin", __name__, template_folder="../templates")


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
        return render_template("admin_login.html", csrf=_csrf(), bg_video=BACKGROUND_VIDEO)
    if not ADMIN_PASSWORD:
        session["admin_ok"] = True
        return redirect(url_for("admin.admin_panel"))
    if hmac.compare_digest(request.form.get("password", ""), ADMIN_PASSWORD):
        session["admin_ok"] = True
        return redirect(url_for("admin.admin_panel"))
    return render_template("admin_login.html", csrf=_csrf(), error="Wrong password",
                           bg_video=BACKGROUND_VIDEO)


@admin_bp.route("/logout")
def logout():
    session.pop("admin_ok", None)
    return redirect(url_for("admin.login"))


def _media_file(path):
    return os.path.basename(path) if path else None


def _valid_url(u):
    return isinstance(u, str) and u.startswith(("http://", "https://"))


def _pull_schedule_forward(max_wait_sec=90, new_wait_sec=30):
    users_col = dbmod.users_col
    if users_col is None:
        return 0
    try:
        cutoff = datetime.now() + timedelta(seconds=max_wait_sec)
        target = datetime.now() + timedelta(seconds=new_wait_sec)
        r = users_col.update_many(
            {"ads_enabled": True, "next_ad_time": {"$gt": cutoff}},
            {"$set": {"next_ad_time": target}},
        )
        return r.modified_count
    except Exception as e:
        warn(f"reschedule forward failed: {e}")
        return 0


def _parse_ad_payload(data):
    text = (data.get("text") or data.get("label") or "").strip()
    url = (data.get("url") or "").strip()
    enabled = data.get("enabled", True)
    if not text:
        return None, "link text required"
    if not _valid_url(url):
        return None, "url must start with http:// or https://"
    return {"text": text, "url": url, "enabled": bool(enabled)}, None


def _media_state():
    c = get_ads_config()
    p = c.get("media_path") or c.get("photo_path")
    if not p:
        return {"configured": False, "exists": False, "file": None}
    exists = False
    for cand in (p, os.path.join(UPLOAD_FOLDER, os.path.basename(p))):
        try:
            if cand and os.path.isfile(cand):
                exists = True
                break
        except (OSError, ValueError):
            pass
    return {"configured": True, "exists": exists, "file": os.path.basename(p)}


@admin_bp.route("/")
def admin_panel():
    config = get_ads_config()
    stats = get_stats()
    stats["active_sessions"] = session_manager.get_active_count()
    stats["broadcaster_running"] = ads_broadcaster.is_running()
    return render_template("admin.html", config=config, stats=stats,
                           videos=get_videos(), csrf=_csrf(),
                           bg_video=BACKGROUND_VIDEO,
                           initial_logs=get_logs(limit=150),
                           counters=get_counters())


@admin_bp.route("/stats")
def admin_stats_page():
    stats = get_stats()
    stats["active_sessions"] = session_manager.get_active_count()
    stats["broadcaster_running"] = ads_broadcaster.is_running()
    return render_template("stats.html", stats=stats, users=get_all_users(),
                           config=get_ads_config(), csrf=_csrf(),
                           bg_video=BACKGROUND_VIDEO)


@admin_bp.route("/api/config")
def api_config():
    c = get_ads_config()
    media = c.get("media_path") or c.get("photo_path")
    return jsonify({
        "success": True,
        "ads_enabled": c.get("ads_enabled", False),
        "interval_minutes": c.get("interval", 1800) // 60,
        "caption": c.get("caption", ""),
        "caption_is_html": c.get("caption_is_html", False),
        "media_file": _media_file(media),
        "media_type": c.get("media_type") or "photo",
        "ads": c.get("ads", []),
        "videos": get_videos(),
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
    if dbmod.users_col is not None:
        dbmod.users_col.update_many({}, {"$set": {"ads_enabled": new}})
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
    current = bool((get_ads_config() or {}).get("delete_after_round", False))
    new = not current

    # Use update_ads_config so the in-memory cache is invalidated + upsert happens.
    if not update_ads_config({"delete_after_round": new}):
        error("delete toggle: update_ads_config returned falsy")
        return jsonify({"success": False, "error": "update failed"}), 500

    # Read back the persisted value so the UI always reflects reality.
    persisted = bool((get_ads_config() or {}).get("delete_after_round", False))

    info(f"Delete previous ads: {'ON' if persisted else 'OFF'}")
    return jsonify({
        "success": True,
        "delete_after_round": persisted,
        "message": f"Previous ads will {'be deleted' if persisted else 'be kept'} before each send",
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
        reschedule_all_after_interval_change(m * 60)
        info(f"Interval set to {m} min")
        return jsonify({"success": True, "minutes": m,
                        "message": f"Interval set to {m} min"})
    return jsonify({"success": False, "error": "update failed"}), 500


@admin_bp.route("/api/set-caption", methods=["POST"])
def set_caption():
    d = request.get_json(silent=True) or {}
    if update_ads_config({
        "caption": d.get("caption", ""),
        "caption_is_html": bool(d.get("caption_is_html", False)),
    }):
        n = _pull_schedule_forward()
        info(f"Caption updated ({n} user(s) rescheduled)")
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
    info(f"Ad link added: {payload['text']}")
    return jsonify({"success": True, "ads": new, "message": "Link ad added"})


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
    return jsonify({"success": True, "ads": new, "message": "Link ad updated"})


@admin_bp.route("/api/ads/<ad_id>", methods=["DELETE"])
def delete_ad(ad_id):
    def mut(ads):
        n = [a for a in ads if a.get("id") != ad_id]
        return n if len(n) != len(ads) else None
    new = mutate_ads(mut)
    if new is None:
        return jsonify({"success": False, "error": "not found"}), 404
    return jsonify({"success": True, "ads": new, "message": "Link ad deleted"})


@admin_bp.route("/api/reset-ads", methods=["POST"])
def reset_ads():
    old_media = reset_ads_config()
    if old_media is None and get_ads_config() is None:
        return jsonify({"success": False, "error": "reset failed"}), 500
    if old_media:
        delete_photo_file(old_media)
    info("Ads reset to defaults")
    return jsonify({"success": True, "ads": get_ads_config().get("ads", []),
                    "message": "Ads reset to defaults"})


@admin_bp.route("/api/videos", methods=["GET"])
def api_videos():
    return jsonify({"success": True, "videos": get_videos()})


@admin_bp.route("/api/videos", methods=["POST"])
def api_save_videos():
    d = request.get_json(silent=True) or {}
    vids = d.get("videos") or []
    if not isinstance(vids, list):
        return jsonify({"success": False, "error": "videos must be a list"}), 400
    for v in vids:
        url = (v.get("url") or "").strip()
        if url and not _valid_url(url):
            return jsonify({"success": False, "error": f"bad url: {url}"}), 400
    if not update_videos(vids):
        return jsonify({"success": False, "error": "update failed"}), 500
    info("Web videos updated")
    return jsonify({"success": True, "videos": get_videos(), "message": "Videos saved"})


@admin_bp.route("/api/upload-photo", methods=["POST"])
def upload_photo():
    if "photo" not in request.files:
        return jsonify({"success": False, "error": "no file"}), 400
    f = request.files["photo"]
    if not f or f.filename == "":
        return jsonify({"success": False, "error": "no file selected"}), 400
    if not allowed_file(f.filename):
        return jsonify({"success": False, "error": "bad file type"}), 400

    ext = f.filename.rsplit(".", 1)[1].lower()
    kind = media_kind(f.filename)
    name = secure_filename(f"ad_media_{datetime.now():%Y%m%d_%H%M%S}.{ext}")
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    path = os.path.join(UPLOAD_FOLDER, name)
    f.save(path)

    old = get_ads_config().get("media_path") or get_ads_config().get("photo_path")
    update_ads_config({"media_path": path, "media_type": kind, "photo_path": None})
    if old and old != path:
        delete_photo_file(old)

    n = _pull_schedule_forward()
    info(f"{kind} uploaded ({name}) - {n} user(s) rescheduled")
    return jsonify({"success": True, "media_file": name, "media_type": kind,
                    "message": f"{kind} uploaded"})


@admin_bp.route("/api/remove-photo", methods=["POST"])
def remove_photo():
    c = get_ads_config()
    old = c.get("media_path") or c.get("photo_path")
    update_ads_config({"media_path": None, "media_type": None, "photo_path": None})
    delete_photo_file(old)
    _pull_schedule_forward()
    info("Media removed")
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
        "caption_is_html": c.get("caption_is_html", False),
        "ads": c.get("ads", []),
        "interval_minutes": c.get("interval", 1800) // 60,
        "ads_enabled": c.get("ads_enabled", False),
        "delete_after_round": c.get("delete_after_round", False),
    })


@admin_bp.route("/api/broadcast-now", methods=["POST"])
def broadcast_now():
    users_col = dbmod.users_col
    if users_col is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        n = users_col.update_many(
            {"ads_enabled": True},
            {"$set": {"next_ad_time": datetime.now()}},
        ).modified_count
    except Exception as e:
        error(f"broadcast-now failed: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

    if not ads_broadcaster.is_running():
        ads_broadcaster.start()
    ads_broadcaster.run_now()
    info(f"Manual broadcast queued for {n} user(s)")
    return jsonify({"success": True, "queued": n,
                    "message": f"{n} user(s) queued - watch the log below"})


@admin_bp.route("/api/test-send/<user_id>", methods=["POST"])
def test_send(user_id):
    users_col = dbmod.users_col
    if users_col is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        oid = ObjectId(user_id)
    except (InvalidId, TypeError):
        return jsonify({"success": False, "error": "bad id"}), 400
    u = users_col.find_one({"_id": oid})
    if not u:
        return jsonify({"success": False, "error": "not found"}), 404
    if not u.get("session_string"):
        warn(f"test send: {u.get('phone')} has no session_string")
        return jsonify({"success": False,
                        "error": "this user has no session_string - re-link them"}), 400
    ads_broadcaster.send_test(u)
    return jsonify({"success": True, "message": f"Test queued for {u.get('phone')}"})


@admin_bp.route("/api/user/<user_id>/toggle", methods=["POST"])
def toggle_user_ads(user_id):
    users_col = dbmod.users_col
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


@admin_bp.route("/api/user/<user_id>/reschedule", methods=["POST"])
def reschedule_user(user_id):
    users_col = dbmod.users_col
    if users_col is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        oid = ObjectId(user_id)
    except (InvalidId, TypeError):
        return jsonify({"success": False, "error": "bad id"}), 400
    users_col.update_one({"_id": oid}, {"$set": {"next_ad_time": datetime.now()}})
    return jsonify({"success": True, "message": "queued for next tick"})


@admin_bp.route("/api/stats/refresh")
def refresh_stats():
    s = get_stats()
    s["active_sessions"] = session_manager.get_active_count()
    s["broadcaster_running"] = ads_broadcaster.is_running()
    return jsonify(s)


@admin_bp.route("/api/logs")
def api_logs():
    try:
        since = int(request.args.get("since", 0))
    except (TypeError, ValueError):
        since = 0
    level = request.args.get("level") or "all"
    return jsonify({
        "success": True,
        "logs": get_logs(since_id=since, limit=400, level=level),
        "counters": get_counters(),
        "last_error": last_error(),
    })


@admin_bp.route("/api/logs/clear", methods=["POST"])
def api_logs_clear():
    n = clear_logs()
    reset_counters()
    return jsonify({"success": True, "cleared": n, "message": f"{n} log line(s) cleared"})


@admin_bp.route("/api/diagnostics")
def diagnostics():
    users_col = dbmod.users_col
    cfg = get_ads_config()
    media = _media_state()

    total = subs = no_session = due = 0
    if users_col is not None:
        try:
            total = users_col.count_documents({})
            subs = users_col.count_documents({"ads_enabled": True})
            no_session = dbmod.count_users_missing_session()
            due = dbmod.count_due_now()
        except Exception:
            pass

    ads = cfg.get("ads", [])
    active_links = sum(1 for a in ads if a.get("enabled", True)
                       and (a.get("url") or "").startswith("http"))

    checks = []

    def chk(ok, label, detail=""):
        checks.append({"ok": bool(ok), "label": label, "detail": detail})

    chk(dbmod.db_connected, "MongoDB connected")
    chk(ads_broadcaster.is_running(), "Broadcaster running")
    chk(cfg.get("ads_enabled", False), "Broadcast enabled")
    chk(subs > 0, "Has subscribed users", f"{subs}")
    chk(no_session == 0, "All users have a session_string",
        f"{no_session} missing" if no_session else "")
    chk(not media["configured"] or media["exists"], "Media file present on disk",
        media["file"] or "none")
    chk(active_links > 0, "At least one active ad link", f"{active_links}")
    chk(bool((cfg.get("caption") or "").strip()) or bool(active_links) or media["configured"],
        "Something to send (caption / link / media)")

    le = last_error()

    return jsonify({
        "success": True,
        "db_connected": dbmod.db_connected,
        "broadcaster_running": ads_broadcaster.is_running(),
        "ads_enabled": cfg.get("ads_enabled", False),
        "interval_minutes": cfg.get("interval", 1800) // 60,
        "next_broadcast_at": (cfg.get("next_broadcast_at").strftime("%Y-%m-%d %H:%M:%S")
                              if isinstance(cfg.get("next_broadcast_at"), datetime) else None),
        "total_users": total,
        "subscribed": subs,
        "users_without_session": no_session,
        "due_now": due,
        "active_links": active_links,
        "media": media,
        "counters": get_counters(),
        "last_error": le,
        "checks": checks,
    })


@admin_bp.route("/api/clear-messages", methods=["POST"])
def clear_messages():
    msgs = dbmod.broadcast_msgs_col
    if msgs is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        n = msgs.count_documents({})
        msgs.delete_many({})
        return jsonify({"success": True, "cleared_count": n})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500