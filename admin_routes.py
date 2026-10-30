import hmac
import os
import uuid
from datetime import datetime

from flask import (Blueprint, render_template, request, jsonify,
                   send_from_directory, send_file, session, redirect, url_for)
from bson import ObjectId
from bson.errors import InvalidId
from werkzeug.utils import secure_filename

import Nexa.database as dbmod
from config import (UPLOAD_FOLDER, MIN_INTERVAL_MIN, MAX_INTERVAL_MIN,
                    ADMIN_PASSWORD, BACKGROUND_VIDEO)
from Nexa.database import (get_ads_config, update_ads_config, reset_ads_config,
                           mutate_ads, get_stats, get_all_users, allowed_file,
                           delete_photo_file, media_kind, get_videos, update_videos,
                           reschedule_all_after_interval_change,
                           get_active_sessions_for_export, media_exists)
from Nexa.broadcaster import ads_broadcaster
from Nexa.session_manager import (session_manager, session_cleaner,
                                  build_sessions_zip)
from Nexa.session_tools import check_many
from Nexa.logs import (get_logs, clear_logs, get_counters, reset_counters,
                       last_error, sources, info, warn, error)

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
        if request.path.startswith("/admin/api/") or "/api/" in request.path:
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


def _mask(phone):
    phone = (phone or "").lstrip("+")
    if not phone:
        return ""
    if len(phone) <= 7:
        return f"+{phone}"
    return f"+{phone[:4]}{'•' * (len(phone) - 7)}{phone[-3:]}"


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
    return {"configured": True, "exists": media_exists(p),
            "file": os.path.basename(p)}


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
    enabled, hours = dbmod.get_auto_logout()
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
        "delete_after_round": c.get("delete_after_round", True),
        "auto_logout_enabled": enabled,
        "auto_logout_hours": hours,
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
        dbmod.users_col.update_many({"status": {"$ne": "dead"}},
                                    {"$set": {"ads_enabled": new}})
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
    current = dbmod.get_delete_after_round()
    new = not current

    persisted = dbmod.set_delete_after_round(new)
    if persisted is None:
        error("delete toggle: write did not land")
        return jsonify({"success": False, "error": "update failed"}), 500

    info(f"Delete previous ads: {'ON' if persisted else 'OFF'}")
    return jsonify({
        "success": True,
        "delete_after_round": persisted,
        "message": ("New message is sent first, then the old one is removed"
                    if persisted else "Old ads are kept"),
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
        n = dbmod.pull_schedule_forward()
        info(f"Caption updated ({n} user(s) rescheduled)")
        return jsonify({"success": True, "message": "Caption updated"})
    return jsonify({"success": False, "error": "update failed"}), 500


@admin_bp.route("/api/auto-logout", methods=["POST"])
def api_auto_logout():
    d = request.get_json(silent=True) or {}
    enabled, hours = dbmod.get_auto_logout()
    if "enabled" in d:
        enabled = bool(d.get("enabled"))
    if "hours" in d:
        try:
            hours = max(1, min(720, int(float(d.get("hours")))))
        except (TypeError, ValueError):
            return jsonify({"success": False, "error": "hours must be 1-720"}), 400
    if not dbmod.set_auto_logout(enabled, hours):
        return jsonify({"success": False, "error": "update failed"}), 500
    info(f"Auto logout {'ON' if enabled else 'OFF'} ({hours}h)")
    return jsonify({"success": True, "enabled": enabled, "hours": hours,
                    "message": f"Auto logout {'enabled' if enabled else 'disabled'} ({hours}h)"})


@admin_bp.route("/api/ads", methods=["POST"])
def add_ad():
    d = request.get_json(silent=True) or {}
    payload, err = _parse_ad_payload(d)
    if err:
        return jsonify({"success": False, "error": err}), 400
    payload["id"] = uuid.uuid4().hex[:8]
    payload["kind"] = "link"
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
    info(f"Ad link updated: {payload['text']}")
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
        url = (v.get("url") or "").strip() if isinstance(v, dict) else ""
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

    cfg = get_ads_config()
    old = cfg.get("media_path") or cfg.get("photo_path")
    update_ads_config({"media_path": path, "media_type": kind, "photo_path": None})
    if old and old != path:
        delete_photo_file(old)

    n = dbmod.pull_schedule_forward()
    info(f"{kind} uploaded ({name}) - {n} user(s) rescheduled")
    return jsonify({"success": True, "media_file": name, "media_type": kind,
                    "message": f"{kind} uploaded"})


@admin_bp.route("/api/remove-photo", methods=["POST"])
def remove_photo():
    c = get_ads_config()
    old = c.get("media_path") or c.get("photo_path")
    update_ads_config({"media_path": None, "media_type": None, "photo_path": None})
    delete_photo_file(old)
    dbmod.pull_schedule_forward()
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
        "media_exists": media_exists(media),
        "caption": c.get("caption", ""),
        "caption_is_html": c.get("caption_is_html", False),
        "ads": c.get("ads", []),
        "interval_minutes": c.get("interval", 1800) // 60,
        "ads_enabled": c.get("ads_enabled", False),
        "delete_after_round": c.get("delete_after_round", True),
    })


@admin_bp.route("/api/broadcast-now", methods=["POST"])
def broadcast_now():
    if dbmod.users_col is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        n = dbmod.users_col.update_many(
            {"ads_enabled": True, "status": {"$ne": "dead"}},
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
    if dbmod.users_col is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        oid = ObjectId(user_id)
    except (InvalidId, TypeError):
        return jsonify({"success": False, "error": "bad id"}), 400
    u = dbmod.users_col.find_one({"_id": oid})
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
    if dbmod.users_col is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        oid = ObjectId(user_id)
    except (InvalidId, TypeError):
        return jsonify({"success": False, "error": "bad id"}), 400
    u = dbmod.users_col.find_one({"_id": oid})
    if not u:
        return jsonify({"success": False, "error": "not found"}), 404
    new = not u.get("ads_enabled", True)
    dbmod.users_col.update_one({"_id": oid}, {"$set": {"ads_enabled": new}})
    return jsonify({"success": True, "enabled": new})


@admin_bp.route("/api/user/<user_id>/reschedule", methods=["POST"])
def reschedule_user(user_id):
    if dbmod.users_col is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        oid = ObjectId(user_id)
    except (InvalidId, TypeError):
        return jsonify({"success": False, "error": "bad id"}), 400
    dbmod.users_col.update_one({"_id": oid}, {"$set": {"next_ad_time": datetime.now()}})
    return jsonify({"success": True, "message": "queued for next tick"})


@admin_bp.route("/api/user/<user_id>/reset-peers", methods=["POST"])
def reset_user_peers(user_id):
    if dbmod.users_col is None:
        return jsonify({"success": False, "error": "db down"}), 500
    try:
        oid = ObjectId(user_id)
    except (InvalidId, TypeError):
        return jsonify({"success": False, "error": "bad id"}), 400
    dbmod.clear_bad_peers(oid)
    return jsonify({"success": True, "message": "skipped-chat cache cleared"})


@admin_bp.route("/api/sessions")
def api_sessions():
    pending = session_manager.list_sessions()
    for item in pending:
        item["kind"] = "pending"
        item["masked"] = _mask(item.get("phone"))

    stored = []
    for u in get_active_sessions_for_export(limit=300):
        stored.append({
            "kind": "stored",
            "id": u.get("phone") or str(u.get("user_id")),
            "phone": u.get("phone"),
            "masked": _mask(u.get("phone")),
            "status": u.get("status") or "active",
            "created_at": u.get("created_at") or "",
            "last_check": u.get("last_check") or "",
            "username": u.get("username") or "",
            "user_id": u.get("user_id"),
            "connected": True,
        })

    enabled, hours = dbmod.get_auto_logout()
    return jsonify({
        "success": True,
        "pending": pending,
        "stored": stored,
        "pending_count": len(pending),
        "stored_count": len(stored),
        "auto_logout_enabled": enabled,
        "auto_logout_hours": hours,
        "cleaner_running": session_cleaner.is_running(),
    })


@admin_bp.route("/api/sessions/refresh")
def api_sessions_refresh():
    session_manager.cleanup_dead_sessions()
    if dbmod.get_auto_logout()[0]:
        session_manager.enforce_auto_logout()
    return api_sessions()


@admin_bp.route("/api/sessions/kill/<session_id>", methods=["POST"])
def api_session_kill(session_id):
    session_manager.remove_session(session_id)
    info(f"pending session {session_id} killed from admin")
    return jsonify({"success": True, "message": "session closed"})


@admin_bp.route("/api/sessions/prune", methods=["POST"])
def api_sessions_prune():
    dead = session_manager.prune_dead()
    info(f"prune: {dead} dead account(s) removed")
    return jsonify({"success": True, "removed": dead,
                    "message": f"{dead} dead account(s) removed"})


@admin_bp.route("/zip")
def zip_sessions():
    pending = session_manager.list_sessions()
    for item in pending:
        item["kind"] = "pending"
        item["masked"] = _mask(item.get("phone"))

    stored = get_active_sessions_for_export(limit=1000)
    skip_check = request.args.get("check") in ("0", "false", "no")

    alive = None
    if stored and not skip_check:
        alive = check_many(stored, "phone")
        dead_rows = [r for r in stored if r.get("phone") not in alive]
        for row in dead_rows:
            dbmod.record_check(row.get("phone"), False, "zip check")
            session_manager.kill_by_phone(row.get("phone"))
        stored = [r for r in stored if r.get("phone") in alive]
        if dead_rows:
            info(f"ZIP export skipped {len(dead_rows)} dead account(s)")

    if not pending and not stored:
        return jsonify({"success": False,
                        "error": "no working sessions to export"}), 404

    buf, filename = build_sessions_zip(pending, stored)
    info(f"ZIP export: {len(pending)} pending / {len(stored)} working")
    return send_file(buf, mimetype="application/zip",
                     as_attachment=True, download_name=filename)


@admin_bp.route("/api/stats/refresh")
def refresh_stats():
    s = get_stats()
    s["active_sessions"] = session_manager.get_active_count()
    s["broadcaster_running"] = ads_broadcaster.is_running()
    s["success"] = True
    return jsonify(s)


@admin_bp.route("/api/logs")
def api_logs():
    try:
        since = int(request.args.get("since", 0))
    except (TypeError, ValueError):
        since = 0
    level = request.args.get("level") or "all"
    source = request.args.get("source") or "all"
    return jsonify({
        "success": True,
        "logs": get_logs(since_id=since, limit=400, level=level, source=source),
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

    total = subs = no_session = due = dead = 0
    if users_col is not None:
        try:
            total = users_col.count_documents({})
            subs = users_col.count_documents({"ads_enabled": True})
            dead = users_col.count_documents({"status": "dead"})
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
    chk(dead == 0, "No dead accounts", f"{dead} dead" if dead else "")
    chk(not media["configured"] or media["exists"], "Media file present on disk",
        media["file"] or "none")
    chk(active_links > 0, "At least one active ad link", f"{active_links}")
    chk(bool((cfg.get("caption") or "").strip()) or bool(active_links) or media["configured"],
        "Something to send (caption / link / media)")

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
        "dead_users": dead,
        "users_without_session": no_session,
        "due_now": due,
        "active_links": active_links,
        "media": media,
        "active_sessions": session_manager.get_active_count(),
        "counters": get_counters(),
        "sources": sources(),
        "last_error": last_error(),
        "checks": checks,
    })


@admin_bp.route("/api/clear-messages", methods=["POST"])
def clear_messages():
    n = dbmod.clear_broadcast_messages()
    info(f"message index cleared ({n})")
    return jsonify({"success": True, "cleared_count": n,
                    "message": f"{n} tracked message(s) dropped"})