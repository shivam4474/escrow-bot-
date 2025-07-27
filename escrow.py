import re
import sqlite3
import logging
import sys
from datetime import datetime, timedelta
from types import ModuleType
import pytz
import csv
import io

# --- Configuration ---
TOKEN = "7628957531:AAF91TVglDnQJbF7lkyY9LoqUssDDEkcpKQ"
# !!! IMPORTANT: Replace with your numeric Telegram User ID !!!
ADMIN_ID = 549086084
DB_NAME = "escrow_bot.db"
IST = pytz.timezone('Asia/Kolkata')
USER_LIST_PAGE_SIZE = 5 # Number of users to show per page in the admin list

# Custom imghdr implementation for Python 3.13+
class DummyImghdr:
    @staticmethod
    def what(file, h=None): return None

sys.modules['imghdr'] = DummyImghdr()

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters, ContextTypes,
    ConversationHandler, CallbackQueryHandler
)
from telegram.error import Forbidden

# Set up logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Keyboard & Button Definitions ---

# User Buttons
BTN_INR_DASH = "🇮🇳 INR Dashboard"
BTN_CRYPTO_DASH = "💰 CRYPTO Dashboard"
BTN_TOTAL_FUNDS = "📊 My Funds"
BTN_TOTAL_FEES = "💸 My Fees"
BTN_PENDING = "⏳ My Pending Deals"

# Admin Buttons
BTN_ADMIN_GLOBAL_STATS = "🌐 Global Stats"
BTN_ADMIN_USER_LIST = "👥 User List"
BTN_ADMIN_TO_USER_VIEW = "👤 Switch to My User View"
BTN_ADMIN_EXPORT_DATA = "📊 Export Data"
BTN_ADMIN_BROADCAST = "📣 Broadcast"

# Shared Buttons
BTN_BACK_TO_USER_MENU = "◀️ Back"
BTN_BACK_TO_ADMIN_PANEL = "🧑‍💼 Back to Admin Panel"
BTN_CONFIRM_RESET = "✅ Yes, Delete"
BTN_CANCEL_RESET = "❌ No, Cancel"
BTN_CANCEL_MSG = "❌ Cancel Message"
BTN_CANCEL_BROADCAST = "❌ Cancel Broadcast"


# Fees Buttons
BTN_FEES_TODAY = "Today's Fees"
BTN_FEES_WEEKLY = "This Week's Fees"
BTN_FEES_MONTHLY = "This Month's Fees"
BTN_FEES_ALL_TIME = "All-Time Fees"

# Dynamic Button Prefixes
BTN_VIEW_DEALS_PREFIX = "👁️ View Deals for"
BTN_MSG_USER_PREFIX = "✉️ Message"
CALLBACK_USER_LIST_PREFIX = "user_list_page_"

# --- Keyboards ---
USER_KEYBOARD = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_INR_DASH), KeyboardButton(BTN_CRYPTO_DASH)],
    [KeyboardButton(BTN_TOTAL_FUNDS), KeyboardButton(BTN_TOTAL_FEES)],
    [KeyboardButton(BTN_PENDING)]
], resize_keyboard=True)

ADMIN_KEYBOARD = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_ADMIN_GLOBAL_STATS), KeyboardButton(BTN_ADMIN_USER_LIST)],
    [KeyboardButton(BTN_ADMIN_EXPORT_DATA), KeyboardButton(BTN_ADMIN_BROADCAST)],
    [KeyboardButton(BTN_ADMIN_TO_USER_VIEW)]
], resize_keyboard=True)

ADMIN_IMPERSONATION_KEYBOARD = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_INR_DASH), KeyboardButton(BTN_CRYPTO_DASH)],
    [KeyboardButton(BTN_TOTAL_FUNDS), KeyboardButton(BTN_TOTAL_FEES)],
    [KeyboardButton(BTN_PENDING)],
    [KeyboardButton(BTN_BACK_TO_ADMIN_PANEL)]
], resize_keyboard=True)

FEES_KEYBOARD = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_FEES_TODAY), KeyboardButton(BTN_FEES_WEEKLY)],
    [KeyboardButton(BTN_FEES_MONTHLY), KeyboardButton(BTN_FEES_ALL_TIME)],
    [KeyboardButton(BTN_BACK_TO_USER_MENU)]
], resize_keyboard=True)

# States for the broadcast ConversationHandler
BROADCAST_MESSAGE, BROADCAST_CONFIRM = range(2)

# --- Database Initialization ---
def setup_database():
    conn = sqlite3.connect(DB_NAME, check_same_thread=False)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (user_id INTEGER PRIMARY KEY,
                  first_name TEXT,
                  username TEXT,
                  last_seen TEXT)''')

    c.execute('''CREATE TABLE IF NOT EXISTS transactions
                 (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL,
                  currency TEXT, received_amount REAL, release_amount REAL, fee REAL,
                  trade_id TEXT, status TEXT DEFAULT 'holding',
                  received_date TEXT, released_date TEXT, escrowed_by TEXT,
                  buyer TEXT, seller TEXT, UNIQUE(user_id, trade_id),
                  FOREIGN KEY(user_id) REFERENCES users(user_id))''')
    conn.commit()
    logger.info("Database initialized with 'users' and 'transactions' tables.")
    return conn, c

conn, c = setup_database()

# --- Helper & Core Functions ---
async def register_user(update: Update):
    """Saves or updates user info in the database."""
    user = update.effective_user
    now_utc_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    c.execute("""
        INSERT INTO users (user_id, first_name, username, last_seen)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
        first_name=excluded.first_name,
        username=excluded.username,
        last_seen=excluded.last_seen
    """, (user.id, user.first_name, user.username, now_utc_str))
    conn.commit()

def get_user_id_for_query(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Determines which user's data to query."""
    if update.effective_user.id == ADMIN_ID and 'impersonated_user_id' in context.user_data:
        return context.user_data['impersonated_user_id']
    return update.effective_user.id

def format_datetime_ist(utc_dt_str: str) -> str:
    """Formats a UTC datetime string into a readable IST string."""
    if not utc_dt_str: return "N/A"
    try:
        utc_dt = pytz.utc.localize(datetime.strptime(utc_dt_str, "%Y-%m-%d %H:%M:%S"))
        return utc_dt.astimezone(IST).strftime('%d-%b-%Y, %I:%M %p')
    except (ValueError, TypeError):
        return utc_dt_str

# --- Command Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command."""
    await register_user(update)
    user_id = update.effective_user.id

    context.user_data.pop('impersonated_user_id', None)
    context.user_data.pop('messaging_user_id', None)

    if user_id == ADMIN_ID:
        await admin_menu(update, context)
    else:
        await user_menu(update, context)

async def reset_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Admin-Only) Resets data for a specific user or globally."""
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔️ This is an admin-only command.")
        return

    if context.args:
        try:
            user_id_to_reset = int(context.args[0])
            c.execute("SELECT first_name, username FROM users WHERE user_id=?", (user_id_to_reset,))
            user_info = c.fetchone()
            if not user_info:
                await update.message.reply_text(f"❌ User with ID `{user_id_to_reset}` not found.", parse_mode='Markdown')
                return

            c.execute("DELETE FROM transactions WHERE user_id=?", (user_id_to_reset,))
            conn.commit()

            user_display = f"{user_info[0]} (@{user_info[1]})" if user_info[1] else user_info[0]
            await update.message.reply_text(f"✅ **Success!** All transaction data for user `{user_id_to_reset}` ({user_display}) has been deleted.", parse_mode='Markdown')

        except (ValueError, IndexError):
            await update.message.reply_text("⚠️ **Invalid format.** Use `/reset <user_id>` or `/reset`.", parse_mode='Markdown')
        except Exception as e:
            logger.error(f"Error during targeted reset: {e}")
            await update.message.reply_text("❌ An error occurred.")
    else:
        confirmation_keyboard = ReplyKeyboardMarkup([[KeyboardButton(BTN_CONFIRM_RESET)], [KeyboardButton(BTN_CANCEL_RESET)]], resize_keyboard=True, one_time_keyboard=True)
        await update.message.reply_text("⚠️ **DANGER ZONE!** ⚠️\n\nAre you sure you want to delete ALL transaction data for ALL users? This cannot be undone.", reply_markup=confirmation_keyboard, parse_mode='Markdown')

async def perform_global_reset(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """(Admin-Only) Wipes the transactions and users tables."""
    global conn, c
    try:
        await update.message.reply_text("🔄 Resetting entire database...", reply_markup=ADMIN_KEYBOARD)
        c.execute("DROP TABLE IF EXISTS transactions")
        c.execute("DROP TABLE IF EXISTS users")
        conn.commit()
        conn, c = setup_database()
        await update.message.reply_text("✅ **Global Reset Complete!**")
    except Exception as e:
        logger.error(f"Failed to reset database: {e}", exc_info=True)
        await update.message.reply_text("❌ **Error:** Could not reset the database.")


# --- Menu Display Functions ---
async def user_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows the standard user menu."""
    await register_user(update)
    keyboard = USER_KEYBOARD
    
    # --- MODIFIED: More professional welcome message for users ---
    text = (
        "🙏🏻 **Welcome ** \n\n"
        
    )

    if update.effective_user.id == ADMIN_ID:
        # --- MODIFIED: More professional message for admin's personal view ---
        text = (
            "👤 **Personal User View** 👤\n\n"
            "You are now managing your personal deals.\n"
            "To return to the Admin Panel, use the `/start` command."
        )

    await update.message.reply_text(text, reply_markup=keyboard, parse_mode='Markdown')

async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Shows the main admin menu."""
    context.user_data.pop('impersonated_user_id', None)
    context.user_data.pop('messaging_user_id', None)
    
    # --- MODIFIED: More professional welcome message for the admin ---
    text = (
        "🧑‍💼 **GOOSE Panel** 🧑‍💼\n\n"
        "Welcome, Admin. You have full control over the bot's operations.\n\n"
        "Please select an option from the panel below."
    )
    
    await update.message.reply_text(text, reply_markup=ADMIN_KEYBOARD, parse_mode='Markdown')

async def show_impersonation_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, target_user_id: int):
    """Shows the user menu, but for an admin impersonating a user."""
    context.user_data['impersonated_user_id'] = target_user_id
    c.execute("SELECT first_name, username FROM users WHERE user_id=?", (target_user_id,))
    user_info = c.fetchone()
    user_display = f"{user_info[0]} (@{user_info[1]})" if user_info[1] else user_info[0]

    text = f"👁️ **Viewing as {user_display}** (`{target_user_id}`)\n\n"
    text += "You are now seeing this user's data. Use the keyboard below to navigate their dashboards."

    await update.message.reply_text(text, reply_markup=ADMIN_IMPERSONATION_KEYBOARD, parse_mode='Markdown')


# --- Message & Button Click Handlers ---
async def handle_button_clicks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles all button clicks (but not stateful messages)."""
    await register_user(update)
    text = update.message.text
    user_id = update.effective_user.id

    # --- Admin-Specific Actions ---
    if user_id == ADMIN_ID:
        if text == BTN_ADMIN_GLOBAL_STATS: await show_global_stats(update, context)
        elif text == BTN_ADMIN_USER_LIST: await show_user_list_paginated(update, context)
        elif text == BTN_ADMIN_TO_USER_VIEW: await user_menu(update, context)
        elif text == BTN_BACK_TO_ADMIN_PANEL: await start(update, context)
        elif text == BTN_ADMIN_EXPORT_DATA: await export_data(update, context)

        elif text.startswith(BTN_VIEW_DEALS_PREFIX):
            try:
                target_user_id = int(re.search(r'\((\d+)\)', text).group(1))
                await show_impersonation_menu(update, context, target_user_id)
            except (AttributeError, ValueError):
                await update.message.reply_text("Could not parse user ID from button.")
            return
        elif text.startswith(BTN_MSG_USER_PREFIX):
            await start_messaging_user(update, context)
            return
        elif text == BTN_CONFIRM_RESET: await perform_global_reset(update, context)

    # --- Shared Actions (User & Impersonating Admin) ---
    if text == BTN_INR_DASH: await show_inr_dashboard(update, context)
    elif text == BTN_CRYPTO_DASH: await show_crypto_dashboard(update, context)
    elif text == BTN_TOTAL_FUNDS: await show_total_funds(update, context)
    elif text == BTN_PENDING: await show_pending_releases(update, context)
    elif text == BTN_TOTAL_FEES: await show_fees_menu(update, context)
    elif text == BTN_FEES_TODAY: await show_fees_today(update, context)
    elif text == BTN_FEES_WEEKLY: await show_fees_weekly(update, context)
    elif text == BTN_FEES_MONTHLY: await show_fees_monthly(update, context)
    elif text == BTN_FEES_ALL_TIME: await show_fees_all_time(update, context)
    elif text.startswith("Release "): await release_funds(update, context)

    # --- Back Buttons ---
    elif text == BTN_BACK_TO_USER_MENU:
        if user_id == ADMIN_ID and 'impersonated_user_id' in context.user_data:
            target_user_id = context.user_data['impersonated_user_id']
            await show_impersonation_menu(update, context, target_user_id)
        else:
            await user_menu(update, context)

    elif text == BTN_CANCEL_RESET:
        await update.message.reply_text("👍 **Reset Cancelled.**", reply_markup=ADMIN_KEYBOARD, parse_mode='Markdown')

async def handle_forward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles forwarded messages for deal creation."""
    await register_user(update)
    msg = update.message.text
    if "Continue the Deal" in msg:
        currency = "inr" if '₹' in msg else "crypto"
        await process_new_deal(update, msg, currency)
    elif "Deal Completed" in msg:
        await process_completed_deal(update, msg)


# --- Dashboard & Data Display Functions ---
async def show_inr_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(update, context)
    c.execute("SELECT SUM(received_amount) FROM transactions WHERE currency='inr' AND status='holding' AND user_id=?", (query_user_id,))
    holding = (c.fetchone()[0] or 0)
    c.execute("SELECT trade_id, received_amount FROM transactions WHERE currency='inr' AND status='holding' AND user_id=?", (query_user_id,))
    pending = c.fetchall()
    buttons = [[KeyboardButton(f"Release {row[0]} (₹{row[1]:.2f})")] for row in pending]
    buttons.append([KeyboardButton(BTN_BACK_TO_USER_MENU)])
    keyboard = ReplyKeyboardMarkup(buttons, resize_keyboard=True)
    text = f"🇮🇳 **INR DASHBOARD**\n\n💵 **Holding:** ₹{holding:.2f}\n\n⬇️ **Pending Releases:**"
    if not pending: text += "\nNo pending INR releases."
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode='Markdown')

async def show_crypto_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(update, context)
    c.execute("SELECT SUM(received_amount) FROM transactions WHERE currency='crypto' AND status='holding' AND user_id=?", (query_user_id,))
    holding = (c.fetchone()[0] or 0)
    c.execute("SELECT SUM(fee) FROM transactions WHERE currency='crypto' AND status='completed' AND user_id=?", (query_user_id,))
    fees = (c.fetchone()[0] or 0)
    c.execute("SELECT trade_id, received_amount FROM transactions WHERE currency='crypto' AND status='holding' AND user_id=?", (query_user_id,))
    pending = c.fetchall()
    buttons = [[KeyboardButton(f"Release {row[0]} (${row[1]:.2f})")] for row in pending]
    buttons.append([KeyboardButton(BTN_BACK_TO_USER_MENU)])
    keyboard = ReplyKeyboardMarkup(buttons, resize_keyboard=True)
    text = f"💰 **CRYPTO DASHBOARD**\n\n💵 **Holding:** ${holding:.2f}\n⚡️ **Fees Earned:** ${fees:.2f}\n\n⬇️ **Pending Releases:**"
    if not pending: text += "\nNo pending crypto releases."
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode='Markdown')

async def show_total_funds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(update, context)
    c.execute("SELECT currency, SUM(received_amount) FROM transactions WHERE status='holding' AND user_id=? GROUP BY currency", (query_user_id,))
    holdings = c.fetchall()
    text = "📊 **TOTAL FUNDS HOLDING**\n\n"
    if not holdings or all(h[1] is None for h in holdings):
        text += "No funds are currently held in escrow."
    else:
        for currency, amount in holdings:
            if amount is not None and amount > 0:
                symbol = '₹' if currency == 'inr' else '$'
                text += f"▪️ {currency.upper()}: {symbol}{amount:.2f}\n"
    is_impersonating = update.effective_user.id == ADMIN_ID and 'impersonated_user_id' in context.user_data
    reply_markup = ADMIN_IMPERSONATION_KEYBOARD if is_impersonating else USER_KEYBOARD
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')

# --- MODIFIED: show_pending_releases to include release amount ---
async def show_pending_releases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(update, context)
    c.execute(
        "SELECT trade_id, currency, received_amount, release_amount, fee, received_date, escrowed_by "
        "FROM transactions WHERE status='holding' AND user_id=? ORDER BY received_date ASC",
        (query_user_id,)
    )
    pending = c.fetchall()

    if not pending:
        text = "✅ **NO PENDING RELEASES**\n\nAll deals have been completed!"
    else:
        text = f"⏳ **PENDING RELEASES ({len(pending)})**\n"
        for trade_id, currency, received_amount, release_amount, fee, received_date_str, escrowed_by in pending:
            symbol = '₹' if currency == 'inr' else '$'
            escrow_agent = escrowed_by.strip() if escrowed_by else "N/A"
            formatted_date = format_datetime_ist(received_date_str)

            text += (
                f"\n\n"
                f"🟩 **ESCROW DEAL** 🟩\n"
                f"**ID**: `{trade_id}`\n"
                f"**Received Amount**: {symbol}{received_amount:.2f}\n"
                f"**Fee**: {symbol}{fee:.2f}\n"
                f"**Release Amount**: **{symbol}{release_amount:.2f}**\n"
                f"**date&time **: {formatted_date}\n"
                f"**Escrowed By**: {escrow_agent}"
            )

    is_impersonating = update.effective_user.id == ADMIN_ID and 'impersonated_user_id' in context.user_data
    reply_markup = ADMIN_IMPERSONATION_KEYBOARD if is_impersonating else USER_KEYBOARD
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode='Markdown')


# --- Fee Calculation Functions ---
async def show_fees_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("💸 **Fee Report**\n\nSelect a time period.", reply_markup=FEES_KEYBOARD, parse_mode='Markdown')

async def calculate_and_send_fees(update: Update, context: ContextTypes.DEFAULT_TYPE, start_utc: datetime, end_utc: datetime, title: str):
    query_user_id = get_user_id_for_query(update, context)
    start_str, end_str = start_utc.strftime("%Y-%m-%d %H:%M:%S"), end_utc.strftime("%Y-%m-%d %H:%M:%S")
    c.execute("SELECT currency, SUM(fee) FROM transactions WHERE status='completed' AND user_id=? AND released_date BETWEEN ? AND ? GROUP BY currency", (query_user_id, start_str, end_str))
    fees = c.fetchall()
    text = f"💸 **{title}**\n\n"
    total_earned = 0
    if not fees or all(f[1] is None for f in fees):
        text += "No fees earned in this period."
    else:
        for currency, amount in fees:
            if amount is not None and amount > 0:
                symbol = '₹' if currency == 'inr' else '$'
                text += f"▪️ {currency.upper()}: {symbol}{amount:.2f}\n"
                total_earned += 1
    if not total_earned: text += "No fees earned in this period."
    await update.message.reply_text(text, reply_markup=FEES_KEYBOARD, parse_mode='Markdown')

async def show_fees_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now_ist = datetime.now(IST)
    start_utc = now_ist.replace(hour=0, minute=0, second=0, microsecond=0).astimezone(pytz.utc)
    await calculate_and_send_fees(update, context, start_utc, datetime.now(pytz.utc), "FEES EARNED TODAY")

async def show_fees_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now_ist = datetime.now(IST)
    start_of_week_ist = (now_ist - timedelta(days=now_ist.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    await calculate_and_send_fees(update, context, start_of_week_ist.astimezone(pytz.utc), datetime.now(pytz.utc), "FEES EARNED THIS WEEK")

async def show_fees_monthly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    now_ist = datetime.now(IST)
    start_of_month_ist = now_ist.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    await calculate_and_send_fees(update, context, start_of_month_ist.astimezone(pytz.utc), datetime.now(pytz.utc), "FEES EARNED THIS MONTH")

async def show_fees_all_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(update, context)
    c.execute("SELECT currency, SUM(fee) FROM transactions WHERE status='completed' AND user_id=? GROUP BY currency", (query_user_id,))
    fees = c.fetchall()
    text = "💸 **ALL-TIME FEES EARNED**\n\n"
    total_earned = 0
    if not fees or all(f[1] is None for f in fees):
        text += "No fees have been earned yet."
    else:
        for currency, amount in fees:
            if amount is not None and amount > 0:
                symbol = '₹' if currency == 'inr' else '$'
                text += f"▪️ {currency.upper()}: {symbol}{amount:.2f}\n"
                total_earned += 1
    if not total_earned: text += "No fees have been earned yet."
    await update.message.reply_text(text, reply_markup=FEES_KEYBOARD, parse_mode='Markdown')

# --- ADMIN-ONLY Global & User Management Functions ---
async def show_global_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    c.execute("SELECT currency, SUM(received_amount) FROM transactions WHERE status='holding' GROUP BY currency")
    holdings = c.fetchall()
    text = "🌐 **GLOBAL BOT STATISTICS**\n\n📊 **Total Funds Holding (All Users):**\n"
    if not holdings or all(h[1] is None for h in holdings):
        text += "No funds are currently held in escrow.\n"
    else:
        for currency, amount in holdings:
            if amount is not None and amount > 0: text += f"▪️ {currency.upper()}: {'₹' if currency == 'inr' else '$'}{amount:.2f}\n"

    c.execute("SELECT currency, SUM(fee) FROM transactions WHERE status='completed' GROUP BY currency")
    fees = c.fetchall()
    text += "\n💸 **Total Fees Earned (All Users):**\n"
    if not fees or all(f[1] is None for f in fees):
        text += "No fees have been earned."
    else:
        for currency, amount in fees:
            if amount is not None and amount > 0: text += f"▪️ {currency.upper()}: {'₹' if currency == 'inr' else '$'}{amount:.2f}\n"
    await update.message.reply_text(text, reply_markup=ADMIN_KEYBOARD, parse_mode='Markdown')

async def show_user_list_paginated(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int = 1):
    c.execute("SELECT COUNT(*) FROM users")
    total_users = c.fetchone()[0]

    if total_users == 0:
        await update.message.reply_text("No users have interacted with the bot yet.", reply_markup=ADMIN_KEYBOARD)
        return

    offset = (page - 1) * USER_LIST_PAGE_SIZE
    c.execute("""
        SELECT u.user_id, u.first_name, u.username,
               (SELECT COUNT(t.id) FROM transactions t WHERE t.user_id = u.user_id),
               (SELECT COUNT(t.id) FROM transactions t WHERE t.user_id = u.user_id AND t.status = 'holding')
        FROM users u
        ORDER BY (SELECT COUNT(t.id) FROM transactions t WHERE t.user_id = u.user_id) DESC
        LIMIT ? OFFSET ?
    """, (USER_LIST_PAGE_SIZE, offset))
    users = c.fetchall()

    text = f"👥 **USER LIST ({total_users} Total) - Page {page}**\n\n"
    reply_keyboard_buttons = []
    for user_id, first_name, username, total_deals, pending_deals in users:
        user_display = f"{first_name} (@{username})" if username else first_name
        text += f"▪️ **{user_display}**\n"
        text += f"   `ID: {user_id}` | `Deals: {total_deals} | Pending: {pending_deals}`\n\n"

        view_btn = KeyboardButton(f"{BTN_VIEW_DEALS_PREFIX} {first_name} ({user_id})")
        msg_btn = KeyboardButton(f"{BTN_MSG_USER_PREFIX} {first_name} ({user_id})")
        reply_keyboard_buttons.append([view_btn, msg_btn])

    reply_keyboard_buttons.append([KeyboardButton(BTN_BACK_TO_ADMIN_PANEL)])
    final_reply_markup = ReplyKeyboardMarkup(reply_keyboard_buttons, resize_keyboard=True)

    inline_keyboard_buttons = []
    if page > 1:
        inline_keyboard_buttons.append(InlineKeyboardButton("◀️ Previous", callback_data=f"{CALLBACK_USER_LIST_PREFIX}{page-1}"))
    if offset + len(users) < total_users:
        inline_keyboard_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"{CALLBACK_USER_LIST_PREFIX}{page+1}"))

    pagination_markup = InlineKeyboardMarkup([inline_keyboard_buttons]) if inline_keyboard_buttons else None

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, parse_mode='Markdown', reply_markup=pagination_markup
        )
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Use the buttons below to interact with users or navigate.",
            reply_markup=final_reply_markup
        )
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=final_reply_markup)
        if pagination_markup:
            await update.message.reply_text("Navigate pages:", reply_markup=pagination_markup)

async def user_list_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split('_')[-1])
    await show_user_list_paginated(update, context, page=page)

async def start_messaging_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        text = update.message.text
        target_user_id = int(re.search(r'\((\d+)\)', text).group(1))
        c.execute("SELECT first_name FROM users WHERE user_id=?", (target_user_id,))
        user_info = c.fetchone()
        user_name = user_info[0] if user_info else f"User ID {target_user_id}"
        context.user_data['messaging_user_id'] = target_user_id
        cancel_keyboard = ReplyKeyboardMarkup([[KeyboardButton(BTN_CANCEL_MSG)]], resize_keyboard=True, one_time_keyboard=True)
        await update.message.reply_text(
            f"✍️ **Composing Message**\n\nSend the message for **{user_name}**.",
            parse_mode='Markdown', reply_markup=cancel_keyboard
        )
    except (AttributeError, ValueError, TypeError) as e:
        logger.error(f"Error in start_messaging_user: {e}")
        await update.message.reply_text("Could not parse user ID.", reply_markup=ADMIN_KEYBOARD)

async def forward_admin_message_to_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    target_user_id = context.user_data.pop('messaging_user_id')
    try:
        await context.bot.send_message(chat_id=target_user_id, text="🔔 A message from the admin:")
        await context.bot.copy_message(
            chat_id=target_user_id,
            from_chat_id=update.effective_chat.id,
            message_id=update.message.message_id
        )
        await update.message.reply_text("✅ Message sent successfully!", reply_markup=ADMIN_KEYBOARD)
    except Exception as e:
        logger.error(f"Failed to send message to user {target_user_id}: {e}")
        await update.message.reply_text(f"❌ **Failed to send.** Error: {e}", parse_mode='Markdown', reply_markup=ADMIN_KEYBOARD)

async def cancel_messaging(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.pop('messaging_user_id', None)
    await update.message.reply_text("👍 Message cancelled.", reply_markup=ADMIN_KEYBOARD)
    await show_user_list_paginated(update, context)

async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔄 Generating CSV export...")
    try:
        c.execute("SELECT * FROM transactions ORDER BY received_date DESC")
        transactions = c.fetchall()

        if not transactions:
            await update.message.reply_text("No transaction data to export.", reply_markup=ADMIN_KEYBOARD)
            return

        output = io.StringIO()
        writer = csv.writer(output)
        header = [desc[0] for desc in c.description]
        writer.writerow(header)
        for row in transactions:
            writer.writerow(row)
        output.seek(0)

        filename = f"escrow_export_{datetime.now(IST).strftime('%Y-%m-%d_%H-%M')}.csv"
        await context.bot.send_document(
            chat_id=update.effective_chat.id,
            document=InputFile(output, filename=filename),
            caption="✅ Here is the transaction data export."
        )
    except Exception as e:
        logger.error(f"Failed to export data: {e}", exc_info=True)
        await update.message.reply_text("❌ An error occurred during the data export.")

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID: return
    await update.message.reply_text(
        "📣 **Broadcast Mode**\n\nPlease send the message you want to broadcast to all users.\nTo exit, use /cancel.",
        reply_markup=ReplyKeyboardMarkup([[BTN_CANCEL_BROADCAST]], resize_keyboard=True, one_time_keyboard=True)
    )
    return BROADCAST_MESSAGE

async def broadcast_receive_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['broadcast_message_id'] = update.message.message_id
    context.user_data['broadcast_chat_id'] = update.message.chat_id
    c.execute("SELECT COUNT(*) FROM users")
    user_count = c.fetchone()[0]
    keyboard = [[
        InlineKeyboardButton("✅ Yes, Send It!", callback_data='broadcast_confirm_send'),
        InlineKeyboardButton("❌ No, Cancel", callback_data='broadcast_confirm_cancel')
    ]]
    await update.message.reply_text(
        f"⚠️ **CONFIRM BROADCAST**\n\nYour message is ready to be sent to **{user_count}** users. Are you sure?",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    return BROADCAST_CONFIRM

async def broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("🚀 **Broadcasting...** Please wait.", reply_markup=None)

    c.execute("SELECT user_id FROM users")
    users = c.fetchall()
    message_id = context.user_data['broadcast_message_id']
    chat_id = context.user_data['broadcast_chat_id']
    success_count, fail_count = 0, 0

    for user in users:
        try:
            await context.bot.copy_message(chat_id=user[0], from_chat_id=chat_id, message_id=message_id)
            success_count += 1
        except Forbidden:
            logger.warning(f"Broadcast failed for user {user[0]}: Bot blocked.")
            fail_count += 1
        except Exception as e:
            logger.error(f"Broadcast failed for user {user[0]}: {e}")
            fail_count += 1

    await query.message.reply_text(
        f"✅ **Broadcast Complete**\n\nSent: {success_count} | Failed: {fail_count}",
        reply_markup=ADMIN_KEYBOARD
    )
    context.user_data.clear()
    return ConversationHandler.END

async def broadcast_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("👍 Broadcast cancelled.", reply_markup=None)
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Returning to Admin Panel.", reply_markup=ADMIN_KEYBOARD)
    else:
        await update.message.reply_text("👍 Broadcast cancelled.", reply_markup=ADMIN_KEYBOARD)
    return ConversationHandler.END


# --- Data Processing Functions ---
async def process_new_deal(update, msg, currency):
    user_id = update.effective_user.id
    try:
        trade_id_regex = r"🆔?\s*Trade ID: (#\w+)"
        patterns = {
            "inr": { "received": r"Received Amount : ₹([\d,]+\.?\d*)", "fee": r"Escrow Fee : ₹([\d,]+\.?\d*)", "trade_id": trade_id_regex, "escrowed_by": r"Escrowed By : (.*?)(\n|$)" },
            "crypto": { "received": r"Received Amount : ([\d,]+\.?\d*)\$", "trade_id": trade_id_regex, "escrowed_by": r"Escrowed By : (.*?)(\n|$)" }
        }
        data = {key: (match.group(1).replace(',', '').strip() if (match := re.search(pattern, msg)) else "0") for key, pattern in patterns[currency].items()}
        if data["trade_id"] == "0":
            await update.message.reply_text("❌ **Error:** Could not find a valid Trade ID.", parse_mode='Markdown')
            return
        received_amount = float(data.get("received", "0"))
        fee = received_amount * 0.007 if currency == "crypto" else float(data.get("fee", "0"))
        release_amount = received_amount - fee
        now_utc_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("INSERT OR IGNORE INTO transactions (user_id, currency, received_amount, release_amount, fee, trade_id, received_date, escrowed_by, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'holding')",
                  (user_id, currency, received_amount, release_amount, fee, data["trade_id"], now_utc_str, data["escrowed_by"]))
        conn.commit()

        if c.rowcount > 0:
            symbol = '₹' if currency == 'inr' else '$'
            reply_text = (f"✅ **New Escrow Added!**\n\n"
                          f"🆔 **Trade ID:** `{data['trade_id']}`\n"
                          f"📥 **Received:** {symbol}{received_amount:.2f}\n"
                          f"💸 **Fee Cut:** {symbol}{fee:.2f}\n"
                          f"📤 **To Release:** {symbol}{release_amount:.2f}")
            await update.message.reply_text(reply_text, parse_mode='Markdown')
        else:
            await update.message.reply_text(f"⚠️ **Duplicate:** You already have a deal with ID `{data['trade_id']}`.", parse_mode='Markdown')

    except Exception as e:
        logger.error(f"Error processing new deal: {e}", exc_info=True)
        await update.message.reply_text("❌ **Error:** Could not process message.", parse_mode='Markdown')

async def process_completed_deal(update, msg):
    user_id = update.effective_user.id
    try:
        trade_id_match = re.search(r"🆔?\s*Trade ID: (#\w+)", msg)
        if not trade_id_match:
            await update.message.reply_text("❌ **Error:** Could not find Trade ID.", parse_mode='Markdown')
            return
        trade_id = trade_id_match.group(1)
        now_utc_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE transactions SET status='completed', released_date=? WHERE trade_id=? AND user_id=? AND status='holding'", (now_utc_str, trade_id, user_id))
        conn.commit()
        if c.rowcount > 0:
            await update.message.reply_text(f"✅ **Deal Completed:** `{trade_id}`.", parse_mode='Markdown')
        else:
            await update.message.reply_text(f"⚠️ **Not Found:** No pending transaction for `{trade_id}`.", parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Error completing deal: {e}", exc_info=True)
        await update.message.reply_text("❌ **Error:** Could not process completion.", parse_mode='Markdown')

async def release_funds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(update, context)
    trade_id_match = re.search(r"Release (#\w+)", update.message.text)
    if not trade_id_match: return
    trade_id = trade_id_match.group(1)
    try:
        c.execute("SELECT currency FROM transactions WHERE trade_id=? AND user_id=? AND status='holding'", (trade_id, query_user_id))
        result = c.fetchone()
        if not result:
            await update.message.reply_text(f"⚠️ Transaction `{trade_id}` not found or already completed.", parse_mode='Markdown')
            return
        currency = result[0]
        now_utc_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        c.execute("UPDATE transactions SET status='completed', released_date=? WHERE trade_id=? AND user_id=?", (now_utc_str, trade_id, query_user_id))
        conn.commit()
        await update.message.reply_text(f"✅ **Funds Released!**\nTrade ID `{trade_id}` is now complete.", parse_mode='Markdown')
        if currency == 'inr': await show_inr_dashboard(update, context)
        else: await show_crypto_dashboard(update, context)
    except Exception as e:
        logger.error(f"Error releasing funds: {e}", exc_info=True)
        await update.message.reply_text("❌ An error occurred.")

# --- Main Router & Bot Execution ---
async def message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message_text = update.message.text if update.message and update.message.text else ""
    if user_id == ADMIN_ID and 'messaging_user_id' in context.user_data:
        if message_text == BTN_CANCEL_MSG:
            await cancel_messaging(update, context)
        else:
            await forward_admin_message_to_user(update, context)
        return
    if message_text:
        await handle_button_clicks(update, context)
        return

def main():
    application = Application.builder().token(TOKEN).build()

    broadcast_handler = ConversationHandler(
        entry_points=[
            CommandHandler("broadcast", broadcast_command, filters=filters.User(ADMIN_ID)),
            MessageHandler(filters.TEXT & filters.User(ADMIN_ID) & filters.Regex(f'^{BTN_ADMIN_BROADCAST}$'), broadcast_command),
        ],
        states={
            BROADCAST_MESSAGE: [MessageHandler(filters.ALL & ~filters.COMMAND, broadcast_receive_message)],
            BROADCAST_CONFIRM: [
                CallbackQueryHandler(broadcast_send, pattern='^broadcast_confirm_send$'),
                CallbackQueryHandler(broadcast_cancel, pattern='^broadcast_confirm_cancel$'),
            ],
        },
        fallbacks=[
            CommandHandler("cancel", broadcast_cancel),
            MessageHandler(filters.TEXT & filters.Regex(f'^{BTN_CANCEL_BROADCAST}$'), broadcast_cancel)
        ],
    )
    application.add_handler(broadcast_handler)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("reset", reset_command, filters=filters.User(ADMIN_ID)))
    application.add_handler(MessageHandler(filters.FORWARDED & filters.TEXT, handle_forward))
    application.add_handler(CallbackQueryHandler(user_list_callback, pattern=f'^{CALLBACK_USER_LIST_PREFIX}'))
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND & ~filters.FORWARDED, message_router))

    logger.info("✅ Bot is running...")
    application.run_polling()

if __name__ == "__main__":
    if ADMIN_ID == 123456789:
        logger.warning("! IMPORTANT: Please set your numeric ADMIN_ID in the script. !")
    main()