import re
import psycopg2
import os
import logging
import sys
from datetime import datetime, timedelta
import pytz
import csv
import io
import asyncio
import time
from contextlib import contextmanager
from psycopg2 import pool
from functools import partial

# --- Configuration ---
# It's recommended to load these from environment variables for better security
TOKEN = os.getenv("TELEGRAM_TOKEN", "7628957531:AAF91TVglDnQJbF7lkyY9LoqUssDDEkcpKQ")
BOT_OWNER_ID = int(os.getenv("BOT_OWNER_ID", 549086084))
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_0jZ3cNayoxPH@ep-holy-fire-aeaxqqri-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

# --- Constants & Settings ---
IST = pytz.timezone('Asia/Kolkata')
PERSISTENCE_FILE = "bot_persistence.pickle"

# --- Dummy imghdr to prevent import errors on some systems ---
class DummyImghdr:
    @staticmethod
    def what(file, h=None): return None
sys.modules['imghdr'] = DummyImghdr()

# --- Telegram Imports ---
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InputFile, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters, ContextTypes,
    PicklePersistence, ConversationHandler, CallbackQueryHandler
)
from telegram.error import BadRequest
from telegram.constants import ParseMode

# --- Logging Setup ---
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# --- UI Button Constants ---
BTN_INR_DASH, BTN_CRYPTO_DASH, BTN_TOTAL_FUNDS, BTN_TOTAL_FEES, BTN_PENDING = "🇮🇳 INR Dashboard", "💰 CRYPTO Dashboard", "📊 My Holding", "💸 My Fees", "⏳ My Pending Deals"
BTN_ESCROW_VOLUME = "📈 My Escrow Volume"
BTN_ADMIN_GLOBAL_STATS, BTN_ADMIN_ALL_PENDING, BTN_ADMIN_EXPORT_DATA, BTN_ADMIN_BROADCAST = "🌐 Global Stats", "⏳ All Pending Deals", "📊 Export Data", "📣 Broadcast"
BTN_BACK_TO_USER_MENU, BTN_BACK_TO_ADMIN_PANEL = "◀️ Back to Menu", "◀️ Back to Admin Panel"
BTN_FEES_TODAY, BTN_FEES_WEEKLY, BTN_FEES_MONTHLY, BTN_FEES_ALL_TIME = "Today's Fees", "This Week's Fees", "This Month's Fees", "All-Time Fees"
BTN_VOLUME_TODAY, BTN_VOLUME_WEEKLY, BTN_VOLUME_MONTHLY, BTN_VOLUME_ALL_TIME = "Today's Volume", "This Week's Volume", "This Month's Volume", "All-Time Volume"
WATCH_USER_PREFIX = "👤 Watch "
CALLBACK_FEE_SELECT_PREFIX = "fee_select|||"

# --- Conversation Handler States ---
BROADCAST_MESSAGE, BROADCAST_CONFIRM, RESET_CONFIRM, RESET_ALL_CONFIRM = range(4)

# --- Keyboards ---
USER_KEYBOARD = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_INR_DASH), KeyboardButton(BTN_CRYPTO_DASH)],
    [KeyboardButton(BTN_TOTAL_FUNDS), KeyboardButton(BTN_PENDING)],
    [KeyboardButton(BTN_ESCROW_VOLUME), KeyboardButton(BTN_TOTAL_FEES)]
], resize_keyboard=True)

ADMIN_WATCH_KEYBOARD = ReplyKeyboardMarkup([
    [KeyboardButton(BTN_INR_DASH), KeyboardButton(BTN_CRYPTO_DASH)],
    [KeyboardButton(BTN_TOTAL_FUNDS), KeyboardButton(BTN_PENDING)],
    [KeyboardButton(BTN_ESCROW_VOLUME), KeyboardButton(BTN_TOTAL_FEES)],
    [KeyboardButton(BTN_BACK_TO_ADMIN_PANEL)]
], resize_keyboard=True)

# --- Database Connection Pool ---
db_pool = None

def initialize_db_pool():
    """Initializes the database connection pool and creates tables if they don't exist."""
    global db_pool
    try:
        db_pool = pool.SimpleConnectionPool(1, 20, dsn=DATABASE_URL)
        logger.info("Database connection pool created successfully.")
        with db_pool.getconn() as conn:
            with conn.cursor() as c:
                c.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        user_id BIGINT PRIMARY KEY,
                        first_name TEXT,
                        username TEXT,
                        last_seen TIMESTPTZ
                    )
                ''')
                c.execute('''
                    CREATE TABLE IF NOT EXISTS transactions (
                        id BIGSERIAL PRIMARY KEY,
                        user_id BIGINT NOT NULL,
                        currency TEXT,
                        received_amount REAL,
                        release_amount REAL,
                        fee REAL,
                        trade_id TEXT,
                        status TEXT DEFAULT 'holding',
                        received_date TIMESTPTZ,
                        released_date TIMESTPTZ,
                        escrowed_by TEXT,
                        UNIQUE(user_id, trade_id),
                        FOREIGN KEY(user_id) REFERENCES users(user_id) ON DELETE CASCADE
                    )
                ''')
                c.execute('CREATE INDEX IF NOT EXISTS idx_transactions_user_id_status ON transactions (user_id, status)')
                c.execute('CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users (last_seen DESC NULLS LAST)')
                c.execute('CREATE INDEX IF NOT EXISTS idx_transactions_received_date ON transactions (received_date DESC)')
            conn.commit()
            db_pool.putconn(conn)
        logger.info("Database tables and performance indexes checked/created.")
    except Exception as e:
        logger.critical(f"FATAL error during DB initialization: {e}", exc_info=True)
        sys.exit("Database initialization failed.")

# --- HIGH-PERFORMANCE DATABASE HELPER ---
async def db_query(sql: str, params: tuple = None, fetch: str = "all", autocommit: bool = True):
    if db_pool is None:
        raise Exception("Database pool is not initialized.")
    loop = asyncio.get_running_loop()
    func = partial(_sync_db_query, sql, params, fetch, autocommit)
    return await loop.run_in_executor(None, func)

def _sync_db_query(sql: str, params: tuple, fetch: str, autocommit: bool):
    max_retries = 3
    last_exception = None
    for attempt in range(max_retries):
        conn = None
        try:
            conn = db_pool.getconn()
            conn.autocommit = autocommit
            with conn.cursor() as cur:
                cur.execute(sql, params)
                if fetch == "one":
                    result = cur.fetchone()
                elif fetch == "all":
                    result = cur.fetchall()
                elif fetch == "rowcount":
                    result = cur.rowcount
                else:
                    result = None
            db_pool.putconn(conn)
            return result
        except psycopg2.OperationalError as e:
            last_exception = e
            logger.warning(f"Database OperationalError on attempt {attempt + 1}: {e}. Retrying...")
            if conn:
                db_pool.putconn(conn, close=True)
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
            continue
        except Exception as e:
            logger.error(f"Database query failed with a non-retriable error: {e}\nQuery: {sql}", exc_info=True)
            if conn:
                db_pool.putconn(conn, close=True)
            raise
    logger.critical(f"Database query failed after {max_retries} retries. Giving up. Last error: {last_exception}")
    raise last_exception

# --- Helper Functions ---
async def register_user(update: Update):
    if not update.effective_user: return
    user = update.effective_user
    now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
    sql = """
        INSERT INTO users (user_id, first_name, username, last_seen)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET
            first_name=EXCLUDED.first_name,
            username=EXCLUDED.username,
            last_seen=EXCLUDED.last_seen;
    """
    await db_query(sql, (user.id, user.first_name, user.username, now_utc), fetch="none")

def get_user_id_for_query(context: ContextTypes.DEFAULT_TYPE) -> int:
    return context.user_data.get('managed_user_id') or context.user_data.get('original_user_id')

def format_datetime_ist(utc_dt: datetime) -> str:
    if not utc_dt: return "N/A"
    try:
        if utc_dt.tzinfo is None:
            utc_dt = pytz.utc.localize(utc_dt)
        return utc_dt.astimezone(IST).strftime('%d-%b-%Y, %I:%M %p')
    except (ValueError, TypeError): return str(utc_dt)

def escape_md_v1(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return text.replace('_', r'\_').replace('*', r'\*').replace('`', r'\`')

def get_time_range(period: str) -> tuple[datetime, datetime]:
    now_ist = datetime.now(IST)
    end_utc = datetime.now(pytz.utc)
    if period == "today":
        start_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "weekly":
        start_ist = (now_ist - timedelta(days=now_ist.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "monthly":
        start_ist = now_ist.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return datetime.min.replace(tzinfo=pytz.utc), end_utc
    return start_ist.astimezone(pytz.utc), end_utc

# --- Command Handlers ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    await register_user(update)
    context.user_data['original_user_id'] = update.effective_user.id
    await user_menu(update, context)

async def admin_panel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != BOT_OWNER_ID:
        await update.message.reply_text("You are not authorized to use this command.")
        return
    await admin_menu(update, context)

# --- Menu Display Functions ---
async def user_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    text = f"🙏🏻 **Welcome, {user.first_name}!**"
    await update.message.reply_text(text, reply_markup=USER_KEYBOARD, parse_mode=ParseMode.MARKDOWN)

async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    context.user_data['original_user_id'] = update.effective_user.id
    users = await db_query(
        "SELECT first_name, user_id FROM users WHERE user_id != %s ORDER BY first_name ASC",
        (BOT_OWNER_ID,)
    )
    admin_buttons = [
        [KeyboardButton(BTN_ADMIN_GLOBAL_STATS), KeyboardButton(BTN_ADMIN_ALL_PENDING)],
        [KeyboardButton(BTN_ADMIN_EXPORT_DATA), KeyboardButton(BTN_ADMIN_BROADCAST)],
    ]
    if users:
        watch_buttons = [KeyboardButton(f"{WATCH_USER_PREFIX}{user[0]} ({user[1]})") for user in users]
        user_rows = [watch_buttons[i:i + 2] for i in range(0, len(watch_buttons), 2)]
        admin_buttons = user_rows + admin_buttons
        
    keyboard = ReplyKeyboardMarkup(admin_buttons, resize_keyboard=True)
    await update.message.reply_text("🧑‍💼 **Admin Panel**", reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)

# --- Deal Processing ---
async def handle_new_deal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await register_user(update)
    user_id = update.effective_user.id
    msg = update.message.text
    try:
        trade_id_match = re.search(r"🆔?\s*Trade ID: (#\w+)", msg)
        escrowed_by_match = re.search(r"Escrowed By : (.*?)(\n|$)", msg)
        if not trade_id_match or not escrowed_by_match:
             await update.message.reply_text("❌ **Error:** Could not find `Trade ID` and `Escrowed By`.", parse_mode=ParseMode.MARKDOWN)
             return
        trade_id = trade_id_match.group(1)
        escrowed_by = escrowed_by_match.group(1).strip()
        
        existing_deal = await db_query(
            "SELECT id FROM transactions WHERE user_id=%s AND trade_id=%s",
            (user_id, trade_id), fetch="one"
        )
        if existing_deal:
            await update.message.reply_text(f"⚠️ **Duplicate:** You already have a deal with ID `{trade_id}`.", parse_mode=ParseMode.MARKDOWN)
            return

        currency = "inr" if '₹' in msg else "crypto"
        if currency == "inr":
            received_match = re.search(r"Received Amount : ₹([\d,]+\.?\d*)", msg)
            fee_match = re.search(r"Escrow Fee : ₹([\d,]+\.?\d*)", msg)
            if not received_match or not fee_match:
                await update.message.reply_text("❌ **Error:** Could not find INR `Received Amount` and `Escrow Fee`.", parse_mode=ParseMode.MARKDOWN)
                return
            received_amount = float(received_match.group(1).replace(',', ''))
            fee = float(fee_match.group(1).replace(',', ''))
            await insert_and_confirm_deal(context, update.effective_chat.id, user_id=user_id, currency=currency, received_amount=received_amount, fee=fee, trade_id=trade_id, escrowed_by=escrowed_by)
        else:
            received_match = re.search(r"Received Amount : ([\d,]+\.?\d*)\$", msg)
            if not received_match:
                 await update.message.reply_text("❌ **Error:** Could not find Crypto `Received Amount`.", parse_mode=ParseMode.MARKDOWN)
                 return
            received_amount = float(received_match.group(1).replace(',', ''))
            
            context.user_data.setdefault('pending_crypto_deals', {})[trade_id] = {
                'received_amount': received_amount, 'escrowed_by': escrowed_by
            }
            keyboard = [[
                InlineKeyboardButton("1% Fee", callback_data=f"{CALLBACK_FEE_SELECT_PREFIX}1.0|||{trade_id}"),
                InlineKeyboardButton("0.7% Fee", callback_data=f"{CALLBACK_FEE_SELECT_PREFIX}0.7|||{trade_id}")
            ]]
            reply_text = (f"**Confirm Crypto Deal: `{trade_id}`**\n\nReceived: ${received_amount:,.2f}\n\nPlease select the escrow fee percentage:")
            await update.message.reply_text(reply_text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode=ParseMode.MARKDOWN)
            
    except Exception as e:
        logger.error(f"Error in handle_new_deal: {e}", exc_info=True)
        await update.message.reply_text("❌ **Error:** Could not process the forwarded message.", parse_mode=ParseMode.MARKDOWN)

async def select_crypto_fee(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    try:
        _, fee_percent_str, trade_id = query.data.split('|||')
        pending_deals = context.user_data.get('pending_crypto_deals', {})
        if trade_id not in pending_deals:
            await query.edit_message_text("❌ **Error:** This deal selection has expired. Please forward the deal message again.")
            return
        deal_data = pending_deals.pop(trade_id)
        received_amount = deal_data['received_amount']
        escrowed_by = deal_data['escrowed_by']
        user_id = query.from_user.id
        fee = received_amount * (float(fee_percent_str) / 100.0)
        await insert_and_confirm_deal(
            context, query.message.chat_id, user_id=user_id, currency='crypto', 
            received_amount=received_amount, fee=fee, trade_id=trade_id, 
            escrowed_by=escrowed_by, original_message_id_to_edit=query.message.message_id
        )
    except (ValueError, KeyError, IndexError) as e:
        logger.error(f"Error parsing crypto fee callback: {e}", exc_info=True)
        await query.edit_message_text("❌ **Error:** Could not process your selection. Please try again.")
    except Exception as e:
        logger.error(f"Error in select_crypto_fee: {e}", exc_info=True)
        await query.edit_message_text("❌ An unexpected error occurred.")

async def insert_and_confirm_deal(context: ContextTypes.DEFAULT_TYPE, chat_id: int, *, user_id: int, currency: str, received_amount: float, fee: float, trade_id: str, escrowed_by: str, original_message_id_to_edit: int = None):
    release_amount = received_amount - fee
    now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
    sql = """
        INSERT INTO transactions 
        (user_id, currency, received_amount, release_amount, fee, trade_id, received_date, escrowed_by, status) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'holding');
    """
    await db_query(sql, (user_id, currency, received_amount, release_amount, fee, trade_id, now_utc, escrowed_by), fetch="none")
    symbol = '₹' if currency == 'inr' else '$'
    reply_text = (f"✅ **New {currency.upper()} Escrow Added!**\n\n"
                  f"🆔 **Trade ID:** `{trade_id}`\n"
                  f"📥 **Received:** {symbol}{received_amount:,.2f}\n"
                  f"💸 **Fee Cut:** {symbol}{fee:,.2f}\n"
                  f"📤 **To Release:** {symbol}{release_amount:,.2f}")
    if original_message_id_to_edit:
        await context.bot.edit_message_text(chat_id=chat_id, message_id=original_message_id_to_edit, text=reply_text, parse_mode=ParseMode.MARKDOWN)
    else:
        await context.bot.send_message(chat_id=chat_id, text=reply_text, parse_mode=ParseMode.MARKDOWN)

async def handle_completed_deal_forward(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await register_user(update)
    user_id = update.effective_user.id
    trade_id_match = re.search(r"🆔?\s*Trade ID: (#\w+)", update.message.text)
    if not trade_id_match:
        await update.message.reply_text("❌ **Error:** Could not find `Trade ID:` in 'Deal Completed' message.", parse_mode=ParseMode.MARKDOWN)
        return
    trade_id = trade_id_match.group(1)
    now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
    sql = "UPDATE transactions SET status='completed', released_date=%s WHERE trade_id=%s AND user_id=%s AND status='holding'"
    rowcount = await db_query(sql, (now_utc, trade_id, user_id), fetch="rowcount")
    if rowcount > 0:
        await update.message.reply_text(f"✅ **Deal Completed:** `{trade_id}` has been marked as completed.", parse_mode=ParseMode.MARKDOWN)
    else:
        await update.message.reply_text(f"⚠️ **Not Found:** No pending transaction for `{trade_id}` found to complete.", parse_mode=ParseMode.MARKDOWN)

# --- Dashboard and Report Handlers ---
async def show_inr_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(context)
    if not query_user_id: return
    sql = """
        SELECT SUM(received_amount), 
               (SELECT array_agg(trade_id || '|||' || received_amount ORDER BY received_date) 
                FROM transactions WHERE currency='inr' AND status='holding' AND user_id=%s)
        FROM transactions 
        WHERE currency='inr' AND status='holding' AND user_id=%s;
    """
    result = await db_query(sql, (query_user_id, query_user_id), fetch="one")
    holding = result[0] or 0.0
    pending_raw = result[1] or []
    pending = [p.split('|||') for p in pending_raw]
    release_buttons = [[KeyboardButton(f"Release {trade_id} (₹{float(amount):,.2f})")] for trade_id, amount in pending]
    is_managing = 'managed_user_id' in context.user_data
    back_button_text = BTN_BACK_TO_ADMIN_PANEL if is_managing else BTN_BACK_TO_USER_MENU
    back_button = [[KeyboardButton(back_button_text)]]
    keyboard = ReplyKeyboardMarkup(release_buttons + back_button, resize_keyboard=True, one_time_keyboard=True)
    text = f"🇮🇳 **INR DASHBOARD**\n\n💵 **Holding:** ₹{holding:,.2f}\n\n⬇️ **Pending Releases:**"
    if not pending: text += "\nNo pending INR releases."
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)

async def show_crypto_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(context)
    if not query_user_id: return
    sql = """
        SELECT 
            (SELECT SUM(received_amount) FROM transactions WHERE currency='crypto' AND status='holding' AND user_id=%s),
            (SELECT SUM(fee) FROM transactions WHERE currency='crypto' AND user_id=%s),
            (SELECT array_agg(trade_id || '|||' || received_amount ORDER BY received_date) 
             FROM transactions WHERE currency='crypto' AND status='holding' AND user_id=%s)
    """
    result = await db_query(sql, (query_user_id, query_user_id, query_user_id), fetch="one")
    holding = result[0] or 0.0
    fees = result[1] or 0.0
    pending_raw = result[2] or []
    
    pending = [p.split('|||') for p in pending_raw]
    release_buttons = [[KeyboardButton(f"Release {trade_id} (${float(amount):,.2f})")] for trade_id, amount in pending]
    is_managing = 'managed_user_id' in context.user_data
    back_button_text = BTN_BACK_TO_ADMIN_PANEL if is_managing else BTN_BACK_TO_USER_MENU
    back_button = [[KeyboardButton(back_button_text)]]
    keyboard = ReplyKeyboardMarkup(release_buttons + back_button, resize_keyboard=True, one_time_keyboard=True)
    text = f"💰 **CRYPTO DASHBOARD**\n\n💵 **Holding:** ${holding:,.2f}\n⚡️ **Fees Earned:** ${fees:,.2f}\n\n⬇️ **Pending Releases:**"
    if not pending: text += "\nNo pending crypto releases."
    await update.message.reply_text(text, reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)

async def release_funds(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(context)
    if not query_user_id: return
    trade_id_match = re.search(r"Release (#\w+)", update.message.text)
    if not trade_id_match: return
    trade_id = trade_id_match.group(1)
    currency_result = await db_query(
        "SELECT currency FROM transactions WHERE trade_id=%s AND user_id=%s AND status='holding'",
        (trade_id, query_user_id), fetch="one"
    )
    if not currency_result:
        await update.message.reply_text(f"⚠️ Transaction `{trade_id}` not found or already completed.", parse_mode=ParseMode.MARKDOWN)
        return
    currency = currency_result[0]
    now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
    await db_query(
        "UPDATE transactions SET status='completed', released_date=%s WHERE trade_id=%s AND user_id=%s",
        (now_utc, trade_id, query_user_id), fetch="none"
    )
    await update.message.reply_text(f"✅ **Funds Released!**\nTrade ID `{trade_id}` is now complete.", parse_mode=ParseMode.MARKDOWN)
    if currency == 'inr':
        await show_inr_dashboard(update, context)
    else:
        await show_crypto_dashboard(update, context)

async def show_total_holding(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(context)
    if not query_user_id: return
    holdings = await db_query(
        "SELECT currency, SUM(received_amount) FROM transactions WHERE status='holding' AND user_id=%s GROUP BY currency",
        (query_user_id,)
    )
    text = "📊 **TOTAL HOLDING**\n\n"
    if not holdings or all(h[1] is None or h[1] == 0 for h in holdings):
        text += "No funds are currently held in escrow."
    else:
        for currency, amount in holdings:
            if amount and amount > 0:
                symbol = '₹' if currency == 'inr' else '$'
                text += f"▪️ {currency.upper()}: {symbol}{amount:,.2f}\n"
    is_managing = 'managed_user_id' in context.user_data
    reply_markup = ADMIN_WATCH_KEYBOARD if is_managing else USER_KEYBOARD
    await update.message.reply_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

async def show_pending_releases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(context)
    if not query_user_id: return
    
    pending = await db_query(
        """SELECT trade_id, currency, received_amount, release_amount, fee, received_date, escrowed_by 
           FROM transactions WHERE status='holding' AND user_id=%s ORDER BY received_date ASC""",
        (query_user_id,)
    )
    is_managing = 'managed_user_id' in context.user_data
    reply_markup = ADMIN_WATCH_KEYBOARD if is_managing else USER_KEYBOARD
    
    if not pending:
        await update.message.reply_text("✅ **NO PENDING RELEASES**", reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
        return
        
    TELEGRAM_MSG_LIMIT = 4000
    current_chunk_parts = [f"⏳ **PENDING RELEASES ({len(pending)})**\n"]
    for deal in pending:
        trade_id, currency, received, release, fee, date_obj, escrowed_by = deal
        symbol = '₹' if currency == 'inr' else '$'
        safe_escrowed_by = escape_md_v1(escrowed_by.strip() or 'N/A')
        deal_text = (
            f"\n\n🟩 **ESCROW DEAL** 🟩\n"
            f"**ID**: `{trade_id}`\n"
            f"**Received**: {symbol}{received:,.2f}\n"
            f"**Fee**: {symbol}{fee:,.2f}\n"
            f"**Release**: **{symbol}{release:,.2f}**\n"
            f"**Date**: {format_datetime_ist(date_obj)}\n"
            f"**Escrowed By**: {safe_escrowed_by}"
        )
        if len("".join(current_chunk_parts)) + len(deal_text) > TELEGRAM_MSG_LIMIT:
            await update.message.reply_text("".join(current_chunk_parts), parse_mode=ParseMode.MARKDOWN)
            current_chunk_parts = []
        current_chunk_parts.append(deal_text)

    if current_chunk_parts:
        final_text = "".join(current_chunk_parts)
        if not final_text.startswith("⏳"):
             final_text = f"...(continued)\n{final_text}"
        await update.message.reply_text(text=final_text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

async def show_fees_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_managing = 'managed_user_id' in context.user_data
    back_button_text = BTN_BACK_TO_ADMIN_PANEL if is_managing else BTN_BACK_TO_USER_MENU
    keyboard = ReplyKeyboardMarkup([
        [KeyboardButton(BTN_FEES_TODAY), KeyboardButton(BTN_FEES_WEEKLY)],
        [KeyboardButton(BTN_FEES_MONTHLY), KeyboardButton(BTN_FEES_ALL_TIME)],
        [KeyboardButton(back_button_text)]
    ], resize_keyboard=True)
    await update.message.reply_text("💸 **Fee Report**\n\nSelect a time period.", reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)

async def show_volume_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    is_managing = 'managed_user_id' in context.user_data
    back_button_text = BTN_BACK_TO_ADMIN_PANEL if is_managing else BTN_BACK_TO_USER_MENU
    keyboard = ReplyKeyboardMarkup([
        [KeyboardButton(BTN_VOLUME_TODAY), KeyboardButton(BTN_VOLUME_WEEKLY)],
        [KeyboardButton(BTN_VOLUME_MONTHLY), KeyboardButton(BTN_VOLUME_ALL_TIME)],
        [KeyboardButton(back_button_text)]
    ], resize_keyboard=True)
    await update.message.reply_text("📈 **Escrow Volume Report**\n\nSelect a time period.", reply_markup=keyboard, parse_mode=ParseMode.MARKDOWN)

async def calculate_and_send_fees(update: Update, context: ContextTypes.DEFAULT_TYPE, start_utc: datetime, end_utc: datetime, title: str):
    query_user_id = get_user_id_for_query(context)
    if not query_user_id: return
    results = await db_query(
        """SELECT currency, SUM(fee) FROM transactions 
           WHERE user_id=%s AND received_date BETWEEN %s AND %s 
           GROUP BY currency""",
        (query_user_id, start_utc, end_utc)
    )
    text = f"💸 **{title}**\n\n"
    if not any(r[1] for r in results): text += "No fees earned in this period."
    else:
        for currency, amount in results:
            if amount and amount > 0: text += f"▪️ {currency.upper()}: {'₹' if currency == 'inr' else '$'}{amount:,.2f}\n"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def calculate_and_send_volume(update: Update, context: ContextTypes.DEFAULT_TYPE, start_utc: datetime, end_utc: datetime, title: str):
    query_user_id = get_user_id_for_query(context)
    if not query_user_id: return
    results = await db_query(
        """SELECT currency, SUM(received_amount) FROM transactions 
           WHERE user_id=%s AND received_date BETWEEN %s AND %s 
           GROUP BY currency""",
        (query_user_id, start_utc, end_utc)
    )
    text = f"📈 **{title}**\n\n"
    if not any(r[1] for r in results): text += "No escrow deals were started in this period."
    else:
        for currency, amount in results:
            if amount and amount > 0: text += f"▪️ {currency.upper()}: {'₹' if currency == 'inr' else '$'}{amount:,.2f}\n"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def show_fees_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start, end = get_time_range("today")
    await calculate_and_send_fees(update, context, start, end, "FEES EARNED TODAY")

async def show_fees_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start, end = get_time_range("weekly")
    await calculate_and_send_fees(update, context, start, end, "FEES EARNED THIS WEEK")

async def show_fees_monthly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start, end = get_time_range("monthly")
    await calculate_and_send_fees(update, context, start, end, "FEES EARNED THIS MONTH")

async def show_fees_all_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(context)
    if not query_user_id: return
    fees = await db_query(
        "SELECT currency, SUM(fee) FROM transactions WHERE user_id=%s GROUP BY currency",
        (query_user_id,)
    )
    text = "💸 **ALL-TIME FEES EARNED**\n\n"
    fee_lines = [
        f"▪️ {currency.upper()}: {'₹' if currency == 'inr' else '$'}{amount:,.2f}"
        for currency, amount in fees if amount and amount > 0
    ]
    text += "\n".join(fee_lines) if fee_lines else "No fees have been earned yet."
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def show_volume_today(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start, end = get_time_range("today")
    await calculate_and_send_volume(update, context, start, end, "ESCROW VOLUME TODAY")

async def show_volume_weekly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start, end = get_time_range("weekly")
    await calculate_and_send_volume(update, context, start, end, "ESCROW VOLUME THIS WEEK")

async def show_volume_monthly(update: Update, context: ContextTypes.DEFAULT_TYPE):
    start, end = get_time_range("monthly")
    await calculate_and_send_volume(update, context, start, end, "ESCROW VOLUME THIS MONTH")

async def show_volume_all_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query_user_id = get_user_id_for_query(context)
    if not query_user_id: return
    volumes = await db_query(
        "SELECT currency, SUM(received_amount) FROM transactions WHERE user_id=%s GROUP BY currency",
        (query_user_id,)
    )
    text = "📈 **ALL-TIME ESCROW VOLUME**\n\n"
    volume_lines = [
        f"▪️ {currency.upper()}: {'₹' if currency == 'inr' else '$'}{amount:,.2f}"
        for currency, amount in volumes if amount and amount > 0
    ]
    text += "\n".join(volume_lines) if volume_lines else "No deals have been processed yet."
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

# --- Admin Panel Functions ---
async def show_global_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != BOT_OWNER_ID: return
    sql = """
        SELECT
            (SELECT COUNT(*) FROM users),
            (SELECT COUNT(*) FROM transactions WHERE status='holding'),
            (SELECT jsonb_object_agg(currency, total_fee) FROM (SELECT currency, SUM(fee) as total_fee FROM transactions GROUP BY currency) as fees),
            (SELECT jsonb_object_agg(currency, total_holding) FROM (SELECT currency, SUM(received_amount) as total_holding FROM transactions WHERE status='holding' GROUP BY currency) as holdings)
    """
    total_users, pending_deals, fees_json, holdings_json = await db_query(sql, fetch="one")
    text = f"🌐 **Global Bot Statistics**\n\n👥 **Total Users:** {total_users:,}\n⏳ **Pending Deals:** {pending_deals:,}\n\n"
    text += "💰 **Total Fees Earned (All Time)**\n"
    if not fees_json: text += "  - No fees earned yet.\n"
    else:
        for curr, amount in fees_json.items(): text += f"  - `{curr.upper()}`: {'₹' if curr == 'inr' else '$'}{amount or 0:,.2f}\n"
    text += "\n📊 **Total Funds Holding (Current)**\n"
    if not holdings_json: text += "  - No funds are being held.\n"
    else:
        for curr, amount in holdings_json.items(): text += f"  - `{curr.upper()}`: {'₹' if curr == 'inr' else '$'}{amount or 0:,.2f}\n"
    await update.message.reply_text(text, parse_mode=ParseMode.MARKDOWN)

async def show_all_pending_deals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != BOT_OWNER_ID: return
    sql = """
        SELECT
            t.trade_id, t.currency, t.received_amount, t.release_amount,
            t.fee, t.received_date, t.escrowed_by,
            u.first_name, u.username
        FROM
            transactions AS t
        JOIN
            users AS u ON t.user_id = u.user_id
        WHERE
            t.status = 'holding'
        ORDER BY
            t.received_date ASC
    """
    all_pending = await db_query(sql)

    reply_markup = ReplyKeyboardMarkup([
        [KeyboardButton(BTN_ADMIN_GLOBAL_STATS), KeyboardButton(BTN_ADMIN_ALL_PENDING)],
        [KeyboardButton(BTN_ADMIN_EXPORT_DATA), KeyboardButton(BTN_ADMIN_BROADCAST)],
        [KeyboardButton(BTN_BACK_TO_ADMIN_PANEL)]
    ], resize_keyboard=True)

    if not all_pending:
        await update.message.reply_text("✅ **NO PENDING DEALS GLOBALLY**", reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)
        return
        
    TELEGRAM_MSG_LIMIT = 4000
    current_chunk_parts = [f"🌐 **ALL PENDING DEALS ({len(all_pending)})**\n"]
    for deal in all_pending:
        trade_id, currency, received, release, fee, date_obj, escrowed_by, first_name, username = deal
        symbol = '₹' if currency == 'inr' else '$'
        user_display_name = escape_md_v1(first_name)
        if username:
            user_display_name += f" (@{escape_md_v1(username)})"
        safe_escrowed_by = escape_md_v1(escrowed_by.strip() or 'N/A')
        deal_text = (
            f"\n\n🟩 **ESCROW DEAL** 🟩\n"
            f"**User**: {user_display_name}\n"
            f"**ID**: `{trade_id}`\n"
            f"**Received**: {symbol}{received:,.2f}\n"
            f"**Fee**: {symbol}{fee:,.2f}\n"
            f"**Release**: **{symbol}{release:,.2f}**\n"
            f"**Date**: {format_datetime_ist(date_obj)}\n"
            f"**Escrowed By**: {safe_escrowed_by}"
        )

        if len("".join(current_chunk_parts)) + len(deal_text) > TELEGRAM_MSG_LIMIT:
            await update.message.reply_text("".join(current_chunk_parts), parse_mode=ParseMode.MARKDOWN)
            current_chunk_parts = []
        current_chunk_parts.append(deal_text)

    if current_chunk_parts:
        final_text = "".join(current_chunk_parts)
        if not final_text.startswith("🌐"):
             final_text = f"...(continued)\n{final_text}"
        await update.message.reply_text(
            text=final_text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.MARKDOWN
        )

async def start_watching_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != BOT_OWNER_ID: return
    match = re.search(r'\((\d+)\)', update.message.text)
    if not match:
        await update.message.reply_text("Could not identify the user from the button.")
        return
    target_user_id = int(match.group(1))
    name_to_find = update.message.text.replace(WATCH_USER_PREFIX, "").split(" (")[0]
    context.user_data['managed_user_id'] = target_user_id
    await update.message.reply_text(
        f"🎭 You are now watching **{name_to_find}**. All dashboard buttons will now show their data.",
        reply_markup=ADMIN_WATCH_KEYBOARD,
        parse_mode=ParseMode.MARKDOWN
    )

async def export_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Initiates the data export process."""
    if update.effective_user.id != BOT_OWNER_ID: return
    await update.message.reply_text("⏳ Generating CSV export... This may take a moment.")
    chat_id = update.effective_chat.id
    context.application.create_task(
        _do_export_data(context, chat_id=chat_id)
    )

async def _do_export_data(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Performs the actual data fetching and file sending in the background."""
    try:
        output = io.StringIO()
        writer = csv.writer(output)
        
        all_transactions = await db_query(
            "SELECT id, user_id, currency, received_amount, release_amount, fee, trade_id, status, received_date, released_date, escrowed_by FROM transactions ORDER BY id",
            fetch="all"
        )
        
        if all_transactions:
            writer.writerow(["id", "user_id", "currency", "received_amount", "release_amount", "fee", "trade_id", "status", "received_date_utc", "released_date_utc", "escrowed_by"])
            writer.writerows(all_transactions)
            output.seek(0)
            await context.bot.send_document(
                chat_id=chat_id,
                document=InputFile(output, filename=f"transactions_{datetime.now(IST):%Y-%m-%d}.csv"),
                caption="Full export of the `transactions` table."
            )
        else:
            await context.bot.send_message(chat_id=chat_id, text="No transaction data to export.")
            
    except Exception as e:
        logger.error(f"Failed to export data: {e}", exc_info=True)
        await context.bot.send_message(chat_id=chat_id, text="❌ An error occurred during the export.")

async def broadcast_job(context: ContextTypes.DEFAULT_TYPE):
    job_data = context.job.data
    from_chat_id, message_id, admin_chat_id = job_data['from_chat_id'], job_data['message_id'], job_data['admin_chat_id']
    user_rows = await db_query("SELECT user_id FROM users WHERE user_id != %s", (BOT_OWNER_ID,))
    user_ids = [row[0] for row in user_rows]
    sent_count, failed_count = 0, 0
    for user_id in user_ids:
        try:
            await context.bot.copy_message(chat_id=user_id, from_chat_id=from_chat_id, message_id=message_id)
            sent_count += 1
            if sent_count % 25 == 0: await asyncio.sleep(1)
        except Exception as e:
            logger.warning(f"Broadcast to user {user_id} failed: {e}")
            failed_count += 1
    await context.bot.send_message(
        chat_id=admin_chat_id,
        text=f"✅ **Broadcast Complete!**\n\nSent: {sent_count}\nFailed: {failed_count}",
        parse_mode=ParseMode.MARKDOWN
    )

# --- Conversation Handlers (Broadcast) ---
async def universal_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await admin_menu(update, context)
    return ConversationHandler.END

async def broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📣 **Broadcast Mode**\nSend any message (text, photo, sticker, etc.) to broadcast. /cancel to return.", parse_mode=ParseMode.MARKDOWN)
    return BROADCAST_MESSAGE

async def broadcast_get_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data['broadcast_from_chat_id'] = update.message.chat_id
    context.user_data['broadcast_message_id'] = update.message.message_id
    keyboard = ReplyKeyboardMarkup([['yes']], one_time_keyboard=True, resize_keyboard=True)
    await update.message.reply_text("This is the message to send to ALL users. Reply 'yes' to send, or /cancel.", reply_markup=keyboard)
    await update.message.copy(chat_id=update.effective_chat.id)
    return BROADCAST_CONFIRM

async def broadcast_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text.lower() != 'yes': return await universal_cancel(update, context)
    context.job_queue.run_once(
        broadcast_job, when=1,
        data={
            'from_chat_id': context.user_data.pop('broadcast_from_chat_id'),
            'message_id': context.user_data.pop('broadcast_message_id'),
            'admin_chat_id': update.effective_chat.id
        }, name=f"broadcast_{update.effective_chat.id}"
    )
    await update.message.reply_text("🚀 **Broadcast scheduled!** Sending in the background. I will notify you when it's complete.", parse_mode=ParseMode.MARKDOWN)
    await admin_menu(update, context)
    return ConversationHandler.END
    
# --- Message Router ---
async def message_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not (update.message and update.message.text): return
    await register_user(update)
    if 'original_user_id' not in context.user_data:
        context.user_data['original_user_id'] = update.effective_user.id
    text = update.message.text
    user_id = update.effective_user.id

    user_handlers = {
        BTN_INR_DASH: show_inr_dashboard,
        BTN_CRYPTO_DASH: show_crypto_dashboard,
        BTN_TOTAL_FUNDS: show_total_holding,
        BTN_PENDING: show_pending_releases,
        BTN_TOTAL_FEES: show_fees_menu,
        BTN_ESCROW_VOLUME: show_volume_menu,
        BTN_BACK_TO_USER_MENU: start,
        BTN_FEES_TODAY: show_fees_today,
        BTN_FEES_WEEKLY: show_fees_weekly,
        BTN_FEES_MONTHLY: show_fees_monthly,
        BTN_FEES_ALL_TIME: show_fees_all_time,
        BTN_VOLUME_TODAY: show_volume_today,
        BTN_VOLUME_WEEKLY: show_volume_weekly,
        BTN_VOLUME_MONTHLY: show_volume_monthly,
        BTN_VOLUME_ALL_TIME: show_volume_all_time,
    }
    admin_handlers = {
        BTN_ADMIN_GLOBAL_STATS: show_global_stats,
        BTN_ADMIN_ALL_PENDING: show_all_pending_deals,
        BTN_ADMIN_EXPORT_DATA: export_data,
        BTN_BACK_TO_ADMIN_PANEL: admin_menu,
    }
    handler = user_handlers.get(text)
    if user_id == BOT_OWNER_ID:
        handler = admin_handlers.get(text, handler)

    if handler:
        await handler(update, context)
    elif text.startswith("Release "):
        await release_funds(update, context)
    elif user_id == BOT_OWNER_ID and text.startswith(WATCH_USER_PREFIX):
        await start_watching_user(update, context)
    elif user_id != BOT_OWNER_ID:
        await update.message.reply_text("Please use the buttons or forward a deal message.")

def main():
    if not all([TOKEN, BOT_OWNER_ID, DATABASE_URL]):
        logger.critical("FATAL: Configuration variables missing (TELEGRAM_TOKEN, BOT_OWNER_ID, DATABASE_URL).")
        sys.exit(1)
        
    initialize_db_pool()
    persistence = PicklePersistence(filepath=PERSISTENCE_FILE)
    application = Application.builder().token(TOKEN).persistence(persistence).build()

    broadcast_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Text([BTN_ADMIN_BROADCAST]) & filters.User(user_id=BOT_OWNER_ID), broadcast_start)],
        states={
            BROADCAST_MESSAGE: [MessageHandler(filters.ALL & ~filters.COMMAND, broadcast_get_message)],
            BROADCAST_CONFIRM: [MessageHandler(filters.Regex(re.compile(r'yes', re.IGNORECASE)), broadcast_send)],
        },
        fallbacks=[CommandHandler('cancel', universal_cancel)],
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin_panel_command, filters=filters.User(user_id=BOT_OWNER_ID)))
    application.add_handler(broadcast_handler)
    application.add_handler(MessageHandler(filters.FORWARDED & filters.TEXT & filters.Regex("Continue the Deal"), handle_new_deal))
    application.add_handler(MessageHandler(filters.FORWARDED & filters.TEXT & filters.Regex("Deal Completed"), handle_completed_deal_forward))
    application.add_handler(CallbackQueryHandler(select_crypto_fee, pattern=f"^{CALLBACK_FEE_SELECT_PREFIX}"))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_router))
    
    logger.info("✅ Bot is configured and ready to start polling.")
    application.run_polling()

if __name__ == "__main__":
    main()
