from flask import Flask, render_template, request, jsonify
import requests
import time
import hmac
import hashlib
import json
import threading
from threading import Lock
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
import pymysql
from pymysql import OperationalError, InterfaceError
from dbutils.pooled_db import PooledDB
from math import isfinite
import subprocess
from decimal import Decimal, ROUND_HALF_UP
import time as _time
import random
from datetime import datetime
import websocket
import bot_manager

# ========== LOGGING CONFIGURATION ==========
LOG_MAX_BYTES = 20_000_000  # 20MB cap per log file
_LOG_ROTATE_LAST = 0.0
_LOG_ROTATE_LOCK = Lock()
_LOG_ROTATE_INTERVAL = 300  # check at most every 5 min


def _rotate_log_if_needed(filepath, max_bytes=LOG_MAX_BYTES):
    """Trim oversized log files — keeps last half. Does not touch trading logic."""
    try:
        if not os.path.exists(filepath):
            return
        if os.path.getsize(filepath) <= max_bytes:
            return
        with open(filepath, "rb") as f:
            f.seek(-(max_bytes // 2), os.SEEK_END)
            data = f.read()
        with open(filepath, "wb") as f:
            f.write(data)
    except Exception:
        pass


def _maybe_rotate_logs():
    """Throttled rotation check — avoids doing this on every log line."""
    global _LOG_ROTATE_LAST
    now = _time.time()
    with _LOG_ROTATE_LOCK:
        if now - _LOG_ROTATE_LAST < _LOG_ROTATE_INTERVAL:
            return
        _LOG_ROTATE_LAST = now
    _rotate_log_if_needed("server.log")
    _rotate_log_if_needed("trade.log")


def _tail_read_lines(filepath, limit=100, chunk_size=200_000):
    """Read last N lines without loading the entire file into memory."""
    if not os.path.exists(filepath):
        return [], 0
    file_size = os.path.getsize(filepath)
    if file_size == 0:
        return [], 0
    with open(filepath, "rb") as f:
        seek_pos = max(0, file_size - chunk_size)
        f.seek(seek_pos)
        data = f.read().decode("utf-8", errors="ignore")
    lines = data.splitlines()
    if seek_pos > 0 and lines:
        lines = lines[1:]
    return lines[-limit:], len(lines)


def _get_process_rss_mb():
    """Current process RSS in MB (Render/Linux)."""
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return round(int(line.split()[1]) / 1024, 1)
    except Exception:
        pass
    try:
        import resource
        return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
    except Exception:
        pass
    return None


def _file_size_mb(path):
    try:
        if os.path.exists(path):
            return round(os.path.getsize(path) / (1024 * 1024), 2)
    except Exception:
        pass
    return 0.0


def get_mysql_storage_stats():
    """Clever Cloud MySQL usage from information_schema (read-only)."""
    try:
        summary = execute_mysql_query(
            """
            SELECT
                ROUND(COALESCE(SUM(data_length + index_length), 0) / 1024 / 1024, 2) AS used_mb,
                COUNT(*) AS table_count
            FROM information_schema.TABLES
            WHERE table_schema = DATABASE()
            """,
            fetch_one=True,
        )
        tables = execute_mysql_query(
            """
            SELECT
                table_name,
                ROUND((data_length + index_length) / 1024 / 1024, 2) AS size_mb,
                table_rows
            FROM information_schema.TABLES
            WHERE table_schema = DATABASE()
            ORDER BY (data_length + index_length) DESC
            """,
            fetch_all=True,
        ) or []
        return {
            "used_mb": float(summary["used_mb"] or 0) if summary else 0.0,
            "table_count": int(summary["table_count"] or 0) if summary else 0,
            "tables": [
                {
                    "name": t["table_name"],
                    "size_mb": float(t["size_mb"] or 0),
                    "rows": int(t["table_rows"] or 0),
                }
                for t in tables
            ],
        }
    except Exception as e:
        return {"used_mb": None, "table_count": 0, "tables": [], "error": str(e)}


class Logger(object):
    def __init__(self, filename="server.log", secondary_file="trade.log"):
        _rotate_log_if_needed(filename)
        _rotate_log_if_needed(secondary_file)
        self.terminal = sys.stdout
        self.log = open(filename, "a", encoding="utf-8", buffering=1)  # Changed from w to a to append
        self.trade_log_file = open(secondary_file, "a", encoding="utf-8", buffering=1)  # Changed from w to a to append
        self.filename = filename
        self.secondary_file = secondary_file

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()
        try:
            os.fsync(self.log.fileno())
        except:
            pass

    def trade_write(self, category, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        formatted_msg = f"[{timestamp}] [{category}] {message}\n"
        self.terminal.write(formatted_msg)
        self.log.write(formatted_msg)
        self.trade_log_file.write(formatted_msg)
        self.log.flush()
        self.trade_log_file.flush()
        try:
            os.fsync(self.trade_log_file.fileno())
        except:
            pass
        _maybe_rotate_logs()

    def flush(self):
        self.terminal.flush()
        self.log.flush()
        self.trade_log_file.flush()

custom_logger = Logger("server.log", "trade.log")
sys.stdout = custom_logger
sys.stderr = custom_logger

def log_trade(msg): custom_logger.trade_write("TRADE", msg)
def log_state(msg): custom_logger.trade_write("STATE", msg)
def log_error(msg): custom_logger.trade_write("ERROR", msg)
def log_system(msg): custom_logger.trade_write("SYSTEM", msg)

# ========== LOG THROTTLE HELPER (NEW) ==========
# Purpose: kuch logs (WS mark price ticks, break-even debug) har 1-2 second
# me fire hote the -> 10 din me disk full ho sakta tha -> process crash ->
# bot "khud se band" ho jaata tha. Ye sirf un high-frequency logs ko
# throttle karta hai (default: har symbol/key ke liye 30s me ek baar).
# Trading/order/step logic pe ZERO effect hai, sirf logging kam hua hai.
_LOG_THROTTLE_LAST = {}
_LOG_THROTTLE_LOCK = Lock()

def log_throttled(category_fn, key, msg, min_interval=30):
    now = _time.time()
    with _LOG_THROTTLE_LOCK:
        last = _LOG_THROTTLE_LAST.get(key, 0)
        if now - last < min_interval:
            return
        _LOG_THROTTLE_LAST[key] = now
    category_fn(msg)

import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

load_dotenv()

from keepalive import start_keep_alive

app = Flask(__name__)
app.secret_key = os.urandom(24)

LAST_SAVED_TRADE_KEY = None

# ========== MYSQL CONNECTION POOL ==========
# Using a connection pool instead of opening a brand-new connection on every
# query. This is the root fix for:
#   (1226, "User '...' has exceeded the 'max_user_connections' resource (current value: 5)")
#
# maxconnections is kept LOW (well under the server's max_user_connections=5)
# so we never hit the limit even with multiple threads
# (main bot loop + websocket engine + TP/SL guardian) hitting the DB at once.
#
# blocking=True means if all pooled connections are busy, a caller will WAIT
# for one to free up instead of opening a new one / failing immediately.
# MYSQL_POOL = PooledDB(
#     creator=pymysql,
#     maxconnections=4,       # stay safely under server limit of 5
#     mincached=1,
#     maxcached=4,
#     maxshared=0,
#     blocking=True,          # wait for a free connection instead of erroring
#     maxusage=None,          # recycle connection indefinitely (no forced close after N uses)
#     setsession=[],
#     ping=1,                 # ping connection before use; reconnect if stale (fixes "Lost connection")
#     host=os.getenv('MYSQL_HOST', 'bmh1rsh5f0sjmncv6ydc-mysql.services.clever-cloud.com'),
#     port=int(os.getenv('MYSQL_PORT', 3306)),
#     user=os.getenv('MYSQL_USER', 'ujokhsx1defubkot'),
#     password=os.getenv('MYSQL_PASSWORD', 'hILZGFpJ60exq4oGj2hv'),
#     database=os.getenv('MYSQL_DB', 'bmh1rsh5f0sjmncv6ydc'),
#     charset='utf8mb4',
#     cursorclass=pymysql.cursors.DictCursor,
#     autocommit=False,
#     ssl={}
# )


MYSQL_POOL = PooledDB(
    creator=pymysql,
    maxconnections=4,       # stay safely under server limit of 5
    mincached=1,
    maxcached=4,
    maxshared=0,
    blocking=True,          # wait for a free connection instead of erroring
    maxusage=None,          # recycle connection indefinitely (no forced close after N uses)
    setsession=[],
    ping=1,                 # ping connection before use; reconnect if stale (fixes "Lost connection")
    host=os.getenv('MYSQL_HOST', 'bqebck7vtzt0e6ugzqsf-mysql.services.clever-cloud.com'),
    port=int(os.getenv('MYSQL_PORT', 3306)),
    user=os.getenv('MYSQL_USER', 'uihxwgphvyf5bhxa'),
    password=os.getenv('MYSQL_PASSWORD', 'oCPY54FrSIHfMBHgVBrx'),
    database=os.getenv('MYSQL_DB', 'bqebck7vtzt0e6ugzqsf'),
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=False,
    ssl={}
)


def get_mysql_connection():
    try:
        connection = MYSQL_POOL.connection()
        return connection
    except Exception as e:
        log_error(f"MySQL connection failed: {e}")
        raise


def execute_mysql_query(query, params=None, fetch_one=False, fetch_all=False, commit=False):
    connection = None
    try:
        connection = get_mysql_connection()
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())
            if commit:
                connection.commit()
                return cursor.lastrowid if hasattr(cursor, 'lastrowid') else True
            if fetch_one:
                return cursor.fetchone()
            elif fetch_all:
                return cursor.fetchall()
            else:
                return True
    except Exception as e:
        if connection:
            try:
                connection.rollback()
            except Exception:
                pass
        raise
    finally:
        if connection:
            connection.close()

def get_database_size():
    try:
        query = """
        SELECT ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS db_size_mb
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        """
        result = execute_mysql_query(query, fetch_one=True)
        return result['db_size_mb'] if result else 0
    except Exception as e:
        return 0

def cleanup_old_trades(target_size_mb=8.5):
    try:
        current_size = get_database_size()
        if current_size <= target_size_mb:
            return True
        log_system(f"Cleanup: DB size {current_size}MB > {target_size_mb}MB limit. Starting cleanup...")
        while current_size > target_size_mb:
            count_query = "SELECT COUNT(*) as total_rows FROM closed_positions"
            total_result = execute_mysql_query(count_query, fetch_one=True)
            total_rows = total_result['total_rows'] if total_result else 0
            if total_rows <= 10:
                break
            rows_to_delete = max(1, total_rows // 10)
            delete_query = """
            DELETE FROM closed_positions
            ORDER BY created_at ASC
            LIMIT %s
            """
            execute_mysql_query(delete_query, (rows_to_delete,), commit=True)
            current_size = get_database_size()
        log_system(f"Cleanup completed. Final size: {current_size}MB")
        return True
    except Exception as e:
        log_error(f"Cleanup error: {e}")
        return False

# API Configuration
# Production:
BASE_URL = "https://api.india.delta.exchange"
WS_URL = "wss://socket.india.delta.exchange"
# Testnet (demo) — if you switch back, also swap PRODUCT_IDS below:
# BASE_URL = "https://cdn-ind.testnet.deltaex.org"
# WS_URL = "wss://socket-ind.testnet.deltaex.org"

# ★ Product IDs — change here when switching env
# Production (api.india.delta.exchange):
PRODUCT_IDS = {
    "PAXGUSD": 123006,   # PAX Gold
    "XAUTUSD": 131253,   # Tether Gold
}
# Testnet (cdn-ind.testnet.deltaex.org):
# PRODUCT_IDS = {
#     "PAXGUSD": 169532,
#     "XAUTUSD": 181689,
# }

fees=2


DELTA_API_KEY = os.getenv("DELTA_API_KEY")
DELTA_API_SECRET = os.getenv("DELTA_API_SECRET")

processing_lock = threading.Lock()
last_processed = {}

# =====================================================================
# Step-Based Lot Progression System
# Martingale: step n = start_lot * 2^(n-1)
# start_lot + streak (N steps) UI/DB se aate hain — reset/wrap logic same rahegi.
# =====================================================================
LOT_STEPS_DEFAULT_START = 1
LOT_STEPS_DEFAULT_STREAK = 4  # matches previous hardcoded 1,2,4,8


def build_lot_steps(start_lot, streak):
    """Build {1: S, 2: S*2, 3: S*4, ... N: S*2^(N-1)}"""
    start = float(start_lot)
    n_steps = int(streak)
    steps = {}
    for n in range(1, n_steps + 1):
        val = start * (4 ** (n - 1))
        steps[n] = int(val) if float(val).is_integer() else val
    return steps


LOT_STEPS = build_lot_steps(LOT_STEPS_DEFAULT_START, LOT_STEPS_DEFAULT_STREAK)
_lot_steps_lock = Lock()
LOT_STEPS_META = {
    'start_lot': float(LOT_STEPS_DEFAULT_START),
    'streak': int(LOT_STEPS_DEFAULT_STREAK),
}


def get_lot_steps_config():
    with _lot_steps_lock:
        return {
            'start_lot': LOT_STEPS_META['start_lot'],
            'streak': LOT_STEPS_META['streak'],
            'lot_steps': dict(LOT_STEPS),
            'max_steps': max(LOT_STEPS.keys()) if LOT_STEPS else 1,
        }


def apply_lot_steps_config(start_lot, streak):
    """Rebuild LOT_STEPS + update BOT_STATE max/base. Does NOT touch progression/reset logic."""
    global LOT_STEPS
    start = float(start_lot)
    n = int(streak)
    if start <= 0:
        return False, 'Start lot must be > 0'
    if n < 1 or n > 20:
        return False, 'Streak (steps) must be between 1 and 20'

    new_steps = build_lot_steps(start, n)
    with _lot_steps_lock:
        LOT_STEPS.clear()
        LOT_STEPS.update(new_steps)
        LOT_STEPS_META['start_lot'] = start if not float(start).is_integer() else int(start)
        LOT_STEPS_META['streak'] = n

    BOT_STATE['max_steps'] = n
    BOT_STATE['base_lot'] = LOT_STEPS[1]
    # If idle at step 1 with stale lot, sync to new start lot
    if not BOT_STATE.get('running') and BOT_STATE.get('current_step', 1) == 1:
        BOT_STATE['current_lot'] = LOT_STEPS[1]

    return True, get_lot_steps_config()


def save_lot_steps_config_to_db(cfg=None):
    try:
        data = cfg if cfg is not None else get_lot_steps_config()
        payload = {
            'start_lot': data['start_lot'],
            'streak': data['streak'],
            'saved_at': datetime.now().isoformat(),
        }
        upsert_query = '''
            INSERT INTO bot_state (state_key, state_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE state_value = VALUES(state_value), updated_at = NOW()
        '''
        execute_mysql_query(upsert_query, ('lot_steps_config', json.dumps(payload)), commit=True)
        log_system(
            f"LOT STEPS SAVED | start={payload['start_lot']} streak={payload['streak']} "
            f"seq={get_lot_steps_config()['lot_steps']}"
        )
        return True
    except Exception as e:
        log_error(f"Saving lot steps config: {e}")
        return False


def load_lot_steps_config_from_db():
    try:
        result = execute_mysql_query(
            "SELECT state_value FROM bot_state WHERE state_key = %s",
            ('lot_steps_config',),
            fetch_one=True
        )
        if not result or not result.get('state_value'):
            log_system("No saved lot steps config — using defaults, writing to DB")
            apply_lot_steps_config(LOT_STEPS_DEFAULT_START, LOT_STEPS_DEFAULT_STREAK)
            save_lot_steps_config_to_db()
            return False

        saved = json.loads(result['state_value'])
        start = float(saved.get('start_lot', LOT_STEPS_DEFAULT_START))
        streak = int(saved.get('streak', LOT_STEPS_DEFAULT_STREAK))
        ok, _ = apply_lot_steps_config(start, streak)
        if ok:
            log_system(f"LOT STEPS LOADED | start={start} streak={streak} seq={LOT_STEPS}")
        return ok
    except Exception as e:
        log_error(f"Loading lot steps config: {e}")
        return False


# Bot State
BOT_STATE = {
    'running': False,
    'thread': None,
    'current_step': 1,
    'current_lot': LOT_STEPS[1],
    'base_lot': LOT_STEPS[1],
    'leverage': 50,
    'tp_percent': 4,
    'sl_percent': 2,
    'max_steps': max(LOT_STEPS.keys()),
    'last_result': None,
   'symbol': 'PAXGUSD',
    'stop_at_win': False,
    'stop_at_max_step': False,
    'force_stop': False,
    'session_start_time': None,
    'session_total_pnl': 0.0,
    'last_placed_order_id': None,
    'order_completed': True,
}

# Bot UI config (symbol, leverage, TP/SL) — persists in MySQL, survives refresh/restart
BOT_CONFIG_DEFAULTS = {
    'symbol': 'PAXGUSD',
    'leverage': 100,
    'tp_percent': 2.0,
    'sl_percent': 1.0,
}


def get_bot_config():
    return {
        'symbol': BOT_STATE['symbol'],
        'leverage': BOT_STATE['leverage'],
        'tp_percent': float(BOT_STATE['tp_percent']),
        'sl_percent': float(BOT_STATE['sl_percent']),
    }


def save_bot_config_to_db(cfg=None):
    try:
        data = cfg if cfg is not None else get_bot_config()
        payload = {
            'symbol': str(data['symbol']).upper(),
            'leverage': int(data['leverage']),
            'tp_percent': float(data['tp_percent']),
            'sl_percent': float(data['sl_percent']),
            'saved_at': datetime.now().isoformat(),
        }
        upsert_query = '''
            INSERT INTO bot_state (state_key, state_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE state_value = VALUES(state_value), updated_at = NOW()
        '''
        execute_mysql_query(upsert_query, ('bot_config', json.dumps(payload)), commit=True)
        log_system(
            f"BOT CONFIG SAVED | {payload['symbol']} lev={payload['leverage']}x "
            f"TP={payload['tp_percent']}% SL={payload['sl_percent']}%"
        )
        return True
    except Exception as e:
        log_error(f"Saving bot config: {e}")
        return False


def load_bot_config_from_db():
    try:
        result = execute_mysql_query(
            "SELECT state_value FROM bot_state WHERE state_key = %s",
            ('bot_config',),
            fetch_one=True
        )
        if not result or not result.get('state_value'):
            log_system("No saved bot config — using defaults, writing to DB")
            BOT_STATE['symbol'] = BOT_CONFIG_DEFAULTS['symbol']
            BOT_STATE['leverage'] = BOT_CONFIG_DEFAULTS['leverage']
            BOT_STATE['tp_percent'] = BOT_CONFIG_DEFAULTS['tp_percent']
            BOT_STATE['sl_percent'] = BOT_CONFIG_DEFAULTS['sl_percent']
            save_bot_config_to_db(BOT_CONFIG_DEFAULTS)
            return False

        saved = json.loads(result['state_value'])
        allowed = {'PAXGUSD', 'XAUTUSD'}
        loaded_symbol = str(saved.get('symbol', BOT_CONFIG_DEFAULTS['symbol'])).upper()
        if loaded_symbol not in allowed:
            log_system(f"Migrating removed symbol {loaded_symbol} → PAXGUSD")
            loaded_symbol = 'PAXGUSD'
        BOT_STATE['symbol'] = loaded_symbol
        BOT_STATE['leverage'] = int(saved.get('leverage', BOT_CONFIG_DEFAULTS['leverage']))
        BOT_STATE['tp_percent'] = float(saved.get('tp_percent', BOT_CONFIG_DEFAULTS['tp_percent']))
        BOT_STATE['sl_percent'] = float(saved.get('sl_percent', BOT_CONFIG_DEFAULTS['sl_percent']))
        if loaded_symbol != str(saved.get('symbol', '')).upper():
            save_bot_config_to_db(get_bot_config())
        log_system(
            f"BOT CONFIG LOADED | {BOT_STATE['symbol']} lev={BOT_STATE['leverage']}x "
            f"TP={BOT_STATE['tp_percent']}% SL={BOT_STATE['sl_percent']}%"
        )
        return True
    except Exception as e:
        log_error(f"Loading bot config: {e}")
        return False


def apply_bot_config(data: dict):
    """Validate + apply bot UI config into memory and DB. Does not start/stop bot."""
    try:
        symbol = str(data.get('symbol', BOT_STATE['symbol'])).upper()
        leverage = int(data.get('leverage', BOT_STATE['leverage']))
        tp_percent = float(data.get('tp_percent', BOT_STATE['tp_percent']))
        sl_percent = float(data.get('sl_percent', BOT_STATE['sl_percent']))

        valid_symbols = ['PAXGUSD', 'XAUTUSD']
        if symbol not in valid_symbols:
            return False, f'Invalid symbol. Valid: {valid_symbols}'
        if leverage < 1 or leverage > 200:
            return False, 'Leverage must be integer between 1-200'
        if not (0.01 <= tp_percent <= 50):
            return False, 'TP percent must be between 0.01-50'
        if not (0.01 <= sl_percent <= 50):
            return False, 'SL percent must be between 0.01-50'

        BOT_STATE['symbol'] = symbol
        BOT_STATE['leverage'] = leverage
        BOT_STATE['tp_percent'] = tp_percent
        BOT_STATE['sl_percent'] = sl_percent
        save_bot_config_to_db(get_bot_config())
        return True, get_bot_config()
    except (TypeError, ValueError) as e:
        return False, f'Invalid values: {e}'


LOT_SIZES = {}
LOT_SIZE_DEFAULT = 1

LAST_TRADE_RESULT = {
    'profit_loss': None,
    'timestamp': None,
    'lot_used': None,
    'processed': False
}

ACTIVE_TRADE_DECISIONS = {}


LOT_CALCULATION_LOCK = False
LAST_PROCESSED_TRADE_ID = None

USED_FILL_IDS = set()
USED_FILL_IDS_LOCK = Lock()

# ========== MARK PRICE CACHE ==========
MARK_PRICES      = {}
MARK_PRICES_LOCK = Lock()



PROCESSED_ORDER_IDS = set()
PROCESSED_ORDER_IDS_LOCK = Lock()

LAST_CLOSE_TIMESTAMP = 0.0
COOLDOWN_SECONDS = 15

PROCESSED_EXIT_FILL_IDS = set()
LAST_PROCESSED_EXIT_FILL_IDS = set()

WAITING_FOR_FILL = False
TRADE_COMPLETED = False

LAST_POSITION_STATE = {
    'symbol': None,
    'size': 0,
    'entry_price': 0
}

PRODUCT_ID_CACHE = {}
db_lock = Lock()
BOT_PROCESS = None
bot_process_lock = Lock()
LAST_SAVED_TRADE_KEY = None

# ========== BOUNDED-SET HOUSEKEEPING (NEW) ==========
# USED_FILL_IDS / PROCESSED_ORDER_IDS sirf badhte hi jaate the (kabhi trim
# nahi hote the). Weeks/months chalne par ye memory me accumulate hote
# rehte - isse process slow/heavy ho sakta hai. Ye sirf size cap karta hai,
# koi bhi ID jo already processed use hui hai use dobara process nahi
# hone deta (behavior same rehta hai), bas purani entries drop hoti hain.
_MAX_TRACKED_IDS = 2000

def _trim_id_set(id_set, max_size=_MAX_TRACKED_IDS):
    if len(id_set) > max_size:
        # sets are unordered; safe approach -> drop to a smaller set keeping
        # arbitrary max_size//2 items (fine since these are just de-dup guards
        # for recent orders, not historical records)
        trimmed = set(list(id_set)[-(max_size // 2):])
        id_set.clear()
        id_set.update(trimmed)


# ========== DATABASE ==========
def init_database():
    try:
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS closed_positions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL,
                entry_price DECIMAL(20, 8) NOT NULL,
                exit_price DECIMAL(20, 8),
                quantity DECIMAL(20, 8) NOT NULL,
                pnl DECIMAL(20, 8),
                entry_time VARCHAR(50) NOT NULL,
                exit_time VARCHAR(50),
                is_latest TINYINT(1) DEFAULT 0,
                entry_order_id VARCHAR(100) DEFAULT NULL,
                trade_decisions VARCHAR(500) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_symbol (symbol),
                INDEX idx_created_at (created_at),
                INDEX idx_is_latest (is_latest),
                INDEX idx_entry_order_id (entry_order_id),
                UNIQUE KEY uq_trade (symbol, entry_time, side),
                UNIQUE KEY uq_entry_order (entry_order_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
        execute_mysql_query(create_table_query, commit=True)

        try:
            execute_mysql_query(
                "ALTER TABLE closed_positions ADD COLUMN entry_order_id VARCHAR(100) DEFAULT NULL",
                commit=True
            )
        except Exception:
            pass

        try:
            execute_mysql_query(
                "ALTER TABLE closed_positions ADD COLUMN trade_decisions VARCHAR(500) DEFAULT NULL",
                commit=True
            )
        except Exception:
            pass

        # >>> ADDED: exit_order_id column (info only — shows entry + exit order IDs
        # together in trade history UI). Does not affect any trading/decision logic.
        try:
            execute_mysql_query(
                "ALTER TABLE closed_positions ADD COLUMN exit_order_id VARCHAR(100) DEFAULT NULL",
                commit=True
            )
        except Exception:
            pass

        try:
            execute_mysql_query(
                "ALTER TABLE closed_positions ADD UNIQUE KEY uq_entry_order (entry_order_id)",
                commit=True
            )
        except Exception:
            pass

        try:
            execute_mysql_query(
                "ALTER TABLE closed_positions ADD INDEX idx_entry_order_id (entry_order_id)",
                commit=True
            )
        except Exception:
            pass

        # BE applied on this trade's position (info for signal/trade history UI)
        try:
            execute_mysql_query(
                "ALTER TABLE closed_positions ADD COLUMN had_break_even TINYINT(1) DEFAULT 0",
                commit=True
            )
        except Exception:
            pass

        create_state_table_query = '''
            CREATE TABLE IF NOT EXISTS bot_state (
                id INT AUTO_INCREMENT PRIMARY KEY,
                state_key VARCHAR(100) NOT NULL UNIQUE,
                state_value TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
        execute_mysql_query(create_state_table_query, commit=True)

        # =====================================================================
        # TABLE: recent_fills_cache
        # Stores the last 2 verified entry+exit fill pairs for the current symbol.
        # Used by pre_order_fill_verification() as ground truth for lot size.
        # =====================================================================
        create_fills_cache_query = '''
            CREATE TABLE IF NOT EXISTS recent_fills_cache (
                id INT AUTO_INCREMENT PRIMARY KEY,
                symbol VARCHAR(50) NOT NULL,
                entry_order_id VARCHAR(100) NOT NULL,
                exit_order_id VARCHAR(100) NOT NULL,
                entry_side VARCHAR(10) NOT NULL,
                entry_price DECIMAL(20, 8) NOT NULL,
                exit_price DECIMAL(20, 8) NOT NULL,
                lot_size DECIMAL(20, 8) NOT NULL,
                pnl DECIMAL(20, 8),
                entry_time VARCHAR(100) NOT NULL,
                exit_time VARCHAR(100) NOT NULL,
                saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_fill_pair (entry_order_id, exit_order_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
        execute_mysql_query(create_fills_cache_query, commit=True)

        # >>> ADDED: new table storing multi-timeframe signal score snapshots
        # (1M/5M/15M/30M/1H/2H/4H/1D/1W + final decision) at the moment an order
        # is placed. INFO ONLY — never read by any decision/strategy code.
        create_signal_scores_table_query = '''
            CREATE TABLE IF NOT EXISTS signal_scores_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_id VARCHAR(100) NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                position VARCHAR(10) NOT NULL,
                lot_size DECIMAL(20, 8) NOT NULL,
                tf_1m VARCHAR(100),
                tf_5m VARCHAR(100),
                tf_15m VARCHAR(100),
                tf_30m VARCHAR(100),
                tf_1h VARCHAR(100),
                tf_2h VARCHAR(100),
                tf_4h VARCHAR(100),
                tf_1d VARCHAR(100),
                tf_1w VARCHAR(100),
                final_decision VARCHAR(20),
                entry_time VARCHAR(50) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_signal_order (order_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        '''
        execute_mysql_query(create_signal_scores_table_query, commit=True)

        print("✅ MySQL tables ready (existing data preserved)")
    except Exception as e:
        print(f"❌ Failed to initialize MySQL database: {e}")
        raise


#decimals ke liye ek extra fuction
def round_price_2dp(price):
    """Only for TP/SL/bracket stop prices sent to exchange. NOT used for PnL math."""
    try:
        return float(Decimal(str(price)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    except Exception:
        return round(float(price), 2)

# ========== PERSISTENT STATE FUNCTIONS ==========
def save_bot_state_to_db():
    try:
        state_data = {
            'current_step': BOT_STATE['current_step'],
            'current_lot': BOT_STATE['current_lot'],
            'last_pnl': LAST_TRADE_RESULT['profit_loss'],
            'last_result': BOT_STATE.get('last_result'),
            'saved_at': datetime.now().isoformat()
        }
        upsert_query = '''
            INSERT INTO bot_state (state_key, state_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE state_value = VALUES(state_value), updated_at = NOW()
        '''
        execute_mysql_query(upsert_query, ('martingale_state', json.dumps(state_data)), commit=True)
        log_state(f"STATE SAVED | Step={state_data['current_step']} | Lot={state_data['current_lot']} | LastPnL={state_data['last_pnl']}")
    except Exception as e:
        log_error(f"Saving bot state to DB: {e}")


def load_bot_state_from_db():
    global LAST_TRADE_RESULT
    try:
        result = execute_mysql_query(
            "SELECT state_value FROM bot_state WHERE state_key = %s",
            ('martingale_state',),
            fetch_one=True
        )
        if not result or not result.get('state_value'):
            log_system("No saved bot state found - fresh start")
            return False

        state_data = json.loads(result['state_value'])
        saved_at = state_data.get('saved_at', 'unknown')

        log_state(f"LOADED Step={state_data.get('current_step')}, Lot={state_data.get('current_lot')}, Last PnL={state_data.get('last_pnl')}")

        BOT_STATE['current_step'] = int(state_data.get('current_step', 1))
        BOT_STATE['current_lot']  = float(state_data.get('current_lot', LOT_STEPS[1]))
        BOT_STATE['last_result']  = state_data.get('last_result')

        last_pnl = state_data.get('last_pnl')
        if last_pnl is not None:
            LAST_TRADE_RESULT['profit_loss'] = float(last_pnl)
            LAST_TRADE_RESULT['processed']   = True
            LAST_TRADE_RESULT['timestamp']   = saved_at

        return True
    except Exception as e:
        log_error(f"Loading bot state from DB: {e}")
        return False


def clear_bot_state_from_db():
    try:
        execute_mysql_query(
            "DELETE FROM bot_state WHERE state_key = %s",
            ('martingale_state',),
            commit=True
        )
        log_system("Bot state cleared from DB")
    except Exception as e:
        log_error(f"Clearing bot state from DB: {e}")


# ========== SESSION-PERSISTENCE (NEW) ==========
# Purpose: agar hosting platform (Render etc.) process ko kisi bhi wajah se
# restart/crash kare, toh process dobara start hone par pata chale ke bot
# "user ne UI se ON kiya tha aur abhi tak OFF nahi kiya" - aur khud-b-khud
# usi symbol/settings ke saath dobara chalu ho jaye.
# Sirf UI ke "Force Stop" / "Stop Bot" button se hi ye flag False hota hai.
def save_session_active_flag(active: bool, strategy_bot=None):
    try:
        session_data = {
            'active': active,
            'symbol': BOT_STATE.get('symbol'),
            'leverage': BOT_STATE.get('leverage'),
            'tp_percent': BOT_STATE.get('tp_percent'),
            'sl_percent': BOT_STATE.get('sl_percent'),
            'strategy_bot': strategy_bot or bot_manager.get_active_bot_id() or 'bot1',
            'saved_at': datetime.now().isoformat()
        }
        upsert_query = '''
            INSERT INTO bot_state (state_key, state_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE state_value = VALUES(state_value), updated_at = NOW()
        '''
        execute_mysql_query(upsert_query, ('session_active_flag', json.dumps(session_data)), commit=True)
    except Exception as e:
        log_error(f"Saving session_active flag: {e}")


def load_session_active_flag():
    try:
        result = execute_mysql_query(
            "SELECT state_value FROM bot_state WHERE state_key = %s",
            ('session_active_flag',),
            fetch_one=True
        )
        if not result or not result.get('state_value'):
            return None
        return json.loads(result['state_value'])
    except Exception as e:
        log_error(f"Loading session_active flag: {e}")
        return None


def verify_and_sync_step_from_db():
    try:
        query = '''
            SELECT pnl, side, entry_time, exit_time, quantity
            FROM closed_positions
            WHERE symbol = %s AND exit_time IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
        '''
        last_trade = execute_mysql_query(query, (BOT_STATE['symbol'],), fetch_one=True)

        if not last_trade:
            print("ℹ️ [DB SYNC] No trades in DB yet - using current step")
            return False

        db_pnl    = float(last_trade['pnl']) if last_trade['pnl'] is not None else 0.0
        db_result = 'PROFIT' if db_pnl > 0 else 'LOSS'

        print(f"\n🔍 [DB SYNC] Last trade from DB:")
        print(f"   PnL: {db_pnl:.5f} → Result: {db_result}")
        print(f"   Memory Step BEFORE sync: {BOT_STATE['current_step']} | Lot: {BOT_STATE['current_lot']}")

        memory_pnl = LAST_TRADE_RESULT.get('profit_loss')

        if memory_pnl is not None and abs(memory_pnl - db_pnl) < 0.001:
            print(f"   ✅ Memory matches DB - no correction needed")
            return True

        print(f"   ⚠️ DB PnL ({db_pnl:.5f}) differs from memory ({memory_pnl}) - CORRECTING STEP")

        LAST_TRADE_RESULT['profit_loss'] = db_pnl
        LAST_TRADE_RESULT['processed']   = True
        BOT_STATE['last_result']         = db_result

        saved_state = execute_mysql_query(
            "SELECT state_value FROM bot_state WHERE state_key = %s",
            ('martingale_state',),
            fetch_one=True
        )

        if saved_state and saved_state.get('state_value'):
            state_data    = json.loads(saved_state['state_value'])
            correct_step  = int(state_data.get('current_step', 1))
            correct_lot   = float(state_data.get('current_lot', LOT_STEPS[1]))
            print(f"   ✅ Corrected from DB state: Step={correct_step}, Lot={correct_lot}")
        else:
            if db_pnl > 0:
                correct_step = 1
                correct_lot  = LOT_STEPS[1]
            else:
                current = BOT_STATE['current_step']
                correct_step = min(current + 1, BOT_STATE['max_steps'])
                correct_lot  = LOT_STEPS[correct_step]
            print(f"   ✅ Derived step from PnL: Step={correct_step}, Lot={correct_lot}")

        BOT_STATE['current_step'] = correct_step
        BOT_STATE['current_lot']  = correct_lot

        print(f"   ✅ [DB SYNC] Step corrected → Step {correct_step}: Lot {correct_lot}")
        return True

    except Exception as e:
        print(f"❌ [DB SYNC] Error: {e}")
        import traceback
        traceback.print_exc()
        return False


# =====================================================================
# INDEPENDENT PRE-ORDER FILL VERIFICATION
# =====================================================================
# Purpose:
#   Before placing ANY new order, this function:
#   1. Waits 15 seconds for fills to propagate (called after position close)
#   2. Fetches the last 5 fills from official API
#   3. Finds the last valid entry+exit fill PAIR for this symbol
#   4. Validates: exit_time >= entry_time, opposite sides, correct symbol
#   5. Saves the verified pair to recent_fills_cache table (keeps last 2)
#   6. Extracts real lot_size from the fill pair
#   7. Determines the correct NEXT step/lot from real last trade result
#   8. Corrects BOT_STATE['current_step'] and BOT_STATE['current_lot'] in memory
#   9. Updates bot_state DB with corrected values
#
# This function is COMPLETELY INDEPENDENT of:
#   - position closure detection logic
#   - WS fill queue
#   - LAST_POSITION_STATE
#   - BOT_STATE['last_result']
#   - Any other in-memory state
#
# It ONLY trusts: official fills API + DB recent_fills_cache
# =====================================================================

def save_fill_pair_to_cache(symbol, entry_order_id, exit_order_id,
                             entry_side, entry_price, exit_price,
                             lot_size, pnl, entry_time, exit_time):
    """
    Save a verified entry+exit fill pair to recent_fills_cache.
    Keeps only the last 2 pairs per symbol (older ones deleted).
    """
    try:
        insert_query = '''
            INSERT IGNORE INTO recent_fills_cache
            (symbol, entry_order_id, exit_order_id, entry_side,
             entry_price, exit_price, lot_size, pnl, entry_time, exit_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        execute_mysql_query(
            insert_query,
            (symbol, str(entry_order_id), str(exit_order_id),
             entry_side, entry_price, exit_price,
             lot_size, pnl, entry_time, exit_time),
            commit=True
        )

        # Keep only last 2 rows per symbol
        cleanup_query = '''
            DELETE FROM recent_fills_cache
            WHERE symbol = %s AND id NOT IN (
                SELECT id FROM (
                    SELECT id FROM recent_fills_cache
                    WHERE symbol = %s
                    ORDER BY saved_at DESC
                    LIMIT 2
                ) AS keep_ids
            )
        '''
        execute_mysql_query(cleanup_query, (symbol, symbol), commit=True)

        log_system(f"[FILL CACHE] Saved pair: entry={entry_order_id} exit={exit_order_id} lot={lot_size} pnl={pnl:.5f}")
    except Exception as e:
        log_error(f"[FILL CACHE] Error saving pair: {e}")


# >>> ADDED: saves ONE row per placed order into signal_scores_history.
# INFO ONLY — called AFTER an order is already placed successfully. It never
# influences whether/what order gets placed; it only records what the signal
# engine showed at that moment for later review in the UI.
def save_signal_score_history(order_id, symbol, position, lot_size,
                               timeframe_scores, final_decision, entry_time):
    try:
        insert_query = '''
            INSERT IGNORE INTO signal_scores_history
            (order_id, symbol, position, lot_size, tf_1m, tf_5m, tf_15m, tf_30m,
             tf_1h, tf_2h, tf_4h, tf_1d, tf_1w, final_decision, entry_time)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        '''
        inserted_id = execute_mysql_query(
            insert_query,
            (
                str(order_id), symbol, position, lot_size,
                timeframe_scores.get('1m', '—'),
                timeframe_scores.get('5m', '—'),
                timeframe_scores.get('15m', '—'),
                timeframe_scores.get('30m', '—'),
                timeframe_scores.get('1h', '—'),
                timeframe_scores.get('2h', '—'),
                timeframe_scores.get('4h', '—'),
                timeframe_scores.get('1d', '—'),
                timeframe_scores.get('1w', '—'),
                final_decision, entry_time
            ),
            commit=True
        )

        log_system(f"[SIGNAL SCORES] Saved snapshot for order {order_id}")
    except Exception as e:
        log_error(f"[SIGNAL SCORES] Error saving snapshot for order {order_id}: {e}")


def get_cached_fill_pairs(symbol, limit=2):
    """
    Get the last N verified fill pairs from recent_fills_cache for a symbol.
    Returns list of dicts, newest first.
    """
    try:
        query = '''
            SELECT * FROM recent_fills_cache
            WHERE symbol = %s
            ORDER BY saved_at DESC
            LIMIT %s
        '''
        result = execute_mysql_query(query, (symbol, limit), fetch_all=True)
        return result if result else []
    except Exception as e:
        log_error(f"[FILL CACHE] Error fetching cached pairs: {e}")
        return []


def pre_order_fill_verification(symbol, wait_seconds=15):
    """
    ══════════════════════════════════════════════════════════════════
    INDEPENDENT PRE-ORDER FILL VERIFICATION
    ══════════════════════════════════════════════════════════════════
 
    Call this BEFORE placing every new order.
    It is completely independent — ignores all in-memory state.
 
    Flow:
      1. Wait wait_seconds for fills to propagate from exchange
      2. Fetch last 5 fills from official API
      3. Find latest valid entry+exit pair for this symbol
         Rule: exit_time >= entry_time, opposite sides, same symbol
      4. Check if this pair is already in recent_fills_cache
         (avoid double processing)
      5. If new pair found:
         a. Save to recent_fills_cache
         b. Compute PnL from real fills
         c. Detect last_step from ACTUAL lot_size in fills (not BOT_STATE)
         d. Determine next step: WIN→Step1, LOSS→last_step+1
         e. Update BOT_STATE['current_step'] and ['current_lot']
         f. Save to bot_state DB
      6. If no new pair found:
         a. Read last cached pair from recent_fills_cache
         b. Use its lot_size to derive correct next step independently
      7. Log everything for debugging
 
    KEY DIFFERENCE FROM OLD VERSION:
      - NEVER uses BOT_STATE['current_step'] to calculate next step
      - ALWAYS derives last_step from actual lot_size in fills/cache
      - This prevents double-increment bug where _apply_step_progression()
        already incremented step, and verify incremented it again
      - On FRESH START (no trades this session yet), skips fill-based
        override entirely and trusts Step 1 from bot startup
 
    Returns:
      dict with keys:
        'verified': bool - whether verification succeeded
        'lot_used': float - lot size of last real trade (0 if unknown)
        'pnl': float - PnL of last real trade (0 if unknown)
        'next_step': int - correct next step to use
        'next_lot': float - correct next lot to place
        'source': str - 'fresh_fills' | 'cache' | 'fresh_start' | 'fallback'
        'pair_found': bool - whether a valid entry+exit pair was found
    """
    log_system(f"[PRE-ORDER VERIFY] Starting verification for {symbol}")
    log_system(f"[PRE-ORDER VERIFY] Waiting {wait_seconds}s for fills to propagate...")
 
    time.sleep(wait_seconds)
 
    result = {
        'verified': False,
        'lot_used': 0.0,
        'pnl': 0.0,
        'next_step': BOT_STATE['current_step'],
        'next_lot': BOT_STATE['current_lot'],
        'source': 'fallback',
        'pair_found': False
    }
 
    # ══════════════════════════════════════════════════════════════════
    # FRESH START GUARD
    # If this is a brand new session (no trades closed yet this session),
    # skip all fill-based overrides. The bot already set Step=1 on startup.
    # We do NOT want old fills from a previous session to change the step.
    # LAST_CLOSE_TIMESTAMP == 0.0 means no position has closed this session.
    # ══════════════════════════════════════════════════════════════════
    if LAST_CLOSE_TIMESTAMP == 0.0:
        log_system("[PRE-ORDER VERIFY] Fresh session detected (no closes this session) — skipping fill-based override, using Step 1")
        result['next_step'] = BOT_STATE['current_step']
        result['next_lot']  = BOT_STATE['current_lot']
        result['source']    = 'fresh_start'
        result['verified']  = True
        return result
 
    # ── HELPER: derive next step purely from lot_size (no BOT_STATE dependency) ──
    def get_next_step_from_lot(lot_size, pnl, be_retry=False):
        """
        Given the actual lot_size used in the last trade and its PnL,
        return the correct (next_step, next_lot) independently.

        Steps:
          - Find which step corresponds to lot_size
          - If BE SL retry → same step/lot (no win/loss progression)
          - If WIN  → next_step = 1
          - If LOSS → next_step = last_step + 1 (wraps to 1 at max)
        """
        # Find the step that matches this lot size exactly
        last_step = None
        for step, lot in LOT_STEPS.items():
            if lot == lot_size:
                last_step = step
                break
 
        # If no exact match, find nearest step (safety fallback)
        if last_step is None:
            closest_step = 1
            closest_diff = float('inf')
            for step, lot in LOT_STEPS.items():
                diff = abs(lot - lot_size)
                if diff < closest_diff:
                    closest_diff = diff
                    closest_step = step
            last_step = closest_step
            log_system(f"[PRE-ORDER VERIFY] No exact lot match for {lot_size} — closest step={last_step}")

        if be_retry and pnl > 0:
            log_system(
                f"[PRE-ORDER VERIFY] BE SL retry suppressed — last trade PROFIT (TP), not BE SL"
            )
            be_retry = False

        if be_retry:
            next_step = last_step
            next_lot  = LOT_STEPS[next_step]
            log_system(
                f"[PRE-ORDER VERIFY] BE SL retry (lot={lot_size} step={last_step}) "
                f"→ Same Step={next_step} Lot={next_lot}"
            )
            return next_step, next_lot

        if pnl > 0:
            # WIN → reset to Step 1
            next_step = 1
            next_lot  = LOT_STEPS[next_step]
            log_system(f"[PRE-ORDER VERIFY] WIN (lot={lot_size} was step={last_step}) → Next Step={next_step} Lot={next_lot}")
        else:
            # LOSS → advance to next step based on ACTUAL last step from fills
            next_step = last_step + 1
            if next_step > BOT_STATE['max_steps']:
                next_step = 1
            next_lot = LOT_STEPS[next_step]
            log_system(f"[PRE-ORDER VERIFY] LOSS (lot={lot_size} was step={last_step}) → Next Step={next_step} Lot={next_lot}")
 
        return next_step, next_lot
 
    # ── STEP 1: Fetch official fills ─────────────────────────────────
    try:
        fills_response = make_api_request('GET', '/fills?page_size=5')
        if not fills_response or not fills_response.get('result'):
            log_error("[PRE-ORDER VERIFY] Could not fetch fills from API")
            fills = []
        else:
            fills = fills_response.get('result', [])
    except Exception as e:
        log_error(f"[PRE-ORDER VERIFY] Exception fetching fills: {e}")
        fills = []
 
    # ── STEP 2: Filter by symbol ──────────────────────────────────────
    symbol_fills = [f for f in fills if f.get('product_symbol') == symbol]
 
    log_system(f"[PRE-ORDER VERIFY] Total fills fetched: {len(fills)} | Symbol fills: {len(symbol_fills)}")
 
    if not symbol_fills:
        log_system("[PRE-ORDER VERIFY] No fills for symbol — checking cache")
    else:
        # ── STEP 3: Group fills by order_id ──────────────────────────
        order_groups = {}
        for fill in symbol_fills:
            oid       = str(fill.get('order_id') or fill.get('id', ''))
            side      = fill.get('side', '').lower()
            size      = float(fill.get('size', 0))
            price     = float(fill.get('price', 0))
            created   = fill.get('created_at', '')
 
            if not oid or size <= 0 or price <= 0:
                continue
 
            if oid not in order_groups:
                order_groups[oid] = {
                    'order_id':    oid,
                    'side':        side,
                    'total_size':  0.0,
                    'total_value': 0.0,
                    'avg_price':   0.0,
                    'timestamp':   created,
                    'fills_count': 0
                }
 
            grp = order_groups[oid]
            grp['total_size']  += size
            grp['total_value'] += price * size
            grp['fills_count'] += 1
            # Keep earliest timestamp as order time
            if created < grp['timestamp']:
                grp['timestamp'] = created
 
        for grp in order_groups.values():
            if grp['total_size'] > 0:
                grp['avg_price'] = grp['total_value'] / grp['total_size']
 
        # Sort by timestamp ascending (oldest first)
        sorted_orders = sorted(order_groups.values(), key=lambda x: x['timestamp'])
 
        log_system(f"[PRE-ORDER VERIFY] Unique orders in fills: {len(sorted_orders)}")
        for o in sorted_orders:
            log_system(f"  → OrderID={o['order_id']} side={o['side']} size={o['total_size']} price={o['avg_price']:.4f} time={o['timestamp']}")
 
        # ── STEP 4: Find latest valid entry+exit pair ─────────────────
        # Rule: exit_time >= entry_time, opposite sides
        # Search from newest to oldest for exit, pair with nearest older entry
        found_pair = None
 
        for i in range(len(sorted_orders) - 1, 0, -1):
            potential_exit  = sorted_orders[i]
            potential_entry = sorted_orders[i - 1]
 
            # Must be opposite sides
            if potential_exit['side'] == potential_entry['side']:
                continue
 
            # exit_time must be >= entry_time (cannot be older)
            if potential_exit['timestamp'] < potential_entry['timestamp']:
                continue
 
            found_pair = (potential_entry, potential_exit)
            log_system(f"[PRE-ORDER VERIFY] Pair found: entry={potential_entry['order_id']} ({potential_entry['side']}) exit={potential_exit['order_id']} ({potential_exit['side']})")
            break
 
        # ── STEP 5: Process found pair ────────────────────────────────
        if found_pair:
            entry_order = found_pair[0]
            exit_order  = found_pair[1]
 
            entry_oid   = entry_order['order_id']
            exit_oid    = exit_order['order_id']
 
            # Check if this pair is already in cache (avoid reprocessing)
            cached_pairs = get_cached_fill_pairs(symbol, limit=2)
            already_cached = any(
                str(cp.get('entry_order_id')) == entry_oid and
                str(cp.get('exit_order_id'))  == exit_oid
                for cp in cached_pairs
            )
 
            entry_side  = entry_order['side']
            entry_price = entry_order['avg_price']
            exit_price  = exit_order['avg_price']
            # lot_size = actual number of contracts traded (e.g. 1, 2, 4, 8, 16, 32)
            lot_size    = min(entry_order['total_size'], exit_order['total_size'])
 
            lot_multiplier  = LOT_SIZES.get(symbol, LOT_SIZE_DEFAULT)
            actual_quantity = lot_size * lot_multiplier
 
            if entry_side == 'buy':
                pnl = (exit_price - (entry_price + fees)) * actual_quantity 
            else:
                pnl = ((entry_price - fees) - exit_price) * actual_quantity 
 
            result['lot_used']   = lot_size
            result['pnl']        = pnl
            result['pair_found'] = True
 
            log_system(f"[PRE-ORDER VERIFY] Last trade: {entry_side.upper()} lot={lot_size} entry={entry_price:.4f} exit={exit_price:.4f} pnl={pnl:.5f} result={'PROFIT' if pnl > 0 else 'LOSS'}")
            log_system(f"[PRE-ORDER VERIFY] Already cached: {already_cached}")
 
            if not already_cached:
                # Save to cache
                save_fill_pair_to_cache(
                    symbol=symbol,
                    entry_order_id=entry_oid,
                    exit_order_id=exit_oid,
                    entry_side=entry_side,
                    entry_price=entry_price,
                    exit_price=exit_price,
                    lot_size=lot_size,
                    pnl=pnl,
                    entry_time=entry_order['timestamp'],
                    exit_time=exit_order['timestamp']
                )
 
                # ── KEY FIX: Derive next step from ACTUAL lot_size in fills ──
                # NEVER use BOT_STATE['current_step'] here — it may already be
                # incremented by _apply_step_progression() causing double-increment
                be_retry = _pop_be_retry_pending(symbol)
                next_step, next_lot = get_next_step_from_lot(lot_size, pnl, be_retry=be_retry)
 
                # Correct BOT_STATE in memory
                old_step = BOT_STATE['current_step']
                old_lot  = BOT_STATE['current_lot']
                BOT_STATE['current_step'] = next_step
                BOT_STATE['current_lot']  = next_lot
                BOT_STATE['last_result']  = 'PROFIT' if pnl > 0 else 'LOSS'
 
                log_system(f"[PRE-ORDER VERIFY] BOT_STATE corrected: Step {old_step}→{next_step} | Lot {old_lot}→{next_lot}")
 
                # Save corrected state to DB
                save_bot_state_to_db()
 
                result['source'] = 'fresh_fills'
            else:
                # ── Pair already cached ───────────────────────────────
                # Still re-derive from lot_size independently — don't trust BOT_STATE
                be_retry = _pop_be_retry_pending(symbol)
                next_step, next_lot = get_next_step_from_lot(lot_size, pnl, be_retry=be_retry)
 
                old_step = BOT_STATE['current_step']
                old_lot  = BOT_STATE['current_lot']
 
                if old_step != next_step or old_lot != next_lot:
                    BOT_STATE['current_step'] = next_step
                    BOT_STATE['current_lot']  = next_lot
                    BOT_STATE['last_result']  = 'PROFIT' if pnl > 0 else 'LOSS'
                    log_system(f"[PRE-ORDER VERIFY] Cached pair — BOT_STATE re-aligned: Step {old_step}→{next_step} | Lot {old_lot}→{next_lot}")
                    save_bot_state_to_db()
                else:
                    log_system(f"[PRE-ORDER VERIFY] Cached pair — BOT_STATE already correct: Step={old_step} Lot={old_lot}")
 
                result['source'] = 'cache'
 
            result['next_step'] = BOT_STATE['current_step']
            result['next_lot']  = BOT_STATE['current_lot']
            result['verified']  = True
            return result
 
    # ── STEP 6: No fresh pair found — check cache ─────────────────────
    log_system("[PRE-ORDER VERIFY] No fresh pair in API fills — checking recent_fills_cache")
 
    cached_pairs = get_cached_fill_pairs(symbol, limit=2)
 
    if cached_pairs:
        last_cached = cached_pairs[0]  # Most recent
        cached_lot  = float(last_cached.get('lot_size', 0))
        cached_pnl  = float(last_cached.get('pnl', 0))
 
        log_system(f"[PRE-ORDER VERIFY] Cache hit: entry={last_cached.get('entry_order_id')} lot={cached_lot} pnl={cached_pnl:.5f}")
 
        result['lot_used']   = cached_lot
        result['pnl']        = cached_pnl
        result['pair_found'] = True
        result['source']     = 'cache'
 
        # ── KEY FIX: Derive next step from cached lot_size independently ──
        # Again: NEVER rely on BOT_STATE['current_step'] here
        be_retry = _pop_be_retry_pending(symbol)
        next_step, next_lot = get_next_step_from_lot(cached_lot, cached_pnl, be_retry=be_retry)
 
        old_step = BOT_STATE['current_step']
        old_lot  = BOT_STATE['current_lot']
 
        if old_step != next_step or old_lot != next_lot:
            BOT_STATE['current_step'] = next_step
            BOT_STATE['current_lot']  = next_lot
            BOT_STATE['last_result']  = 'PROFIT' if cached_pnl > 0 else 'LOSS'
            log_system(f"[PRE-ORDER VERIFY] Cache-only — BOT_STATE corrected: Step {old_step}→{next_step} | Lot {old_lot}→{next_lot}")
            save_bot_state_to_db()
        else:
            log_system(f"[PRE-ORDER VERIFY] Cache-only — BOT_STATE already correct: Step={old_step} Lot={old_lot}")
 
        result['next_step'] = BOT_STATE['current_step']
        result['next_lot']  = BOT_STATE['current_lot']
        result['verified']  = True
        return result
 
    # ── STEP 7: No fills, no cache — pure fallback ────────────────────
    log_system("[PRE-ORDER VERIFY] No fills and no cache — using current BOT_STATE as fallback")
    result['next_step'] = BOT_STATE['current_step']
    result['next_lot']  = BOT_STATE['current_lot']
    result['source']    = 'fallback'
    result['verified']  = True  # Still return True so bot doesn't block forever
    return result


# ========== SIGNAL / STRATEGY BOTS ==========
# Decision logic lives in bot1.py / bot2.py / bot3.py / bot4.py
# Only the bot selected in the UI is loaded into RAM (see bot_manager.py).

def generate_smart_signal(reason="trade_decision"):
    """Delegate to the active strategy bot (lazy-loaded)."""
    return bot_manager.generate_smart_signal(reason=reason)


def save_closed_position(trade_data):
    global LAST_SAVED_TRADE_KEY
    try:
        with db_lock:
            trade_key = (
                trade_data['symbol'],
                trade_data['side'],
                trade_data['entry_time']
            )

            if LAST_SAVED_TRADE_KEY == trade_key:
                log_trade(f"SAVE SKIPPED | Duplicate trade_key in memory: {trade_key}")
                return

            entry_order_id = trade_data.get('entry_order_id')
            if entry_order_id:
                order_id_check = execute_mysql_query(
                    "SELECT id FROM closed_positions WHERE entry_order_id = %s LIMIT 1",
                    (str(entry_order_id),),
                    fetch_one=True
                )
                if order_id_check:
                    log_trade(f"SAVE SKIPPED | entry_order_id={entry_order_id} already in DB")
                    LAST_SAVED_TRADE_KEY = trade_key
                    return

            duplicate_query = """
                SELECT id FROM closed_positions
                WHERE symbol=%s AND entry_time=%s
                LIMIT 1
            """
            existing_trade = execute_mysql_query(
                duplicate_query,
                (trade_data['symbol'], trade_data['entry_time']),
                fetch_one=True
            )

            if existing_trade:
                log_trade(f"SAVE SKIPPED | Duplicate (symbol, entry_time) already in DB")
                LAST_SAVED_TRADE_KEY = trade_key
                return

            cleanup_old_trades(target_size_mb=8.5)

            # Retrieve decisions
            trade_decisions = None
            if entry_order_id:
                trade_decisions = ACTIVE_TRADE_DECISIONS.get(str(entry_order_id))

            if not trade_decisions:
                trade_decisions = "Position already open before bot started"

            execute_mysql_query(
                "UPDATE closed_positions SET is_latest = 0 WHERE symbol = %s",
                (trade_data['symbol'],),
                commit=True
            )

            # >>> ADDED: exit_order_id captured alongside entry_order_id
            exit_order_id = trade_data.get('exit_order_id')

            # BE flag: explicit on trade_data, else consume in-memory mark from guardian
            had_be = bool(trade_data.get('had_break_even'))
            if not had_be and entry_order_id:
                had_be = _consume_order_had_be(str(entry_order_id))

            insert_query = '''
                INSERT INTO closed_positions
                (symbol, side, entry_price, exit_price, quantity, pnl,
                 entry_time, exit_time, is_latest, entry_order_id, exit_order_id,
                 trade_decisions, had_break_even)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,%s,%s,%s,%s)
            '''
            inserted_id = execute_mysql_query(
                insert_query,
                (
                    trade_data['symbol'],
                    trade_data['side'],
                    trade_data['entry_price'],
                    trade_data['exit_price'],
                    trade_data['quantity'],
                    trade_data['pnl'],
                    trade_data['entry_time'],
                    trade_data['exit_time'],
                    str(entry_order_id) if entry_order_id else None,
                    str(exit_order_id) if exit_order_id else None,
                    trade_decisions,
                    1 if had_be else 0,
                ),
                commit=True
            )

            if entry_order_id and str(entry_order_id) in ACTIVE_TRADE_DECISIONS:
                del ACTIVE_TRADE_DECISIONS[str(entry_order_id)]

            LAST_SAVED_TRADE_KEY = trade_key
            log_trade(f"TRADE SAVED | DB write completed | entry_order_id={entry_order_id}")

    except pymysql.err.IntegrityError as e:
        log_trade(f"SAVE SKIPPED | DB IntegrityError (duplicate blocked at DB level): {e}")
        LAST_SAVED_TRADE_KEY = trade_key
    except Exception as e:
        log_error(f"Error saving trade to MySQL: {e}")


# ========== OPTIMIZED POSITION TRACKING ==========
def get_product_id(symbol):
    global PRODUCT_ID_CACHE
    if symbol in PRODUCT_ID_CACHE:
        return PRODUCT_ID_CACHE[symbol]
    # Prefer hardcoded map (fast + correct for current BASE_URL env)
    if symbol in PRODUCT_IDS:
        PRODUCT_ID_CACHE[symbol] = PRODUCT_IDS[symbol]
        return PRODUCT_IDS[symbol]
    try:
        products = make_api_request('GET', '/products')
        if not products or not products.get('result'):
            return None
        for product in products.get('result', []):
            if product.get('symbol') == symbol:
                product_id = product.get('id')
                PRODUCT_ID_CACHE[symbol] = product_id
                return product_id
        return None
    except Exception as e:
        log_error(f"Error getting product_id: {e}")
        return None


def check_position_realtime(product_id, expected_symbol=None):
    """
    NOTE (FIX): added optional expected_symbol param.
    Kabhi-kabhi exchange API '/positions?product_id=X' query param ignore
    karke saari positions ya kisi aur symbol ki position return kar sakti
    hai. Pehle code isse blindly trust kar leta tha, jisse ETH bot ko BTC
    ki position "apni" lagne lagti thi (ya ulta). Ab agar response me
    product_id/symbol field mile aur wo requested product_id se match na
    kare, to us result ko IGNORE kar diya jaata hai (no position maana
    jaata hai) - taaki galat symbol ki position kabhi bhi bot ke apne
    symbol ke logic me mix na ho. Baaki sab kuch same hai.
    """
    try:
        response = make_api_request('GET', f'/positions/margined?product_id={product_id}')
        if not response or not response.get('success'):
            print("⚠️ API FAILED - Returning error state")
            return {'error': True}

        response = make_api_request('GET', f'/positions?product_id={product_id}')
        if response and response.get('success') and response.get('result'):
            result = response['result']

            # FIX: agar API ne list bhej di (kuch endpoints aisा karte hain),
            # to sirf requested product_id wali entry uthao - baaki ignore.
            if isinstance(result, list):
                match = None
                for r in result:
                    if str(r.get('product_id')) == str(product_id):
                        match = r
                        break
                if match is None:
                    return {'has_position': False, 'size': 0, 'entry_price': 0}
                result = match

            # FIX: agar returned object ka product_id requested se mismatch
            # kare, to isse "apni" position mat maano.
            returned_pid = result.get('product_id')
            if returned_pid is not None and str(returned_pid) != str(product_id):
                log_error(f"[POSITION MISMATCH] Requested product_id={product_id} but API returned product_id={returned_pid} — ignoring as safety guard")
                return {'has_position': False, 'size': 0, 'entry_price': 0}

            # FIX (extra safety): agar symbol bhi diya gaya hai aur field available hai to match karo
            returned_symbol = result.get('product_symbol') or result.get('symbol')
            if expected_symbol and returned_symbol and returned_symbol != expected_symbol:
                log_error(f"[POSITION MISMATCH] Expected symbol={expected_symbol} but API returned symbol={returned_symbol} — ignoring as safety guard")
                return {'has_position': False, 'size': 0, 'entry_price': 0}

            size        = float(result.get('size', 0))
            entry_price = float(result.get('entry_price', 0)) if abs(size) > 0.001 else 0
            return {
                'has_position': abs(size) > 0.001,
                'size': size,
                'entry_price': entry_price
            }
        return {'has_position': False, 'size': 0, 'entry_price': 0}
    except Exception as e:
        print(f"❌ Error checking position: {e}")
        return {'has_position': False, 'size': 0, 'entry_price': 0}


# ========== API FUNCTIONS ==========
def get_server_time():
    try:
        response = requests.get(f"{BASE_URL}/v2/time", timeout=5)
        if response.status_code == 200:
            return str(int(response.json()['result']))
        else:
            return str(int(time.time()))
    except:
        return str(int(time.time()))


def sign_request(method, path, body=""):
    ts      = get_server_time()
    payload = method + ts + path + body
    signature = hmac.new(
        DELTA_API_SECRET.encode(),
        payload.encode(),
        hashlib.sha256
    ).hexdigest()
    return {
        "api-key": DELTA_API_KEY,
        "timestamp": ts,
        "signature": signature,
        "Content-Type": "application/json"
    }


def safe_float(value, fallback=0.0):
    try:
        if value is None or value == "":
            return fallback
        f = float(value)
        if not isfinite(f):
            return fallback
        return f
    except:
        return fallback


def make_api_request(method, endpoint, data=None):
    path = f"/v2{endpoint}"
    body = json.dumps(data) if data else ""
    headers = sign_request(method, path, body)
    url = f"{BASE_URL}{path}"
    try:
        if method == 'GET':
            response = requests.get(url, headers=headers, timeout=10)
        elif method == 'POST':
            response = requests.post(url, headers=headers, data=body, timeout=10)
        elif method == 'DELETE':
            response = requests.delete(url, headers=headers, data=body, timeout=10)

        if response.status_code == 200:
            return response.json()
        else:
            try:
                err_body = response.json()
            except Exception:
                err_body = {'success': False, 'error': {'code': 'http_error', 'message': str(response.text)}}
            log_error(f"API HTTP {response.status_code} | {method} {endpoint} | {err_body}")
            # Return body so callers can read error.code (e.g. invalid_contract)
            if isinstance(err_body, dict):
                err_body.setdefault('success', False)
                return err_body
            return None
    except requests.exceptions.Timeout:
        log_error(f"API TIMEOUT | {method} {endpoint}")
        return None
    except requests.exceptions.ConnectionError as e:
        log_error(f"API CONNECTION ERROR | {method} {endpoint} | {e}")
        return None
    except Exception as e:
        log_error(f"API EXCEPTION | {method} {endpoint} | {e}")
        return None


def place_order(symbol, side, quantity, order_type='market_order'):
    order_data = {
        'product_symbol': symbol,
        'side': side,
        'order_type': order_type,
        'size': quantity
    }
    print(f"📋 Order Data: {order_data}")
    return make_api_request('POST', '/orders', order_data)


def set_leverage(symbol, leverage):
    products = make_api_request('GET', '/products')
    if not products or not products.get('result'):
        return None
    product_id = None
    for product in products.get('result', []):
        if product.get('symbol') == symbol:
            product_id = product.get('id')
            break
    if not product_id:
        return None
    return make_api_request('POST', f'/products/{product_id}/orders/leverage', {'leverage': str(leverage)})


def get_wallet_balance():
    try:
        response = make_api_request('GET', '/wallet/balances')
        if response and response.get("success") and response.get("result"):
            balances = response["result"]
            if not isinstance(balances, list):
                balances = [balances] if isinstance(balances, dict) else []

            wallet_balance    = 0.0
            available_balance = 0.0
            asset_symbol      = "USD"

            for balance in balances:
                if not isinstance(balance, dict):
                    continue
                asset = (balance.get("asset_symbol") or "").upper()
                if asset in ("USD", "USDT", "USDC"):
                    wallet_balance    = safe_float(balance.get("balance"), 0)
                    available_balance = safe_float(balance.get("available_balance"), 0)
                    asset_symbol      = asset
                    break

            if wallet_balance == 0:
                for balance in balances:
                    if not isinstance(balance, dict):
                        continue
                    bal_val = safe_float(balance.get("balance"), 0)
                    if bal_val > 0:
                        wallet_balance    = bal_val
                        available_balance = safe_float(balance.get("available_balance"), 0)
                        asset_symbol      = (balance.get("asset_symbol") or "USD")
                        break

            margin_used = wallet_balance - available_balance
            return {
                'success': True,
                'balance': wallet_balance,
                'available_balance': available_balance,
                'margin_used': margin_used,
                'currency': asset_symbol
            }

        return {
            'success': True,
            'balance': 10000.0,
            'available_balance': 8500.0,
            'margin_used': 1500.0,
            'currency': 'USDT'
        }
    except Exception as e:
        print(f"Wallet balance error: {e}")
        return {
            'success': True,
            'balance': 10000.0,
            'available_balance': 8500.0,
            'margin_used': 1500.0,
            'currency': 'USDT'
        }


# ========== OPTIMIZED FILL RETRIEVAL ENGINE ==========
def get_fills_page(page_size=5):
    try:
        safe_page_size = min(page_size, 5)
        fills = make_api_request('GET', f'/fills?page_size={safe_page_size}')
        if not fills or not fills.get('result'):
            return []
        return fills.get('result', [])
    except Exception as e:
        log_error(f"Error fetching fills: {e}")
        return []


def find_trade_by_order_id(symbol, target_order_id):
    if not target_order_id:
        log_error("find_trade_by_order_id called with no target_order_id")
        return 0, None

    with PROCESSED_ORDER_IDS_LOCK:
        if str(target_order_id) in PROCESSED_ORDER_IDS:
            log_trade(f"DUPLICATE BLOCKED | order {target_order_id} already in PROCESSED_ORDER_IDS")
            return 0, None

    fills = get_fills_page(page_size=5)
    if not fills:
        return 0, None

    symbol_fills = [f for f in fills if f.get('product_symbol') == symbol]
    if not symbol_fills:
        return 0, None

    symbol_fills_sorted = sorted(symbol_fills, key=lambda x: x.get('created_at', ''))

    order_groups = {}
    for fill in symbol_fills_sorted:
        order_id   = str(fill.get('order_id') or fill.get('id', ''))
        side       = fill.get('side', '')
        size       = float(fill.get('size', 0))
        price      = float(fill.get('price', 0))
        created_at = fill.get('created_at', '')

        if not order_id or size <= 0 or price <= 0:
            continue

        if order_id not in order_groups:
            order_groups[order_id] = {
                'order_id':    order_id,
                'side':        side,
                'total_size':  0.0,
                'total_value': 0.0,
                'avg_price':   0.0,
                'timestamp':   created_at,
                'fills_count': 0
            }

        grp = order_groups[order_id]
        grp['total_size']  += size
        grp['total_value'] += price * size
        grp['fills_count'] += 1
        if created_at < grp['timestamp']:
            grp['timestamp'] = created_at

    for grp in order_groups.values():
        if grp['total_size'] > 0:
            grp['avg_price'] = grp['total_value'] / grp['total_size']

    entry_order = order_groups.get(str(target_order_id))
    if not entry_order:
        return 0, None

    with USED_FILL_IDS_LOCK:
        already_used = set(USED_FILL_IDS)

    exit_order = None
    for oid, grp in order_groups.items():
        if oid == str(target_order_id):
            continue
        if oid in already_used:
            continue
        if grp['side'] != entry_order['side']:
            if grp['timestamp'] >= entry_order['timestamp']:
                exit_order = grp
                break

    if not exit_order:
        return 0, None

    entry_side  = entry_order['side']
    entry_price = entry_order['avg_price']
    exit_price  = exit_order['avg_price']
    trade_size  = min(entry_order['total_size'], exit_order['total_size'])

    lot_size        = LOT_SIZES.get(symbol, LOT_SIZE_DEFAULT)
    actual_quantity = trade_size * lot_size

    if entry_side == 'buy':
        pnl = (exit_price - (entry_price + fees)) * actual_quantity 
    else:
        pnl = ((entry_price - fees ) - exit_price) * actual_quantity 

    entry_exit_data = {
        'side':           entry_side,
        'entry_price':    entry_price,
        'exit_price':     exit_price,
        'quantity':       trade_size,
        'entry_time':     entry_order['timestamp'],
        'exit_time':      exit_order['timestamp'],
        'entry_order_id': str(target_order_id),
        'exit_order_id':  exit_order['order_id']
    }

    log_trade(f"PNL CALCULATED | {entry_side.upper()} | Entry={entry_price:.4f} | Exit={exit_price:.4f} | PnL={pnl:.5f} | Result={'PROFIT' if pnl > 0 else 'LOSS'}")
    log_trade(f"PAIR FOUND FOR ORDER: {target_order_id}")

    with PROCESSED_ORDER_IDS_LOCK:
        PROCESSED_ORDER_IDS.add(str(target_order_id))
        _trim_id_set(PROCESSED_ORDER_IDS)

    with USED_FILL_IDS_LOCK:
        USED_FILL_IDS.add(str(target_order_id))
        USED_FILL_IDS.add(exit_order['order_id'])
        _trim_id_set(USED_FILL_IDS)

    return pnl, entry_exit_data


def wait_for_trade_fills(symbol, target_order_id, max_retries=5, retry_delay=2):
    if not target_order_id:
        log_error("wait_for_trade_fills: no target_order_id set - cannot retrieve fills")
        return 0, None

    log_trade(f"TRACKING ORDER: {target_order_id}")

    for attempt in range(1, max_retries + 1):
        pnl, data = find_trade_by_order_id(symbol, target_order_id)
        if data:
            return pnl, data
        if attempt < max_retries:
            print(f"⏳ Fill not found yet (attempt {attempt}/{max_retries}) - retrying in {retry_delay}s...")
            time.sleep(retry_delay)

    log_error(f"Fill not found after {max_retries} retries for order {target_order_id}")
    return 0, None


def group_fills_by_order(fills, symbol):
    symbol_fills = [f for f in fills if f.get('product_symbol') == symbol]
    order_groups = {}
    for fill in symbol_fills:
        order_id   = str(fill.get('order_id') or fill.get('id', ''))
        side       = fill.get('side', '')
        size       = float(fill.get('size', 0))
        price      = float(fill.get('price', 0))
        created_at = fill.get('created_at', '')
        if not order_id or size <= 0 or price <= 0:
            continue
        if order_id not in order_groups:
            order_groups[order_id] = {
                'order_id':    order_id,
                'side':        side,
                'total_size':  0.0,
                'total_value': 0.0,
                'avg_price':   0.0,
                'timestamp':   created_at,
                'fills_count': 0
            }
        grp = order_groups[order_id]
        grp['total_size']  += size
        grp['total_value'] += price * size
        grp['fills_count'] += 1
        if created_at < grp['timestamp']:
            grp['timestamp'] = created_at
    for order_id, grp in order_groups.items():
        if grp['total_size'] > 0:
            grp['avg_price'] = grp['total_value'] / grp['total_size']
    sorted_orders = sorted(order_groups.values(), key=lambda x: x['timestamp'])
    return sorted_orders


def find_latest_closed_pair(symbol):
    target_order_id = BOT_STATE.get('last_placed_order_id', None)
    if not target_order_id:
        log_error("find_latest_closed_pair: no last_placed_order_id - cannot match trade")
        return 0, None
    return find_trade_by_order_id(symbol, target_order_id)


def _mark_order_complete(order_id):
    BOT_STATE['order_completed'] = True
    BOT_STATE['last_placed_order_id'] = None
    log_trade(f"ORDER COMPLETED: {order_id}")


# ========== WEBSOCKET SYMBOL SYNC (NEW) ==========
# Purpose: agar bot symbol change ho (update-symbol / naye start pe) to
# WebSocket ko FORCE karke us naye symbol pe hi (re)subscribe karwao.
# Pehle WS sirf connect hone ke waqt wale symbol pe hamesha ke liye latka
# rehta tha - agar symbol badla to WS purane symbol ki hi fills/position
# bhejta rehta, jisse cross-symbol confusion ho sakta tha.
WS_SUBSCRIBED_SYMBOL = None
WS_SYMBOL_SYNC_LOCK  = Lock()


def ensure_ws_symbol_sync(symbol):
    """
    Call this whenever the bot's active symbol is set/changed (on start-bot
    and on update-symbol). If the websocket engine is already subscribed to
    a DIFFERENT symbol, force a clean reconnect so it (re)subscribes to the
    correct symbol's user_trades/positions channels.
    """
    global WS_SUBSCRIBED_SYMBOL
    with WS_SYMBOL_SYNC_LOCK:
        if not WS_RUNNING:
            # not started yet - start_websocket_engine() will pick up the
            # current BOT_STATE['symbol'] on its own when it authenticates
            return
        if WS_SUBSCRIBED_SYMBOL is not None and WS_SUBSCRIBED_SYMBOL != symbol:
            log_system(f"[WS SYNC] Symbol changed ({WS_SUBSCRIBED_SYMBOL} → {symbol}) — restarting WS engine to resubscribe")
            stop_websocket_engine()
            time.sleep(0.5)
            start_websocket_engine()


# ========== MAIN BOT LOOP ==========
def auto_trading_bot_main():
    """
    Main bot loop.
 
    Key change: BEFORE placing each new order, call pre_order_fill_verification()
    which independently checks official fills, finds last real entry+exit pair,
    verifies lot_size, corrects BOT_STATE step/lot, and saves to DB.
 
    This ensures the correct lot is always placed regardless of any state corruption.
    """
    global LAST_CLOSE_TIMESTAMP
 
    print("🤖 Auto Trading Bot Started")

    # FIX: make sure the WS engine is listening on THIS symbol before we
    # start relying on it for fill/position detection.
    ensure_ws_symbol_sync(BOT_STATE['symbol'])
 
    print(f"⚡ Setting leverage: {BOT_STATE['leverage']}x")
    leverage_result = set_leverage(BOT_STATE['symbol'], BOT_STATE['leverage'])
    if not leverage_result:
        print("❌ Failed to set initial leverage, stopping bot")
        return
 
    print(f"\n🔍 CHECKING FOR EXISTING LIVE POSITION...")
    product_id = get_product_id(BOT_STATE['symbol'])
    if product_id:
        current_pos = check_position_realtime(product_id, expected_symbol=BOT_STATE['symbol'])
        if abs(current_pos.get('size', 0)) > 0.001:
            print(f"🚨 EXISTING POSITION FOUND: {current_pos.get('size', 0)} lots")
            print(f"📊 Entry Price: {current_pos.get('entry_price', 0)}")
 
            current_lot   = abs(current_pos.get('size', 0))
            detected_step = detect_current_step_from_lot(current_lot)
 
            BOT_STATE['current_step'] = detected_step
            BOT_STATE['current_lot']  = current_lot
            print(f"✅ Step set from live position: Step={detected_step}, Lot={current_lot}")
 
            LAST_POSITION_STATE['symbol']      = BOT_STATE['symbol']
            LAST_POSITION_STATE['size']        = current_pos.get('size', 0)
            LAST_POSITION_STATE['entry_price'] = current_pos.get('entry_price', 0)
 
            BOT_STATE['last_placed_order_id'] = 'RESUMED'
            BOT_STATE['order_completed']      = False
            log_system(f"TRACKING ORDER: RESUMED (existing position, step={detected_step}, lot={current_lot})")
 
            print("⏳ Waiting for existing position to close...")
            while BOT_STATE['running'] and abs(current_pos.get('size', 0)) > 0.001:
                time.sleep(1)
                current_pos = check_position_realtime(product_id, expected_symbol=BOT_STATE['symbol'])
                print(f"📊 Position Status: {current_pos.get('size', 0)} lots")
 
            if not BOT_STATE['running']:
                return
 
            print("✅ Existing position closed, continuing...")
 
        else:
            print("✅ No existing position found — FRESH START at Step 1")
            BOT_STATE['current_step']         = 1
            BOT_STATE['current_lot']          = LOT_STEPS[1]
            BOT_STATE['order_completed']      = True
            BOT_STATE['last_placed_order_id'] = None
            log_system("No position on startup → Step 1 (fresh start)")
 
            # ══════════════════════════════════════════════════════════
            # FRESH START: Reset LAST_CLOSE_TIMESTAMP to 0.0 so that
            # pre_order_fill_verification() knows this is a fresh session
            # and will NOT read old fills to override Step 1.
            # Also clear in-memory fill tracking sets so old order IDs
            # from previous session don't block new fill detection.
            # ══════════════════════════════════════════════════════════
            LAST_CLOSE_TIMESTAMP = 0.0
            with USED_FILL_IDS_LOCK:
                USED_FILL_IDS.clear()
            with PROCESSED_ORDER_IDS_LOCK:
                PROCESSED_ORDER_IDS.clear()
            log_system("FRESH START: LAST_CLOSE_TIMESTAMP reset, fill ID sets cleared")

    # NEW: persist that a session is actively supposed to be running, so if
    # the process restarts unexpectedly it can auto-resume (see __main__).
    save_session_active_flag(True)
 
    while BOT_STATE['running']:
        try:
            if BOT_STATE['force_stop']:
                print("🛑 Force Stop triggered!")
                BOT_STATE['running']    = False
                BOT_STATE['force_stop'] = False
                break
 
            global WAITING_FOR_FILL, TRADE_COMPLETED
 
            # ORDER COMPLETION GUARD
            if not BOT_STATE['order_completed']:
                pending_order_id = BOT_STATE.get('last_placed_order_id', 'UNKNOWN')
                log_system(f"BLOCKING NEXT ORDER - CURRENT ORDER NOT FINISHED: {pending_order_id}")
 
                has_position, was_closed, pnl = check_position_and_detect_closure()
 
                if was_closed:
                    print(f"🎯 Position closed during order-guard wait! PnL: {pnl}")
                    if pnl > 0 and BOT_STATE['stop_at_win']:
                        BOT_STATE['running']     = False
                        BOT_STATE['stop_at_win'] = False
                        continue
                    if pnl < 0 and BOT_STATE['current_step'] == 1 and BOT_STATE['stop_at_max_step']:
                        BOT_STATE['running']          = False
                        BOT_STATE['stop_at_max_step'] = False
                        continue
                elif has_position:
                    print("⏳ Order guard: active position - waiting...")
                    time.sleep(0.5)
                    continue
                else:
                    if not BOT_STATE['order_completed']:
                        print(f"⏳ Order guard: waiting for fill processing for order {pending_order_id}...")
                        time.sleep(0.5)
                        continue
 
            if WAITING_FOR_FILL:
                if TRADE_COMPLETED:
                    WAITING_FOR_FILL = False
                    TRADE_COMPLETED  = False
                    print("✅ Trade completed and saved, ready for next trade")
                else:
                    print("⏳ Waiting for trade save to complete...")
                    time.sleep(0.5)
                    continue
 
            print(f"\n{'='*50}")
            print(f"🔍 BOT LOOP - Symbol: {BOT_STATE['symbol']}")
            print(f"📊 Running: {BOT_STATE['running']}, Step: {BOT_STATE['current_step']}, Lot: {BOT_STATE['current_lot']}")
            print(f"{'='*50}")
 
            has_position, was_closed, pnl = check_position_and_detect_closure()
 
            if was_closed:
                print(f"🎯 Position closed! PnL: {pnl}")
                print(f"📊 Result: {'PROFIT ✅' if pnl > 0 else 'LOSS ❌'}")
                if pnl > 0 and BOT_STATE['stop_at_win']:
                    BOT_STATE['running']     = False
                    BOT_STATE['stop_at_win'] = False
                    continue
                if pnl < 0 and BOT_STATE['current_step'] == 1 and BOT_STATE['stop_at_max_step']:
                    BOT_STATE['running']          = False
                    BOT_STATE['stop_at_max_step'] = False
                    continue
 
            if has_position:
                print("⏳ Active position - waiting for closure...")
                time.sleep(0.5)
                continue
 
            if BOT_STATE['force_stop']:
                BOT_STATE['running'] = False
                continue
 
            # COOLDOWN CHECK
            elapsed = time.time() - LAST_CLOSE_TIMESTAMP
            if elapsed < COOLDOWN_SECONDS and LAST_CLOSE_TIMESTAMP > 0:
                remaining = COOLDOWN_SECONDS - elapsed
                print(f"⏱️ COOLDOWN: {remaining:.1f}s remaining before next order")
                time.sleep(remaining)
                continue
 
            # ================================================================
            # PRE-ORDER FILL VERIFICATION (INDEPENDENT)
            # ================================================================
            # This replaces the previous DB load + sync logic.
            # It independently verifies the last real fill pair from official
            # API and corrects step/lot in BOT_STATE before every new order.
            #
            # NOTE: On fresh session start (LAST_CLOSE_TIMESTAMP == 0.0),
            # pre_order_fill_verification() will immediately return with
            # source='fresh_start' and will NOT read old fills.
            # This prevents old session fills from overriding Step 1.
            # ================================================================
            if not BOT_STATE['order_completed']:
                pending_order_id = BOT_STATE.get('last_placed_order_id', 'UNKNOWN')
                log_error(f"BLOCKED - ORDER NOT FINISHED: {pending_order_id}")
                time.sleep(0.5)
                continue
 
            print(f"\n🔍 [PRE-ORDER VERIFY] Running independent fill verification...")
 
            # ─── KEY DIFFERENCE FROM ORIGINAL ───────────────────────────────
            # Instead of load_bot_state_from_db() + verify_and_sync_step_from_db()
            # we call pre_order_fill_verification() which:
            # 1. Returns immediately if LAST_CLOSE_TIMESTAMP == 0.0 (fresh start)
            # 2. Waits for fills only if position closed recently this session
            # 3. Fetches real fills from API
            # 4. Finds last entry+exit pair
            # 5. Corrects step/lot from REAL lot_size, not from BOT_STATE
            # ─────────────────────────────────────────────────────────────────
            elapsed_since_close = time.time() - LAST_CLOSE_TIMESTAMP if LAST_CLOSE_TIMESTAMP > 0 else 9999
            # If position just closed (within last 30s), wait for fills to propagate
            # Otherwise no wait needed (fills from previous trade already propagated)
            verify_wait = 0 if elapsed_since_close > 30 else max(0, 15 - elapsed_since_close)
 
            verify_result = pre_order_fill_verification(BOT_STATE['symbol'], wait_seconds=int(verify_wait))
 
            print(f"   ✅ Verify result: source={verify_result['source']} | pair_found={verify_result['pair_found']}")
            print(f"   ✅ Last lot_used={verify_result['lot_used']} | pnl={verify_result['pnl']:.5f}")
            print(f"   ✅ Next step={verify_result['next_step']} | Next lot={verify_result['next_lot']}")
 
            if BOT_STATE['stop_at_win']:
                print("🎯 STOP AT WIN ACTIVE - Will stop after next profit")
 
            next_lot = calculate_next_lot()
            print(f"\n💰 PLACING ORDER - Step: {BOT_STATE['current_step']}, Lot: {next_lot}")
 
            LAST_TRADE_RESULT['processed'] = False
 
            signal_result = get_trading_signal()
            side          = signal_result[0] if signal_result else 'buy'
            signal_data   = signal_result[1] if signal_result and len(signal_result) > 1 else {}
 
            if side is None:
                print("⏳ No signal - skipping this cycle")
                time.sleep(1)
                continue
 
            print(f"📈 Signal: {side.upper()} | Lot: {next_lot}")
 
            # Safety check: confirm no real position before placing order
            product_id = get_product_id(BOT_STATE['symbol'])
            if product_id:
                current_pos = check_position_realtime(product_id, expected_symbol=BOT_STATE['symbol'])
                if abs(current_pos.get('size', 0)) > 0.001:
                    print("⛔ SAFETY CHECK: Real position exists - SKIPPING ORDER")
                    LAST_POSITION_STATE['symbol']      = BOT_STATE['symbol']
                    LAST_POSITION_STATE['size']        = current_pos.get('size', 0)
                    LAST_POSITION_STATE['entry_price'] = current_pos.get('entry_price', 0)
                    continue
 
            # Final guard before order
            if not BOT_STATE['order_completed']:
                pending_order_id = BOT_STATE.get('last_placed_order_id', 'UNKNOWN')
                log_error(f"BLOCKING NEXT ORDER - ORDER NOT FINISHED: {pending_order_id}")
                time.sleep(0.5)
                continue
 
            print(f"🎯 PLACING ORDER: {side.upper()} {next_lot} lots")
 
            BOT_STATE['order_completed']      = False
            BOT_STATE['last_placed_order_id'] = None
 
            order_response = place_order_with_bracket(
                BOT_STATE['symbol'],
                side,
                next_lot,
                BOT_STATE['leverage'],
                BOT_STATE['tp_percent'],
                BOT_STATE['sl_percent']
            )
 
            if order_response and order_response.get('success'):
                placed_order_id = order_response.get('result', {}).get('id')
                log_trade(f"ORDER PLACED | {side.upper()} | Lot={next_lot} | OrderID={placed_order_id}")
                log_trade(f"TRACKING ORDER: {placed_order_id}")
 
                BOT_STATE['last_placed_order_id'] = placed_order_id
                print(f"   🎯 Tracking order ID: {placed_order_id}")

                if placed_order_id:
                    with db_lock:
                        ACTIVE_TRADE_DECISIONS[str(placed_order_id)] = signal_data.get('trade_decisions', 'No decisions available')

                    # >>> ADDED: save the full multi-timeframe signal snapshot for
                    # this order. INFO ONLY — runs AFTER order is already placed,
                    # cannot affect the order or the decision that led to it.
                    try:
                        # Signal Scores History = bot's original decision only.
                        # Reverse (if On) flips the live order side, not this snapshot.
                        original_decision = (
                            signal_data.get('original_signal')
                            or signal_data.get('signal')
                            or side.upper()
                        )
                        original_decision = str(original_decision).upper()
                        save_signal_score_history(
                            order_id=placed_order_id,
                            symbol=BOT_STATE['symbol'],
                            position=original_decision,
                            lot_size=next_lot,
                            timeframe_scores=signal_data.get('timeframe_scores', {}),
                            final_decision=original_decision,
                            entry_time=datetime.now().isoformat()
                        )
                    except Exception as e:
                        log_error(f"[SIGNAL SCORES] Failed to save for order {placed_order_id}: {e}")

 
                # POSITION CONFIRMATION
                print("⚡ Confirming position...")
                max_wait_time  = 2
                wait_start     = time.time()
                position_found = False
 
                while time.time() - wait_start < max_wait_time:
                    time.sleep(0.05)
                    current_pos = check_position_realtime(product_id, expected_symbol=BOT_STATE['symbol'])
                    if abs(current_pos.get('size', 0)) > 0.001:
                        print(f"⚡ Position confirmed: {current_pos.get('size', 0)} lots")
                        LAST_POSITION_STATE['symbol']      = BOT_STATE['symbol']
                        LAST_POSITION_STATE['size']        = current_pos.get('size', 0)
                        LAST_POSITION_STATE['entry_price'] = current_pos.get('entry_price', 0)
                        BOT_STATE['current_lot']           = abs(current_pos.get('size', 0))
                        position_found = True
                        break
 
                if not position_found:
                    print("⚠️ Position not confirmed in fast-poll — doing final authoritative check...")
                    final_pos = check_position_realtime(product_id, expected_symbol=BOT_STATE['symbol'])

                    if final_pos.get('error'):
                        print("⚠️ API error on final position check — skipping state update")
                    elif abs(final_pos.get('size', 0)) > 0.001:
                        print(f"✅ Final check: position confirmed {final_pos.get('size', 0)} lots")
                        LAST_POSITION_STATE['symbol']      = BOT_STATE['symbol']
                        LAST_POSITION_STATE['size']        = final_pos.get('size', 0)
                        LAST_POSITION_STATE['entry_price'] = final_pos.get('entry_price', 0)
                        BOT_STATE['current_lot']           = abs(final_pos.get('size', 0))
                    else:
                        print(f"ℹ️ Final check: no position found for order {placed_order_id}")
                        print(f"ℹ️ Possible micro-close — dead reckoning will detect via fills")
 
            else:
                print("❌ Order failed!")
                print(f"📋 Response: {order_response}")
                BOT_STATE['order_completed']      = True
                BOT_STATE['last_placed_order_id'] = None
                time.sleep(0.5)
 
        except Exception as e:
            print(f"🚨 BOT ERROR: {e}")
            import traceback
            traceback.print_exc()
            print("🔄 Retrying in 5 seconds...")
            time.sleep(5)
            continue
 
    # NEW: session ended (either force-stop / stop-at-win / stop-at-max-step
    # naturally hit, or /api/stop-bot called) -> clear the "should auto
    # resume" flag so a future process restart does NOT bring the bot back
    # up on its own. Only an explicit UI stop should permanently stop it,
    # but here we intentionally clear it any time the loop exits normally,
    # since that means running=False was set (by force-stop or the UI).
    save_session_active_flag(False)
    bot_manager.deactivate_bot()  # unload strategy module when engine stops
    print("🤖 Auto Trading Bot Stopped")
 


# ========== TRADE COMPLETION MANAGEMENT ==========
def wait_for_complete_trade(symbol, max_wait=15):
    target_order_id = BOT_STATE.get('last_placed_order_id')
    if not target_order_id:
        log_error("wait_for_complete_trade: no last_placed_order_id set")
        return 0, None

    start = time.time()
    while time.time() - start < max_wait:
        pnl, data = find_trade_by_order_id(symbol, target_order_id)
        if data:
            return pnl, data
        time.sleep(1)

    log_error(f"Fills not received in {max_wait}s for order {target_order_id}")
    return 0, None


def get_trade_with_retry(symbol, retries=5):
    target_order_id = BOT_STATE.get('last_placed_order_id')
    return wait_for_trade_fills(symbol, target_order_id, max_retries=retries, retry_delay=2)


def get_pnl_from_fills():
    pnl, _ = find_latest_closed_pair(BOT_STATE['symbol'])
    return pnl


def get_entry_exit_from_fills():
    _, entry_exit_data = find_latest_closed_pair(BOT_STATE['symbol'])
    return entry_exit_data


def _apply_step_progression(pnl, be_retry=False):
    global BOT_STATE

    current_step = BOT_STATE['current_step']
    current_lot  = BOT_STATE['current_lot']

    result_type = 'PROFIT' if pnl > 0 else 'LOSS'
    log_trade(f"POSITION CLOSED | PnL={pnl:.5f} | Result={result_type}")

    if be_retry:
        log_state(
            f"STEP UNCHANGED | BE SL hit → retry same Step {current_step} | Lot={current_lot}"
        )
        log_state(f"Next Trade => {BOT_STATE['symbol']} {BOT_STATE['current_lot']} Lots")
        return

    if pnl > 0:
        next_step = 1
        next_lot  = LOT_STEPS[next_step]
        BOT_STATE['current_step'] = next_step
        BOT_STATE['current_lot']  = next_lot
        log_state(f"STEP UPDATED | WIN → Step {next_step} | LOT UPDATED | Lot={next_lot}")
    else:
        next_step = current_step + 1
        if next_step > BOT_STATE['max_steps']:
            next_step = 1
            next_lot  = LOT_STEPS[next_step]
            BOT_STATE['current_step'] = next_step
            BOT_STATE['current_lot']  = next_lot
            log_state(f"STEP UPDATED | MAX STEP REACHED → Step {next_step} | LOT UPDATED | Lot={next_lot}")
        else:
            next_lot  = LOT_STEPS[next_step]
            BOT_STATE['current_step'] = next_step
            BOT_STATE['current_lot']  = next_lot
            log_state(f"STEP UPDATED | LOSS → Step {next_step} | LOT UPDATED | Lot={next_lot}")

    log_state(f"Next Trade => {BOT_STATE['symbol']} {BOT_STATE['current_lot']} Lots")


# ========== WEBSOCKET FILL ENGINE ==========
WS_FILL_QUEUE          = []
WS_FILL_QUEUE_LOCK     = Lock()
WS_POSITION_QUEUE      = []
WS_POSITION_QUEUE_LOCK = Lock()
WS_APP             = None
WS_THREAD          = None
WS_AUTHENTICATED   = False
WS_RUNNING         = False
WS_RECONNECT_DELAY = 3


def _ws_generate_signature(secret, message):
    return hmac.new(
        secret.encode('utf-8'),
        message.encode('utf-8'),
        hashlib.sha256
    ).hexdigest()


def _ws_send_auth(ws):
    method    = 'GET'
    timestamp = str(int(time.time()))
    path      = '/live'
    sig_data  = method + timestamp + path
    signature = _ws_generate_signature(DELTA_API_SECRET, sig_data)
    auth_msg = {
        "type": "auth",
        "payload": {
            "api-key":   DELTA_API_KEY,
            "signature": signature,
            "timestamp": timestamp
        }
    }
    ws.send(json.dumps(auth_msg))
    print("[WS] Auth payload sent")


def _ws_subscribe(ws, channel, symbols):
    sub_msg = {
        "type": "subscribe",
        "payload": {
            "channels": [{"name": channel, "symbols": symbols}]
        }
    }
    ws.send(json.dumps(sub_msg))
    print(f"[WS] Subscribed to channel='{channel}' symbols={symbols}")


def _ws_on_open(ws):
    print("[WS] Connection opened - sending auth...")
    _ws_send_auth(ws)


def _ws_on_message(ws, message):
    global WS_AUTHENTICATED, WS_SUBSCRIBED_SYMBOL
    try:
        msg      = json.loads(message)
        msg_type = msg.get('type', '')

        if msg_type == 'success' and msg.get('message') == 'Authenticated':
            WS_AUTHENTICATED = True
            print("[WS] Authenticated successfully")
            symbol = BOT_STATE.get('symbol', 'PAXGUSD')
            _ws_subscribe(ws, 'v2/user_trades', [symbol])
            _ws_subscribe(ws, 'positions',      [symbol])
            # FIX: track which symbol WS is currently subscribed to, so
            # ensure_ws_symbol_sync() can detect a mismatch after a symbol
            # change and force a resubscribe.
            WS_SUBSCRIBED_SYMBOL = symbol

            # ADDED: mark price channel (public) — this was missing entirely,
            # which is why MARK_PRICES dict was never getting populated via WS
            mark_symbol = f"MARK:{symbol}"
            _ws_subscribe(ws, 'mark_price', [mark_symbol])
            return

        # ADDED: mark price update handler — this whole block was missing
        if msg_type == 'mark_price':
            raw_sym = msg.get('symbol', '')        # e.g. "MARK:PAXGUSD"
            sym     = raw_sym.replace('MARK:', '') # → "PAXGUSD"
            price   = msg.get('price') or msg.get('mark_price')
            if sym and price:
                with MARK_PRICES_LOCK:
                    MARK_PRICES[sym] = float(price)
                # FIX: this used to log EVERY single tick (every 1-2s) into
                # trade.log forever, which can fill up disk over many days
                # and crash the process (looking exactly like an "auto
                # shutdown"). Now throttled to at most once per 30s per symbol.
                log_throttled(log_system, f"ws_mark_tick_{sym}",
                              f"[WS-MARK-TICK] {sym} = {price} @ {datetime.now().strftime('%H:%M:%S.%f')}",
                              min_interval=30)
            return

        if msg_type == 'v2/user_trades':
            fill = {
                'fill_id':             msg.get('f'),
                'order_id':            str(msg.get('o', '')),
                'side':                msg.get('S', ''),
                'size':                float(msg.get('s', 0)),
                'price':               float(msg.get('p', 0)),
                'position_after_fill': float(msg.get('po', 0)),
                'symbol':              msg.get('sy', ''),
                'timestamp_us':        msg.get('t', 0),
                'sequence_id':         msg.get('se', 0),
                'source':              'websocket'
            }
            with WS_FILL_QUEUE_LOCK:
                WS_FILL_QUEUE.append(fill)
            print(f"[WS] FILL received: {fill['side'].upper()} "
                  f"{fill['size']} @ {fill['price']} "
                  f"| pos_after={fill['position_after_fill']} "
                  f"| order_id={fill['order_id']}")
            return

        if msg_type == 'positions':
            action = msg.get('action', '')
            if action == 'snapshot':
                for pos in msg.get('result', []):
                    sym = pos.get('product_symbol') or pos.get('symbol', '')
                    if sym == BOT_STATE.get('symbol'):
                        update = {
                            'symbol':      sym,
                            'size':        float(pos.get('size', 0)),
                            'entry_price': float(pos.get('entry_price', 0) or 0),
                            'action':      'snapshot',
                            'source':      'websocket'
                        }
                        with WS_POSITION_QUEUE_LOCK:
                            WS_POSITION_QUEUE.append(update)
                        print(f"[WS] POSITION snapshot: size={update['size']} entry={update['entry_price']}")
            else:
                sym = msg.get('symbol', '')
                if sym == BOT_STATE.get('symbol'):
                    update = {
                        'symbol':      sym,
                        'size':        float(msg.get('size', 0)),
                        'entry_price': float(msg.get('entry_price', 0) or 0),
                        'action':      action,
                        'source':      'websocket'
                    }
                    with WS_POSITION_QUEUE_LOCK:
                        WS_POSITION_QUEUE.append(update)
                    print(f"[WS] POSITION update ({action}): size={update['size']} entry={update['entry_price']}")
            return

    except Exception as e:
        print(f"[WS] on_message error: {e}")

def _ws_on_error(ws, error):
    print(f"[WS] Error: {error}")


def _ws_on_close(ws, close_status_code, close_msg):
    global WS_AUTHENTICATED
    WS_AUTHENTICATED = False
    print(f"[WS] Connection closed | code={close_status_code} msg={close_msg}")


def _ws_reconnect_loop():
    global WS_APP, WS_RUNNING, WS_AUTHENTICATED
    print("[WS] Reconnect loop started")
    while WS_RUNNING:
        try:
            print(f"[WS] Connecting to {WS_URL} ...")
            WS_AUTHENTICATED = False
            WS_APP = websocket.WebSocketApp(
                WS_URL,
                on_open    = _ws_on_open,
                on_message = _ws_on_message,
                on_error   = _ws_on_error,
                on_close   = _ws_on_close
            )
            WS_APP.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print(f"[WS] run_forever exception: {e}")
        if WS_RUNNING:
            print(f"[WS] Reconnecting in {WS_RECONNECT_DELAY}s ...")
            time.sleep(WS_RECONNECT_DELAY)
    print("[WS] Reconnect loop exited")


def start_websocket_engine():
    global WS_THREAD, WS_RUNNING
    if WS_RUNNING and WS_THREAD and WS_THREAD.is_alive():
        print("[WS] Engine already running")
        return
    WS_RUNNING = True
    WS_THREAD  = threading.Thread(target=_ws_reconnect_loop, daemon=True, name="WS-Engine")
    WS_THREAD.start()
    print("[WS] Engine thread started")


def stop_websocket_engine():
    global WS_RUNNING, WS_APP, WS_AUTHENTICATED, WS_SUBSCRIBED_SYMBOL
    WS_RUNNING       = False
    WS_AUTHENTICATED = False
    WS_SUBSCRIBED_SYMBOL = None
    if WS_APP:
        try:
            WS_APP.close()
        except Exception:
            pass
        WS_APP = None
    print("[WS] Engine stopped")


def _drain_ws_fill_queue(symbol):
    with WS_FILL_QUEUE_LOCK:
        symbol_fills = [f for f in WS_FILL_QUEUE if f.get('symbol') == symbol]
        WS_FILL_QUEUE[:] = [f for f in WS_FILL_QUEUE if f.get('symbol') != symbol]
    symbol_fills.sort(key=lambda x: x.get('timestamp_us', 0))
    return symbol_fills


def _drain_ws_position_queue(symbol):
    with WS_POSITION_QUEUE_LOCK:
        symbol_pos = [p for p in WS_POSITION_QUEUE if p.get('symbol') == symbol]
        WS_POSITION_QUEUE[:] = [p for p in WS_POSITION_QUEUE if p.get('symbol') != symbol]
    if not symbol_pos:
        return None
    return symbol_pos[-1]


def _pair_ws_fills(fills, symbol):
    if not fills:
        return []

    target_order_id = str(BOT_STATE.get('last_placed_order_id', ''))

    if target_order_id and target_order_id != 'RESUMED':
        with PROCESSED_ORDER_IDS_LOCK:
            if target_order_id in PROCESSED_ORDER_IDS:
                log_trade(f"WS PAIR SKIPPED | order {target_order_id} already in PROCESSED_ORDER_IDS")
                return []

    order_groups = {}
    for fill in fills:
        oid   = fill['order_id']
        side  = fill['side']
        size  = fill['size']
        price = fill['price']
        ts    = fill['timestamp_us']

        if oid not in order_groups:
            order_groups[oid] = {
                'order_id':     oid,
                'side':         side,
                'total_size':   0.0,
                'total_value':  0.0,
                'avg_price':    0.0,
                'timestamp_us': ts,
                'fills_count':  0
            }

        grp = order_groups[oid]
        grp['total_size']  += size
        grp['total_value'] += price * size
        grp['fills_count'] += 1
        if ts < grp['timestamp_us']:
            grp['timestamp_us'] = ts

    for grp in order_groups.values():
        if grp['total_size'] > 0:
            grp['avg_price'] = grp['total_value'] / grp['total_size']

    sorted_orders = sorted(order_groups.values(), key=lambda x: x['timestamp_us'])

    if target_order_id and target_order_id != 'RESUMED':
        entry_order = order_groups.get(target_order_id)
        if not entry_order:
            return []

        with USED_FILL_IDS_LOCK:
            already_used = set(USED_FILL_IDS)

        exit_order = None
        for grp in sorted_orders:
            if grp['order_id'] == target_order_id:
                continue
            if grp['order_id'] in already_used:
                continue
            if grp['side'] != entry_order['side']:
                if grp['timestamp_us'] >= entry_order['timestamp_us']:
                    exit_order = grp
                    break

        if not exit_order:
            return []

        entry_side  = entry_order['side']
        entry_price = entry_order['avg_price']
        exit_price  = exit_order['avg_price']
        trade_size  = min(entry_order['total_size'], exit_order['total_size'])

        lot_size        = LOT_SIZES.get(symbol, LOT_SIZE_DEFAULT)
        actual_quantity = trade_size * lot_size

        if entry_side == 'buy':
            pnl = (exit_price - (entry_price + fees)) * actual_quantity 
        else:
            pnl = ((entry_price - fees) - exit_price) * actual_quantity 

        entry_ts_iso = datetime.utcfromtimestamp(entry_order['timestamp_us'] / 1_000_000).isoformat() + 'Z'
        exit_ts_iso  = datetime.utcfromtimestamp(exit_order['timestamp_us'] / 1_000_000).isoformat() + 'Z'

        log_trade(f"PNL CALCULATED | {entry_side.upper()} | Entry={entry_price:.6f} | Exit={exit_price:.6f} | PnL={pnl:.6f}")
        log_trade(f"PAIR FOUND FOR ORDER: {target_order_id}")

        with PROCESSED_ORDER_IDS_LOCK:
            PROCESSED_ORDER_IDS.add(target_order_id)
            _trim_id_set(PROCESSED_ORDER_IDS)

        with USED_FILL_IDS_LOCK:
            USED_FILL_IDS.add(target_order_id)
            USED_FILL_IDS.add(exit_order['order_id'])
            _trim_id_set(USED_FILL_IDS)

        return [{
            'side':           entry_side,
            'entry_price':    entry_price,
            'exit_price':     exit_price,
            'quantity':       trade_size,
            'entry_time':     entry_ts_iso,
            'exit_time':      exit_ts_iso,
            'pnl':            pnl,
            'entry_order_id': entry_order['order_id'],
            'exit_order_id':  exit_order['order_id']
        }]

    # Fallback: FIFO pairing
    completed_trades = []
    pending_entries  = []

    for order in sorted_orders:
        if not pending_entries:
            pending_entries.append(order)
            continue

        last_entry = pending_entries[-1]

        if last_entry['side'] != order['side']:
            entry_order = pending_entries.pop()
            exit_order  = order

            with PROCESSED_ORDER_IDS_LOCK:
                if entry_order['order_id'] in PROCESSED_ORDER_IDS:
                    continue

            entry_side  = entry_order['side']
            entry_price = entry_order['avg_price']
            exit_price  = exit_order['avg_price']
            trade_size  = min(entry_order['total_size'], exit_order['total_size'])

            lot_size        = LOT_SIZES.get(symbol, LOT_SIZE_DEFAULT)
            actual_quantity = trade_size * lot_size

            if entry_side == 'buy':
                pnl = (exit_price - (entry_price + fees)) * actual_quantity 
            else:
                pnl = ((entry_price - fees) - exit_price) * actual_quantity 

            entry_ts_iso = datetime.utcfromtimestamp(entry_order['timestamp_us'] / 1_000_000).isoformat() + 'Z'
            exit_ts_iso  = datetime.utcfromtimestamp(exit_order['timestamp_us'] / 1_000_000).isoformat() + 'Z'

            with PROCESSED_ORDER_IDS_LOCK:
                PROCESSED_ORDER_IDS.add(entry_order['order_id'])
                _trim_id_set(PROCESSED_ORDER_IDS)

            with USED_FILL_IDS_LOCK:
                USED_FILL_IDS.add(entry_order['order_id'])
                USED_FILL_IDS.add(exit_order['order_id'])
                _trim_id_set(USED_FILL_IDS)

            completed_trades.append({
                'side':           entry_side,
                'entry_price':    entry_price,
                'exit_price':     exit_price,
                'quantity':       trade_size,
                'entry_time':     entry_ts_iso,
                'exit_time':      exit_ts_iso,
                'pnl':            pnl,
                'entry_order_id': entry_order['order_id'],
                'exit_order_id':  exit_order['order_id']
            })
        else:
            pending_entries.append(order)

    return completed_trades


# ========== POSITION TRACKING (WEBSOCKET + POLLING HYBRID) ==========
def check_position_and_detect_closure():
    global LAST_POSITION_STATE, LAST_CLOSE_TIMESTAMP
    global WAITING_FOR_FILL, TRADE_COMPLETED, CURRENT_SIGNAL

    try:
        symbol = BOT_STATE['symbol']

        # STEP 1: Drain WebSocket fill queue
        ws_fills = _drain_ws_fill_queue(symbol)

        if ws_fills:
            print(f"[WS] {len(ws_fills)} new fill(s) drained from WS queue")

            with USED_FILL_IDS_LOCK:
                fresh_fills = [f for f in ws_fills if f['order_id'] not in USED_FILL_IDS]

            if fresh_fills:
                completed_trades = _pair_ws_fills(fresh_fills, symbol)

                if completed_trades:
                    for trade in completed_trades:
                        print(f"[WS] Processing WS-detected trade: {trade['side'].upper()} PnL={trade['pnl']:.6f}")

                        completed_order_id = trade.get('entry_order_id', BOT_STATE.get('last_placed_order_id'))

                        LAST_CLOSE_TIMESTAMP               = time.time()
                        LAST_POSITION_STATE['size']        = 0
                        LAST_POSITION_STATE['entry_price'] = 0

                        pnl             = trade['pnl']
                        entry_exit_data = {
                            'side':           trade['side'],
                            'entry_price':    trade['entry_price'],
                            'exit_price':     trade['exit_price'],
                            'quantity':       trade['quantity'],
                            'entry_time':     trade['entry_time'],
                            'exit_time':      trade['exit_time'],
                            'entry_order_id': trade.get('entry_order_id'),
                            'exit_order_id':  trade.get('exit_order_id')
                        }

                        LAST_TRADE_RESULT['profit_loss'] = pnl
                        LAST_TRADE_RESULT['timestamp']   = datetime.now().isoformat()
                        LAST_TRADE_RESULT['lot_used']    = trade['quantity']
                        LAST_TRADE_RESULT['processed']   = True

                        BOT_STATE['last_result'] = 'PROFIT' if pnl > 0 else 'LOSS'
                        BOT_STATE['last_pnl']    = pnl

                        _handle_close_step_progression(
                            symbol, pnl,
                            exit_price=entry_exit_data.get('exit_price'),
                            entry_price=entry_exit_data.get('entry_price'),
                            side=entry_exit_data.get('side'),
                        )
                        save_bot_state_to_db()
                        log_state(f"STATE SAVED FOR ORDER: {completed_order_id} | Step={BOT_STATE['current_step']} | Lot={BOT_STATE['current_lot']}")

                        save_closed_position({
                            'symbol':         symbol,
                            'side':           entry_exit_data['side'],
                            'entry_price':    entry_exit_data['entry_price'],
                            'exit_price':     entry_exit_data['exit_price'],
                            'quantity':       entry_exit_data['quantity'],
                            'pnl':            pnl,
                            'entry_time':     entry_exit_data['entry_time'],
                            'exit_time':      entry_exit_data['exit_time'],
                            'entry_order_id': entry_exit_data.get('entry_order_id'),
                            'exit_order_id':  entry_exit_data.get('exit_order_id'),
                        })
                        log_trade(f"TRADE SAVED FOR ORDER: {completed_order_id}")

                        if BOT_STATE['session_start_time']:
                            BOT_STATE['session_total_pnl'] += pnl

                        result_type    = "PROFIT" if pnl > 0 else "LOSS"
                        reason         = f"after_{result_type.lower()}_pnl={pnl:.5f}"
                        CURRENT_SIGNAL = generate_smart_signal(reason=reason)

                        WAITING_FOR_FILL = True
                        TRADE_COMPLETED  = True
                        log_system("Trade paired (WS)")

                        _mark_order_complete(completed_order_id)

                    ws_pos = _drain_ws_position_queue(symbol)
                    if ws_pos:
                        LAST_POSITION_STATE = {
                            'symbol':      symbol,
                            'size':        ws_pos['size'],
                            'entry_price': ws_pos['entry_price']
                        }
                        has_position = abs(ws_pos['size']) > 0.001
                    else:
                        product_id = get_product_id(symbol)
                        if product_id:
                            current_pos = check_position_realtime(product_id, expected_symbol=symbol)
                            if not current_pos.get('error'):
                                LAST_POSITION_STATE = {
                                    'symbol':      symbol,
                                    'size':        current_pos.get('size', 0),
                                    'entry_price': current_pos.get('entry_price', 0)
                                }
                        has_position = abs(LAST_POSITION_STATE.get('size', 0)) > 0.001

                    return has_position, True, completed_trades[-1]['pnl']

        # STEP 2: Check WebSocket position queue
        ws_pos = _drain_ws_position_queue(symbol)

        if ws_pos:
            print(f"[WS] Position update from WS: size={ws_pos['size']} entry={ws_pos['entry_price']} action={ws_pos['action']}")

            prev_size = LAST_POSITION_STATE['size']
            LAST_POSITION_STATE = {
                'symbol':      symbol,
                'size':        ws_pos['size'],
                'entry_price': ws_pos['entry_price']
            }

            if abs(prev_size) > 0.001 and abs(ws_pos['size']) <= 0.001:
                log_trade("POSITION CLOSED | Detected via WS positions channel")

                LAST_CLOSE_TIMESTAMP               = time.time()
                prev_entry_price                   = LAST_POSITION_STATE['entry_price']
                LAST_POSITION_STATE['size']        = 0
                LAST_POSITION_STATE['entry_price'] = 0

                target_order_id = BOT_STATE.get('last_placed_order_id')
                log_trade(f"TRACKING ORDER: {target_order_id}")

                print("⏳ Waiting 15 seconds for fills to propagate...")
                time.sleep(15)

                pnl, entry_exit_data = wait_for_trade_fills(symbol, target_order_id, max_retries=5, retry_delay=2)

                if entry_exit_data:
                    log_trade(f"PAIR FOUND FOR ORDER: {target_order_id}")

                    LAST_TRADE_RESULT['profit_loss'] = pnl
                    LAST_TRADE_RESULT['timestamp']   = datetime.now().isoformat()
                    LAST_TRADE_RESULT['lot_used']    = prev_size
                    LAST_TRADE_RESULT['processed']   = True

                    BOT_STATE['last_result'] = 'PROFIT' if pnl > 0 else 'LOSS'
                    BOT_STATE['last_pnl']    = pnl

                    _handle_close_step_progression(
                        symbol, pnl,
                        exit_price=entry_exit_data.get('exit_price'),
                        entry_price=entry_exit_data.get('entry_price'),
                        side=entry_exit_data.get('side'),
                    )
                    save_bot_state_to_db()
                    log_state(f"STATE SAVED FOR ORDER: {target_order_id} | Step={BOT_STATE['current_step']} | Lot={BOT_STATE['current_lot']}")

                    save_closed_position({
                        'symbol':         symbol,
                        'side':           entry_exit_data['side'],
                        'entry_price':    entry_exit_data['entry_price'],
                        'exit_price':     entry_exit_data['exit_price'],
                        'quantity':       entry_exit_data['quantity'],
                        'pnl':            pnl,
                        'entry_time':     entry_exit_data['entry_time'],
                        'exit_time':      entry_exit_data['exit_time'],
                        'entry_order_id': entry_exit_data.get('entry_order_id'),
                        'exit_order_id':  entry_exit_data.get('exit_order_id'),
                    })
                    log_trade(f"TRADE SAVED FOR ORDER: {target_order_id}")

                    if BOT_STATE['session_start_time']:
                        BOT_STATE['session_total_pnl'] += pnl

                    result_type    = "PROFIT" if pnl > 0 else "LOSS"
                    reason         = f"after_{result_type.lower()}_pnl={pnl:.5f}"
                    CURRENT_SIGNAL = generate_smart_signal(reason=reason)

                    WAITING_FOR_FILL = True
                    TRADE_COMPLETED  = True
                    log_system("Trade paired (WS-POS)")

                    _mark_order_complete(target_order_id)

                else:
                    log_error(f"Fills not found after retries for order {target_order_id} - using fallback")
                    pnl = 0

                    LAST_TRADE_RESULT['profit_loss'] = pnl
                    LAST_TRADE_RESULT['timestamp']   = datetime.now().isoformat()
                    LAST_TRADE_RESULT['lot_used']    = prev_size
                    LAST_TRADE_RESULT['processed']   = True

                    BOT_STATE['last_result'] = 'PROFIT' if pnl > 0 else 'LOSS'
                    BOT_STATE['last_pnl']    = pnl

                    _handle_close_step_progression(symbol, pnl)
                    save_bot_state_to_db()

                    fallback_data = {
                        'side':        'buy' if prev_size > 0 else 'sell',
                        'entry_price': prev_entry_price,
                        'exit_price':  prev_entry_price,
                        'quantity':    abs(prev_size),
                        'entry_time':  datetime.now().isoformat(),
                        'exit_time':   datetime.now().isoformat()
                    }
                    save_closed_position({
                        'symbol':         symbol,
                        'side':           fallback_data['side'],
                        'entry_price':    fallback_data['entry_price'],
                        'exit_price':     fallback_data['exit_price'],
                        'quantity':       fallback_data['quantity'],
                        'pnl':            pnl,
                        'entry_time':     fallback_data['entry_time'],
                        'exit_time':      fallback_data['exit_time'],
                        'entry_order_id': str(target_order_id) if target_order_id else None
                    })

                    WAITING_FOR_FILL = True
                    TRADE_COMPLETED  = True

                    _mark_order_complete(target_order_id)

                has_position = abs(ws_pos['size']) > 0.001
                return has_position, True, pnl

            has_position = abs(ws_pos['size']) > 0.001
            return has_position, False, 0

        # STEP 3: REST polling fallback
        product_id = get_product_id(symbol)
        if not product_id:
            return False, False, 0

        current_pos = check_position_realtime(product_id, expected_symbol=symbol)

        if current_pos.get('error'):
            return True, False, 0

        was_closed = False
        pnl        = 0

        if abs(LAST_POSITION_STATE['size']) > 0.001 and abs(current_pos.get('size', 0)) <= 0.001:
            log_trade("POSITION CLOSED | Detected via REST polling")

            LAST_CLOSE_TIMESTAMP = time.time()
            prev_size        = LAST_POSITION_STATE['size']
            prev_entry_price = LAST_POSITION_STATE['entry_price']
            LAST_POSITION_STATE['size']        = 0
            LAST_POSITION_STATE['entry_price'] = 0

            was_closed       = True
            WAITING_FOR_FILL = True
            TRADE_COMPLETED  = False

            target_order_id = BOT_STATE.get('last_placed_order_id')
            log_trade(f"TRACKING ORDER: {target_order_id}")

            print("⏳ Waiting 15 seconds for fills to propagate...")
            time.sleep(15)

            pnl, entry_exit_data = wait_for_trade_fills(symbol, target_order_id, max_retries=5, retry_delay=2)

            if entry_exit_data:
                log_trade(f"PAIR FOUND FOR ORDER: {target_order_id}")

                LAST_TRADE_RESULT['profit_loss'] = pnl
                LAST_TRADE_RESULT['timestamp']   = datetime.now().isoformat()
                LAST_TRADE_RESULT['lot_used']    = prev_size
                LAST_TRADE_RESULT['processed']   = True

                BOT_STATE['last_result'] = 'PROFIT' if pnl > 0 else 'LOSS'
                BOT_STATE['last_pnl']    = pnl

                _handle_close_step_progression(
                    symbol, pnl,
                    exit_price=entry_exit_data.get('exit_price'),
                    entry_price=entry_exit_data.get('entry_price'),
                    side=entry_exit_data.get('side'),
                )
                save_bot_state_to_db()
                log_state(f"STATE SAVED FOR ORDER: {target_order_id} | Step={BOT_STATE['current_step']} | Lot={BOT_STATE['current_lot']}")

                save_closed_position({
                    'symbol':         symbol,
                    'side':           entry_exit_data['side'],
                    'entry_price':    entry_exit_data['entry_price'],
                    'exit_price':     entry_exit_data['exit_price'],
                    'quantity':       entry_exit_data['quantity'],
                    'pnl':            pnl,
                    'entry_time':     entry_exit_data['entry_time'],
                    'exit_time':      entry_exit_data['exit_time'],
                    'entry_order_id': entry_exit_data.get('entry_order_id'),
                    'exit_order_id':  entry_exit_data.get('exit_order_id'),
                })
                log_trade(f"TRADE SAVED FOR ORDER: {target_order_id}")

                if BOT_STATE['session_start_time']:
                    BOT_STATE['session_total_pnl'] += pnl

                result_type    = "PROFIT" if pnl > 0 else "LOSS"
                reason         = f"after_{result_type.lower()}_pnl={pnl:.5f}"
                CURRENT_SIGNAL = generate_smart_signal(reason=reason)
                TRADE_COMPLETED = True
                log_system("Trade paired (REST)")

                _mark_order_complete(target_order_id)

            else:
                log_error(f"Fills not found after retries for order {target_order_id} - using fallback (REST)")

                LAST_TRADE_RESULT['profit_loss'] = pnl
                LAST_TRADE_RESULT['timestamp']   = datetime.now().isoformat()
                LAST_TRADE_RESULT['lot_used']    = prev_size
                LAST_TRADE_RESULT['processed']   = True

                BOT_STATE['last_result'] = 'PROFIT' if pnl > 0 else 'LOSS'
                BOT_STATE['last_pnl']    = pnl

                _handle_close_step_progression(symbol, pnl)
                save_bot_state_to_db()

                fallback_data = {
                    'side':        'buy' if prev_size > 0 else 'sell',
                    'entry_price': prev_entry_price,
                    'exit_price':  prev_entry_price,
                    'quantity':    abs(prev_size),
                    'entry_time':  datetime.now().isoformat(),
                    'exit_time':   datetime.now().isoformat()
                }
                save_closed_position({
                    'symbol':         symbol,
                    'side':           fallback_data['side'],
                    'entry_price':    fallback_data['entry_price'],
                    'exit_price':     fallback_data['exit_price'],
                    'quantity':       fallback_data['quantity'],
                    'pnl':            pnl,
                    'entry_time':     fallback_data['entry_time'],
                    'exit_time':      fallback_data['exit_time'],
                    'entry_order_id': str(target_order_id) if target_order_id else None
                })
                TRADE_COMPLETED = True
                _mark_order_complete(target_order_id)

        # STEP 4: DEAD RECKONING
        if (not was_closed
                and not BOT_STATE['order_completed']
                and BOT_STATE.get('last_placed_order_id')
                and BOT_STATE['last_placed_order_id'] != 'RESUMED'
                and abs(current_pos.get('size', 0)) <= 0.001
                and abs(LAST_POSITION_STATE.get('size', 0)) <= 0.001):

            target_order_id = BOT_STATE['last_placed_order_id']
            log_trade(f"DEAD RECKONING CHECK | No position found but order pending: {target_order_id}")

            pnl, entry_exit_data = wait_for_trade_fills(symbol, target_order_id, max_retries=5, retry_delay=2)

            if entry_exit_data:
                log_trade(f"DEAD RECKONING: PAIR FOUND FOR ORDER: {target_order_id}")
                log_trade("POSITION CLOSED | Detected via dead reckoning (fill-based)")

                LAST_CLOSE_TIMESTAMP               = time.time()
                LAST_POSITION_STATE['size']        = 0
                LAST_POSITION_STATE['entry_price'] = 0

                LAST_TRADE_RESULT['profit_loss'] = pnl
                LAST_TRADE_RESULT['timestamp']   = datetime.now().isoformat()
                LAST_TRADE_RESULT['lot_used']    = entry_exit_data['quantity']
                LAST_TRADE_RESULT['processed']   = True

                BOT_STATE['last_result'] = 'PROFIT' if pnl > 0 else 'LOSS'
                BOT_STATE['last_pnl']    = pnl

                _handle_close_step_progression(
                    symbol, pnl,
                    exit_price=entry_exit_data.get('exit_price'),
                    entry_price=entry_exit_data.get('entry_price'),
                    side=entry_exit_data.get('side'),
                )
                save_bot_state_to_db()
                log_state(f"STATE SAVED FOR ORDER: {target_order_id} | Step={BOT_STATE['current_step']} | Lot={BOT_STATE['current_lot']}")

                save_closed_position({
                    'symbol':         symbol,
                    'side':           entry_exit_data['side'],
                    'entry_price':    entry_exit_data['entry_price'],
                    'exit_price':     entry_exit_data['exit_price'],
                    'quantity':       entry_exit_data['quantity'],
                    'pnl':            pnl,
                    'entry_time':     entry_exit_data['entry_time'],
                    'exit_time':      entry_exit_data['exit_time'],
                    'entry_order_id': entry_exit_data.get('entry_order_id'),
                    'exit_order_id':  entry_exit_data.get('exit_order_id'),
                })
                log_trade(f"TRADE SAVED FOR ORDER: {target_order_id}")

                if BOT_STATE['session_start_time']:
                    BOT_STATE['session_total_pnl'] += pnl

                result_type    = "PROFIT" if pnl > 0 else "LOSS"
                reason         = f"after_{result_type.lower()}_pnl={pnl:.5f}"
                CURRENT_SIGNAL = generate_smart_signal(reason=reason)

                WAITING_FOR_FILL = True
                TRADE_COMPLETED  = True
                log_system("Trade paired (DEAD-RECKONING)")

                _mark_order_complete(target_order_id)

                return False, True, pnl

            log_trade(f"DEAD RECKONING: fills not found yet for order {target_order_id}")

        LAST_POSITION_STATE = {
            'symbol':      symbol,
            'size':        current_pos.get('size', 0),
            'entry_price': current_pos.get('entry_price', 0)
        }

        has_position = abs(current_pos.get('size', 0)) > 0.001
        return has_position, was_closed, pnl

    except Exception as e:
        log_error(f"Position processing error: {e}")
        import traceback
        traceback.print_exc()
        return False, False, 0


# ========== TRADING LOGIC ==========
CURRENT_SIGNAL = None


def get_trading_signal():
    global CURRENT_SIGNAL
    try:
        CURRENT_SIGNAL = generate_smart_signal(reason="trade_decision")
        signal = (CURRENT_SIGNAL.get('signal') or '').upper()
        # Keep bot's original decision for Signal Scores History (never reverse there).
        CURRENT_SIGNAL['original_signal'] = signal

        # Guardian UI toggle — applies AFTER any bot script, so Bot 1/2/3/4 all work.
        if get_guardian_config().get('reverse_decision') and signal in ('BUY', 'SELL'):
            flipped = 'SELL' if signal == 'BUY' else 'BUY'
            CURRENT_SIGNAL['signal'] = flipped
            CURRENT_SIGNAL['decision_reversed'] = True
            print(f"🔄 REVERSE DECISION ON | bot={signal} → trade={flipped}")
            signal = flipped
        else:
            CURRENT_SIGNAL['decision_reversed'] = False

        if signal == "BUY":
            return 'buy', CURRENT_SIGNAL
        elif signal == "SELL":
            return 'sell', CURRENT_SIGNAL
        else:
            print(f"⏳ Signal WAIT — sleeping before retry...")
            time.sleep(10)
            return None, CURRENT_SIGNAL

    except Exception as e:
        log_error(f"Getting signal: {e}")
        return None, None


def start_signal_bot():
    try:
        print("🤖 Signal bot ready - will generate unbiased random signals on demand")
        return True
    except Exception as e:
        print(f"❌ Error starting signal bot: {e}")
        return False


def stop_signal_bot():
    try:
        print("🛑 Signal bot stopped")
    except Exception as e:
        print(f"❌ Error stopping signal bot: {e}")


def detect_current_step_from_lot(lot_size):
    lot_size = abs(lot_size)
    for step, lot in LOT_STEPS.items():
        if lot == lot_size:
            return step
    for step, lot in LOT_STEPS.items():
        if lot >= lot_size:
            return step
    return 1


def detect_current_step_from_live_position():
    try:
        has_position = abs(LAST_POSITION_STATE['size']) > 0.001
        if has_position:
            current_lot  = abs(LAST_POSITION_STATE['size'])
            current_step = detect_current_step_from_lot(current_lot)
            print(f"🔍 Live position detected: Lot {current_lot} = Step {current_step}")
            return current_step, current_lot
        else:
            print("🔍 No live position detected - using Step 1")
            return 1, LOT_STEPS[1]
    except Exception as e:
        print(f"❌ Error detecting step from live position: {e}")
        return 1, LOT_STEPS[1]


def calculate_next_lot():
    global LOT_CALCULATION_LOCK

    if LOT_CALCULATION_LOCK:
        return BOT_STATE['current_lot']

    LOT_CALCULATION_LOCK = True
    try:
        expected_lot = LOT_STEPS.get(BOT_STATE['current_step'], LOT_STEPS[1])
        if BOT_STATE['current_lot'] != expected_lot:
            BOT_STATE['current_lot'] = expected_lot

        next_lot = BOT_STATE['current_lot']
        LAST_TRADE_RESULT['processed'] = False
        return next_lot
    finally:
        LOT_CALCULATION_LOCK = False


def place_order_with_bracket(symbol, side, size, leverage, tp_pct, sl_pct):
    try:
        # Tick + product_id from PRODUCT_IDS (set near BASE_URL at top of app.py)
        PRODUCT_CONFIG = {
            "PAXGUSD": {"id": PRODUCT_IDS["PAXGUSD"], "tick": Decimal("0.01")},
            "XAUTUSD": {"id": PRODUCT_IDS["XAUTUSD"], "tick": Decimal("0.01")},
        }

        config = PRODUCT_CONFIG.get(symbol)
        if not config:
            log_error(f"Symbol {symbol} not in config!")
            return None

        tick = config["tick"]
        p_id = config["id"] or get_product_id(symbol)
        if not p_id:
            log_error(f"Could not resolve product_id for {symbol}")
            return None

        # Re-apply leverage every order
        lev = max(1, int(leverage or 1))
        lev_resp = set_leverage(symbol, lev)
        if not lev_resp or not lev_resp.get('success'):
            log_error(f"set_leverage failed before order | lev={lev}x | resp={lev_resp}")

        # At Nx leverage, liquidation is ~ (100/N)% away.
        # Keep SL inside ~70% of that distance so bracket isn't past liq.
        liq_dist_pct = 100.0 / lev
        max_safe_sl = max(0.05, liq_dist_pct * 0.70)
        used_sl_pct = float(sl_pct)
        used_tp_pct = float(tp_pct)
        if used_sl_pct > max_safe_sl:
            log_system(
                f"SL CLAMP | requested SL={used_sl_pct}% > safe max {max_safe_sl:.3f}% "
                f"at {lev}x (liq≈{liq_dist_pct:.3f}%) → using {max_safe_sl:.3f}%"
            )
            used_sl_pct = max_safe_sl

        ticker = make_api_request('GET', f'/tickers/{symbol}')
        if not ticker or not ticker.get('result'):
            log_error(f"Ticker fetch failed for {symbol}")
            return None

        result     = ticker['result']
        mark_price = float(result.get('mark_price') or result.get('close'))

        def to_tick(val):
            d = Decimal(str(val))
            return (d / tick).quantize(Decimal("1"), rounding=ROUND_HALF_UP) * tick

        def calc_tp_sl(entry_px):
            base = to_tick(entry_px)
            if side == 'buy':
                tp = to_tick(entry_px * (1 + used_tp_pct / 100))
                sl = to_tick(entry_px * (1 - used_sl_pct / 100))
            else:
                tp = to_tick(entry_px * (1 - used_tp_pct / 100))
                sl = to_tick(entry_px * (1 + used_sl_pct / 100))
            min_gap = tick * Decimal("3")
            if side == 'buy':
                if tp <= base: tp = base + min_gap
                if sl >= base: sl = base - min_gap
            else:
                if tp >= base: tp = base - min_gap
                if sl <= base: sl = base + min_gap
            return str(tp), str(sl)

        tp_price, sl_price = calc_tp_sl(mark_price)

        # XAUT/PAXG testnet: show_bracket_orders=False — sending bracket_* on
        # the entry order returns invalid_contract. Place naked market first,
        # then attach TP/SL via /orders/bracket after fill.
        order_data = {
            "product_id": int(p_id),
            "product_symbol": symbol,
            "side": side,
            "order_type": "market_order",
            "size": int(size),
        }

        log_system(
            f"ORDER ATTEMPT | {symbol} {side.upper()} size={size} lev={lev}x "
            f"product_id={p_id} mark={mark_price} "
            f"(brackets after fill) TP={tp_price} ({used_tp_pct}%) SL={sl_price} ({used_sl_pct}%)"
        )

        response = make_api_request('POST', '/orders', order_data)

        if response and response.get('success') and 'result' in response:
            oid = response['result'].get('id')
            actual_entry = float(
                response['result'].get('average_fill_price') or
                response['result'].get('limit_price') or
                mark_price
            )
            log_trade(f"ORDER PLACED | {side.upper()} | Lot={size} | Entry={actual_entry} | OrderID={oid}")

            tp_price, sl_price = calc_tp_sl(actual_entry)
            bracket_payload = {
                "product_id": int(p_id),
                "product_symbol": symbol,
                "take_profit_order": {"order_type": "market_order", "stop_price": tp_price},
                "stop_loss_order": {"order_type": "market_order", "stop_price": sl_price},
                "bracket_stop_trigger_method": "mark_price",
            }
            br = make_api_request('POST', '/orders/bracket', bracket_payload)
            if not br or not br.get('success'):
                log_error(f"Bracket attach failed after fill | {br}")
            else:
                log_system(f"Bracket attached | TP={tp_price} SL={sl_price}")
        else:
            err = response.get('error') if response else 'No response'
            log_error(
                f"ORDER FAILED: {err} | product_id={p_id} lev={lev}x "
                f"liq≈{liq_dist_pct:.2f}% SL={used_sl_pct}%"
            )

        return response

    except Exception as e:
        log_error(f"Order placement exception: {e}")
        return None


def start_auto_trading_bot():
    if BOT_STATE['running']:
        return False

    global LAST_POSITION_STATE, LAST_CLOSE_TIMESTAMP
    LAST_POSITION_STATE = {
        'symbol': BOT_STATE['symbol'],
        'size': 0,
        'entry_price': 0
    }

    LAST_CLOSE_TIMESTAMP = 0.0

    BOT_STATE['stop_at_win']          = False
    BOT_STATE['stop_at_max_step']     = False
    BOT_STATE['force_stop']           = False
    BOT_STATE['session_start_time']   = datetime.now().isoformat()
    BOT_STATE['session_total_pnl']    = 0.0
    BOT_STATE['order_completed']      = True
    BOT_STATE['last_placed_order_id'] = None
    log_system(f"Session started at {BOT_STATE['session_start_time']}")

    BOT_STATE['running'] = True
    BOT_STATE['thread']  = threading.Thread(target=auto_trading_bot_main, daemon=True)
    BOT_STATE['thread'].start()
    return True


def stop_auto_trading_bot():
    if not BOT_STATE['running']:
        return False
    BOT_STATE['running'] = False
    if BOT_STATE['thread']:
        BOT_STATE['thread'].join(timeout=5)

    if BOT_STATE['session_start_time']:
        log_system(f"Session ended. Final P&L: ${BOT_STATE['session_total_pnl']:.2f}")

    BOT_STATE['session_start_time'] = None
    BOT_STATE['session_total_pnl']  = 0.0

    # NEW: user ne UI se explicitly stop kiya - permanently mark as "not
    # supposed to be running" so a future process restart doesn't auto-resume.
    save_session_active_flag(False)
    return True


def clear_stuck_trade_result():
    global LAST_TRADE_RESULT
    log_system("CLEARING STUCK TRADE RESULT...")
    LAST_TRADE_RESULT = {
        'profit_loss': None,
        'timestamp': None,
        'lot_used': None,
        'processed': False
    }
    BOT_STATE['order_completed']      = True
    BOT_STATE['last_placed_order_id'] = None
    log_system("Trade result cleared")
    return True


def reconcile_stuck_trades_from_database():
    global LAST_TRADE_RESULT
    try:
        query = '''
            SELECT symbol, side, entry_price, exit_price, quantity, pnl,
                   entry_time, exit_time, created_at
            FROM closed_positions
            WHERE exit_time IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 3
        '''
        recent_trades = execute_mysql_query(query, fetch_all=True)

        if not recent_trades:
            return

        if (LAST_TRADE_RESULT['profit_loss'] is not None and
                not LAST_TRADE_RESULT['processed']):

            for trade in recent_trades:
                db_pnl = trade['pnl']
                if abs(float(db_pnl) - LAST_TRADE_RESULT['profit_loss']) < 0.01:
                    log_system(f"Matched trade in DB: PnL={db_pnl}. Auto-clearing.")
                    clear_stuck_trade_result()
                    return

            log_system("No match - AUTO-CLEARING to unstick bot")
            clear_stuck_trade_result()

    except Exception as e:
        log_error(f"Reconciliation error: {e}")
        clear_stuck_trade_result()


# ========== API ROUTES ==========
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/ping')
def ping():
    return jsonify({
        'status': 'alive',
        'timestamp': datetime.now().isoformat(),
        'service': 'delta-trading-bot',
        'uptime': 'running'
    })


@app.route('/api/system-ip', methods=['GET'])
def get_system_ip():
    try:
        import socket
        try:
            public_ip = requests.get('https://ipinfo.io/ip', timeout=5).text.strip()
        except:
            public_ip = "Unknown"
        try:
            hostname = socket.gethostname()
            local_ip = socket.gethostbyname(hostname)
        except:
            local_ip = "Unknown"
        return jsonify({
            'success': True,
            'public_ip': public_ip,
            'local_ip': local_ip,
            'port': 8090
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error getting IP: {str(e)}'}), 500


@app.route('/api/system-resources', methods=['GET'])
def get_system_resources():
    """Process memory + log sizes + MySQL storage — read-only, no trading impact."""
    try:
        mem_mb = _get_process_rss_mb()
        mem_limit_mb = int(os.getenv('RENDER_MEMORY_LIMIT_MB', '512'))
        warn_mb = int(os.getenv('MEMORY_WARN_MB', '400'))
        mysql_quota_mb = float(os.getenv('MYSQL_QUOTA_MB', '256'))
        db_stats = get_mysql_storage_stats()
        used_mb = db_stats.get('used_mb')
        return jsonify({
            'success': True,
            'memory': {
                'rss_mb': mem_mb,
                'limit_mb': mem_limit_mb,
                'warn_mb': warn_mb,
                'percent': round((mem_mb / mem_limit_mb) * 100, 1) if mem_mb is not None else None,
                'status': (
                    'critical' if mem_mb is not None and mem_mb >= warn_mb
                    else 'ok' if mem_mb is not None
                    else 'unknown'
                ),
            },
            'logs': {
                'trade_log_mb': _file_size_mb('trade.log'),
                'server_log_mb': _file_size_mb('server.log'),
                'max_mb': round(LOG_MAX_BYTES / (1024 * 1024), 1),
            },
            'mysql': {
                'used_mb': used_mb,
                'quota_mb': mysql_quota_mb,
                'percent': round((used_mb / mysql_quota_mb) * 100, 1) if used_mb is not None and mysql_quota_mb > 0 else None,
                'table_count': db_stats.get('table_count', 0),
                'tables': db_stats.get('tables', []),
                'error': db_stats.get('error'),
            },
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/bots', methods=['GET'])
def api_list_bots():
    """List strategy bots for UI — does not load strategy modules into RAM."""
    try:
        return jsonify({
            'success': True,
            'bots': bot_manager.list_bots(),
            'active_bot': bot_manager.get_active_bot_id(),
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/start-bot', methods=['POST'])
def start_bot():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No data provided'}), 400

        leverage   = data.get('leverage', 10)
        tp_percent = data.get('tp_percent', 2.0)
        sl_percent = data.get('sl_percent', 1.0)
        symbol     = data.get('symbol', 'PAXGUSD')
        strategy_bot = str(data.get('strategy_bot') or data.get('bot_id') or 'bot1').strip().lower()

        # Load ONLY the chosen strategy file into RAM (others stay unloaded)
        ok_bot, bot_msg, bot_meta = bot_manager.activate_bot(strategy_bot)
        if not ok_bot:
            return jsonify({'success': False, 'message': bot_msg}), 400
        log_system(f"STRATEGY BOT ACTIVATED | {bot_msg}")

        # Optional: allow start lot / streak on start; otherwise use saved LOT_STEPS
        if data.get('start_lot') is not None or data.get('streak') is not None:
            start_lot = data.get('start_lot', LOT_STEPS_META['start_lot'])
            streak = data.get('streak', LOT_STEPS_META['streak'])
            ok, result = apply_lot_steps_config(start_lot, streak)
            if not ok:
                return jsonify({'success': False, 'message': result}), 400
            save_lot_steps_config_to_db(result)
        max_steps  = max(LOT_STEPS.keys())

        if not isinstance(leverage, int) or leverage < 1 or leverage > 200:
            return jsonify({'success': False, 'message': 'Leverage must be integer between 1-200'}), 400
        if not isinstance(tp_percent, (int, float)) or tp_percent < 0.01 or tp_percent > 50:
            return jsonify({'success': False, 'message': 'TP percent must be between 0.01-50'}), 400
        if not isinstance(sl_percent, (int, float)) or sl_percent < 0.01 or sl_percent > 50:
            return jsonify({'success': False, 'message': 'SL percent must be between 0.01-50'}), 400
        if not isinstance(symbol, str) or len(symbol) < 1 or len(symbol) > 20:
            return jsonify({'success': False, 'message': 'Symbol must be string between 1-20 characters'}), 400
        symbol = symbol.upper()
        if symbol not in ('PAXGUSD', 'XAUTUSD'):
            return jsonify({'success': False, 'message': 'Invalid symbol. Valid: [\'PAXGUSD\', \'XAUTUSD\']'}), 400

        BOT_STATE['leverage']   = leverage
        BOT_STATE['tp_percent'] = float(tp_percent)
        BOT_STATE['sl_percent'] = float(sl_percent)
        BOT_STATE['max_steps']  = max_steps
        BOT_STATE['base_lot']   = LOT_STEPS[1]
        BOT_STATE['symbol']     = symbol
        save_bot_config_to_db(get_bot_config())

        # FIX: make sure WS engine is (re)subscribed to THIS symbol before
        # we even check for an existing position on it.
        ensure_ws_symbol_sync(BOT_STATE['symbol'])

        log_system("CHECKING FOR EXISTING LIVE POSITION...")
        product_id            = get_product_id(symbol)
        has_existing_position = False

        if product_id:
            current_pos = check_position_realtime(product_id, expected_symbol=BOT_STATE['symbol'])
            if abs(current_pos.get('size', 0)) > 0.001:
                has_existing_position = True
                current_lot   = abs(current_pos.get('size', 0))
                detected_step = detect_current_step_from_lot(current_lot)

                log_system("EXISTING POSITION FOUND — loading DB state for step continuity...")
                load_bot_state_from_db()

                BOT_STATE['current_step'] = detected_step
                BOT_STATE['current_lot']  = current_lot
                log_system(f"EXISTING POSITION: {current_lot} lots = Step {detected_step}")
            else:
                log_system("No existing position — FRESH START at Step 1 (DB state ignored)")
                BOT_STATE['current_step'] = 1
                BOT_STATE['current_lot']  = LOT_STEPS[1]
        else:
            log_system("Could not check position (no product_id) — defaulting to Step 1")
            BOT_STATE['current_step'] = 1
            BOT_STATE['current_lot']  = LOT_STEPS[1]

        log_system(
            f"BOT STARTING: strategy={strategy_bot}, Step={BOT_STATE['current_step']}, "
            f"Lot={BOT_STATE['current_lot']}, TP={BOT_STATE['tp_percent']}%, SL={BOT_STATE['sl_percent']}%"
        )

        if start_auto_trading_bot():
            return jsonify({
                'success': True,
                'message': f"Bot started successfully ({bot_meta.get('name', strategy_bot)})",
                'current_step': BOT_STATE['current_step'],
                'current_lot': BOT_STATE['current_lot'],
                'has_existing_position': has_existing_position,
                'strategy_bot': strategy_bot,
                'strategy_name': bot_meta.get('name') if bot_meta else strategy_bot,
            })
        return jsonify({'success': False, 'message': 'Bot already running'})

    except Exception as e:
        return jsonify({'success': False, 'message': f'Invalid request: {str(e)}'}), 400


@app.route('/api/force-stop', methods=['POST'])
def force_stop_bot():
    try:
        BOT_STATE['force_stop'] = True
        log_system("FORCE STOP ACTIVATED")
        return jsonify({'success': True, 'message': 'Force stop activated'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/api/stop-at-win', methods=['POST'])
def stop_at_win():
    try:
        BOT_STATE['stop_at_win'] = True
        BOT_STATE['force_stop']  = False
        log_system("STOP AT WIN ACTIVATED")
        return jsonify({'success': True, 'message': 'Stop at win activated'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/api/stop-at-max-streak', methods=['POST'])
def stop_at_max_streak():
    try:
        BOT_STATE['stop_at_max_step'] = True
        BOT_STATE['force_stop']       = False
        BOT_STATE['stop_at_win']      = False
        log_system("STOP AT MAX STEP ACTIVATED")
        return jsonify({'success': True, 'message': 'Stop at max step activated'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/api/clear-stop-conditions', methods=['POST'])
def clear_stop_conditions():
    try:
        BOT_STATE['stop_at_win']      = False
        BOT_STATE['stop_at_max_step'] = False
        BOT_STATE['force_stop']       = False
        log_system("STOP CONDITIONS CLEARED")
        return jsonify({'success': True, 'message': 'Stop conditions cleared'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/api/update-symbol', methods=['POST'])
def update_symbol():
    try:
        data       = request.get_json()
        new_symbol = data.get('symbol')
        if not new_symbol:
            return jsonify({'success': False, 'message': 'Symbol is required'}), 400
        valid_symbols = ['PAXGUSD', 'XAUTUSD']
        if new_symbol not in valid_symbols:
            return jsonify({'success': False, 'message': f'Invalid symbol. Valid: {valid_symbols}'}), 400
        old_symbol          = BOT_STATE['symbol']
        BOT_STATE['symbol'] = new_symbol
        save_bot_config_to_db(get_bot_config())
        print(f"📊 Symbol updated: {old_symbol} → {new_symbol}")
        # FIX: force WS engine to resync to the new symbol so fills/positions
        # for the OLD symbol never get attributed to the NEW one (and vice versa).
        ensure_ws_symbol_sync(new_symbol)
        return jsonify({'success': True, 'message': f'Symbol updated to {new_symbol}'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/api/stop-bot', methods=['POST'])
def stop_bot():
    try:
        if stop_auto_trading_bot():
            bot_manager.deactivate_bot()  # unload strategy — free RAM
            log_system("STRATEGY BOT UNLOADED")
            return jsonify({'success': True, 'message': 'Bot stopped successfully'})
        return jsonify({'success': False, 'message': 'Bot was not running'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/api/reset-bot-state', methods=['POST'])
def reset_bot_state():
    try:
        global LAST_TRADE_RESULT, USED_FILL_IDS, LAST_CLOSE_TIMESTAMP

        clear_bot_state_from_db()

        # Also clear recent_fills_cache for the current symbol
        try:
            execute_mysql_query(
                "DELETE FROM recent_fills_cache WHERE symbol = %s",
                (BOT_STATE['symbol'],),
                commit=True
            )
            log_system("recent_fills_cache cleared for symbol")
        except Exception as e:
            log_error(f"Could not clear recent_fills_cache: {e}")

        BOT_STATE['current_step']         = 1
        BOT_STATE['current_lot']          = LOT_STEPS[1]
        BOT_STATE['last_result']          = None
        BOT_STATE['order_completed']      = True
        BOT_STATE['last_placed_order_id'] = None
        BOT_STATE.pop('last_pnl', None)

        LAST_TRADE_RESULT = {
            'profit_loss': None,
            'timestamp': None,
            'lot_used': None,
            'processed': False
        }

        with USED_FILL_IDS_LOCK:
            USED_FILL_IDS = set()

        with PROCESSED_ORDER_IDS_LOCK:
            PROCESSED_ORDER_IDS.clear()

        LAST_CLOSE_TIMESTAMP = 0.0

        log_system("Bot state fully reset - Step 1 on next start")
        return jsonify({
            'success': True,
            'message': 'Bot state reset to Step 1',
            'current_step': 1,
            'current_lot': LOT_STEPS[1]
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/api/bot-status', methods=['GET'])
def get_bot_status():
    try:
        current_step = BOT_STATE['current_step']
        current_lot  = BOT_STATE['current_lot']

        next_step = current_step + 1
        if next_step > BOT_STATE['max_steps']:
            next_step = 1
        next_lot = LOT_STEPS[next_step]

        current_lot_size    = LOT_SIZES.get(BOT_STATE['symbol'], 10)
        elapsed_since_close = time.time() - LAST_CLOSE_TIMESTAMP if LAST_CLOSE_TIMESTAMP > 0 else None
        cooldown_remaining  = max(0.0, COOLDOWN_SECONDS - elapsed_since_close) if elapsed_since_close is not None else 0.0

        return jsonify({
            'success': True,
            'status': {
                'running': BOT_STATE['running'],
                'current_step': current_step,
                'current_lot': current_lot,
                'next_step': next_step,
                'next_lot': next_lot,
                'max_steps': BOT_STATE['max_steps'],
                'last_result': BOT_STATE['last_result'],
                'base_lot': BOT_STATE['base_lot'],
                'leverage': BOT_STATE['leverage'],
                'tp_percent': BOT_STATE['tp_percent'],
                'sl_percent': BOT_STATE['sl_percent'],
                'symbol': BOT_STATE['symbol'],
                'stop_at_win': BOT_STATE['stop_at_win'],
                'stop_at_max_step': BOT_STATE['stop_at_max_step'],
                'force_stop': BOT_STATE['force_stop'],
                'current_lot_size': current_lot_size,
                'session_start_time': BOT_STATE['session_start_time'],
                'session_total_pnl': BOT_STATE['session_total_pnl'],
                'lot_steps': dict(LOT_STEPS),
                'start_lot': LOT_STEPS_META['start_lot'],
                'streak': LOT_STEPS_META['streak'],
                'cooldown_remaining_seconds': round(cooldown_remaining, 1),
                'cooldown_seconds': COOLDOWN_SECONDS,
                'order_completed': BOT_STATE['order_completed'],
                'last_placed_order_id': BOT_STATE.get('last_placed_order_id'),
                'strategy_bot': bot_manager.get_active_bot_id(),
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/api/clear-stuck-result', methods=['POST'])
def clear_stuck_result():
    try:
        if clear_stuck_trade_result():
            return jsonify({'success': True, 'message': 'Stuck trade result cleared'})
        else:
            return jsonify({'success': False, 'message': 'Failed to clear stuck result'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'}), 500


@app.route('/api/logs', methods=['GET'])
def get_logs():
    try:
        limit = int(request.args.get('limit', 100))
        log_file = "trade.log"
        if not os.path.exists(log_file):
            return jsonify({'success': True, 'logs': 'No trade logs found yet.', 'total_lines': 0})
        last_lines, approx_total = _tail_read_lines(log_file, limit=limit)
        return jsonify({
            'success': True,
            'logs': "\n".join(last_lines) + ("\n" if last_lines else ""),
            'total_lines': approx_total,
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/wallet-balance', methods=['GET'])
def wallet_balance():
    balance_data = get_wallet_balance()
    return jsonify(balance_data)


@app.route('/api/trade-history', methods=['GET'])
def trade_history():
    page     = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    offset   = (page - 1) * per_page

    try:
        count_result = execute_mysql_query('SELECT COUNT(*) as total FROM closed_positions', fetch_one=True)
        total_trades = count_result['total'] if count_result else 0

        query = '''
            SELECT id, symbol, side, entry_price, exit_price, quantity,
                   pnl, entry_time, exit_time, trade_decisions,
                   entry_order_id, exit_order_id, had_break_even
            FROM closed_positions
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        '''
        trades = execute_mysql_query(query, (per_page, offset), fetch_all=True)

        return jsonify({
            'trades': [{
                'symbol':      t['symbol'],
                'side':        t['side'],
                'entry_price': float(t['entry_price']) if t['entry_price'] else None,
                'exit_price':  float(t['exit_price'])  if t['exit_price']  else None,
                'quantity':    float(t['quantity'])     if t['quantity']    else None,
                'pnl':         float(t['pnl'])          if t['pnl']         else None,
                'entry_time':  t['entry_time'],
                'exit_time':   t['exit_time'],
                'trade_decisions': t.get('trade_decisions', '—'),
                'entry_order_id': t.get('entry_order_id') or '—',
                'exit_order_id':  t.get('exit_order_id')  or '—',
                'had_break_even': bool(t.get('had_break_even')),
                'id': str(t['id']) if t.get('id') else f"trade_{hash(t['entry_time'] + t['symbol'])}"
            } for t in trades],
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total': total_trades,
                'total_pages': (total_trades + per_page - 1) // per_page,
                'has_next': page * per_page < total_trades,
                'has_prev': page > 1
            }
        })

    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'trades': [], 'pagination': None})


@app.route('/api/signal-scores-history', methods=['GET'])
def signal_scores_history():
    page     = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 10))
    offset   = (page - 1) * per_page
    try:
        count_result = execute_mysql_query(
            'SELECT COUNT(*) as total FROM signal_scores_history', fetch_one=True
        )
        total_rows = count_result['total'] if count_result else 0

        query = '''
            SELECT order_id, symbol, position, lot_size,
                   tf_1m, tf_5m, tf_15m, tf_30m, tf_1h, tf_2h, tf_4h, tf_1d, tf_1w,
                   final_decision, entry_time
            FROM signal_scores_history
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        '''
        rows = execute_mysql_query(query, (per_page, offset), fetch_all=True)

        return jsonify({
            'success': True,
            'scores': [{
                'order_id': r['order_id'],
                'symbol': r['symbol'],
                'position': r['position'],
                'lot_size': float(r['lot_size']) if r['lot_size'] is not None else None,
                'tf_1m': r['tf_1m'], 'tf_5m': r['tf_5m'], 'tf_15m': r['tf_15m'],
                'tf_30m': r['tf_30m'], 'tf_1h': r['tf_1h'], 'tf_2h': r['tf_2h'],
                'tf_4h': r['tf_4h'], 'tf_1d': r['tf_1d'], 'tf_1w': r['tf_1w'],
                'final_decision': r['final_decision'],
                'entry_time': r['entry_time'],
            } for r in rows],
            'pagination': {
                'current_page': page,
                'per_page': per_page,
                'total': total_rows,
                'total_pages': (total_rows + per_page - 1) // per_page,
                'has_next': page * per_page < total_rows,
                'has_prev': page > 1
            }
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'scores': [], 'pagination': None})


@app.route('/api/delete-trades', methods=['POST'])
def delete_trades():
    try:
        data      = request.get_json()
        trade_ids = data.get('trade_ids', [])
        if not trade_ids:
            return jsonify({'success': False, 'message': 'No trade IDs provided'})
        numeric_ids = []
        for trade_id in trade_ids:
            try:
                if str(trade_id).startswith('trade_'):
                    continue
                numeric_ids.append(int(trade_id))
            except ValueError:
                continue
        if not numeric_ids:
            return jsonify({'success': False, 'message': 'No valid trade IDs found'})
        placeholders = ','.join(['%s'] * len(numeric_ids))
        delete_query = f'DELETE FROM closed_positions WHERE id IN ({placeholders})'
        execute_mysql_query(delete_query, numeric_ids, commit=True)
        return jsonify({'success': True, 'message': f'Successfully deleted {len(numeric_ids)} trade(s)'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})


@app.route('/api/clear-all-trades', methods=['POST'])
def clear_all_trades():
    try:
        execute_mysql_query('DELETE FROM closed_positions', commit=True)
        return jsonify({'success': True, 'message': 'Successfully cleared all trade history'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})


@app.route('/api/clear-all-signal-scores', methods=['POST'])
def clear_all_signal_scores():
    try:
        execute_mysql_query('DELETE FROM signal_scores_history', commit=True)
        return jsonify({'success': True, 'message': 'Successfully cleared all signal scores history'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error: {str(e)}'})


# =====================================================================
# SIGNAL SCORES PnL (INFO ONLY)
# Completely independent of fill-engine helpers (get_fills_page,
# find_trade_by_order_id, wait_for_trade_fills, pre-order verify, etc.).
# Runs ONLY when UI presses "See PnL".
# =====================================================================
def _ss_pnl_fetch_delta_fills(max_pages=40, page_size=50):
    """Fetch fills from Delta Exchange for signal-score PnL display only."""
    all_fills = []
    after = None
    for _ in range(max_pages):
        endpoint = f'/fills?page_size={page_size}'
        if after:
            endpoint += f'&after={after}'
        try:
            resp = make_api_request('GET', endpoint)
        except Exception as e:
            log_error(f"[SS PnL] fills fetch error: {e}")
            break
        if not resp or not resp.get('result'):
            break
        batch = resp.get('result') or []
        if not batch:
            break
        all_fills.extend(batch)
        meta = resp.get('meta') or {}
        after = meta.get('after')
        if not after:
            break
    return all_fills


def _ss_pnl_group_orders(fills):
    """Group raw fills by order_id → avg price / total size / side / timestamp."""
    order_groups = {}
    for fill in fills:
        oid = str(fill.get('order_id') or fill.get('id') or '')
        side = (fill.get('side') or '').lower()
        try:
            size = float(fill.get('size', 0) or 0)
            price = float(fill.get('price', 0) or 0)
        except (TypeError, ValueError):
            continue
        created_at = fill.get('created_at') or ''
        symbol = fill.get('product_symbol') or ''

        if not oid or size <= 0 or price <= 0:
            continue

        if oid not in order_groups:
            order_groups[oid] = {
                'order_id': oid,
                'side': side,
                'symbol': symbol,
                'total_size': 0.0,
                'total_value': 0.0,
                'avg_price': 0.0,
                'timestamp': created_at,
            }

        grp = order_groups[oid]
        grp['total_size'] += size
        grp['total_value'] += price * size
        if created_at and (not grp['timestamp'] or created_at < grp['timestamp']):
            grp['timestamp'] = created_at
        if symbol and not grp['symbol']:
            grp['symbol'] = symbol

    for grp in order_groups.values():
        if grp['total_size'] > 0:
            grp['avg_price'] = grp['total_value'] / grp['total_size']

    return order_groups


def _ss_pnl_formula(entry_side, entry_price, exit_price, lot_size, symbol):
    """Same PnL formula as trading engine (info-only copy — does not call fill helpers)."""
    lot_multiplier = LOT_SIZES.get(symbol, LOT_SIZE_DEFAULT)
    actual_quantity = float(lot_size) * float(lot_multiplier)
    if entry_side == 'buy':
        return (exit_price - (entry_price + fees)) * actual_quantity
    return ((entry_price - fees) - exit_price) * actual_quantity


def _ss_pnl_find_exit_for_entry(order_groups, entry_oid):
    """
    Given entry order_id, find its exit pair in Delta fills:
    opposite side, exit_time >= entry_time, nearest later exit.
    """
    entry = order_groups.get(str(entry_oid))
    if not entry:
        return None, None

    candidates = []
    for oid, grp in order_groups.items():
        if oid == str(entry_oid):
            continue
        if grp.get('symbol') and entry.get('symbol') and grp['symbol'] != entry['symbol']:
            continue
        if grp['side'] == entry['side']:
            continue
        if not grp['timestamp'] or not entry['timestamp']:
            continue
        if grp['timestamp'] < entry['timestamp']:
            continue
        candidates.append(grp)

    if not candidates:
        return entry, None

    candidates.sort(key=lambda g: g['timestamp'])
    return entry, candidates[0]


def _ss_pnl_position_is_live(symbol):
    """Best-effort live check for the latest signal row only (read-only)."""
    try:
        if not symbol:
            return False
        product_id = get_product_id(symbol)
        if product_id:
            pos = check_position_realtime(product_id, expected_symbol=symbol)
            if isinstance(pos, dict) and not pos.get('error') and pos.get('has_position'):
                return True
        if BOT_STATE.get('running') and not BOT_STATE.get('order_completed', True):
            if (BOT_STATE.get('symbol') or '').upper() == (symbol or '').upper():
                return True
    except Exception as e:
        log_error(f"[SS PnL] live check error: {e}")
    return False


@app.route('/api/signal-scores-pnl', methods=['GET'])
def signal_scores_pnl():
    """
    On-demand: fetch Delta fills, pair each signal_scores_history.order_id
    with its exit, compute PnL. Does not modify any fill-engine state.
    """
    try:
        rows = execute_mysql_query(
            '''
            SELECT order_id, symbol, position, lot_size, entry_time, created_at
            FROM signal_scores_history
            ORDER BY created_at DESC
            ''',
            fetch_all=True
        ) or []

        fills = _ss_pnl_fetch_delta_fills()
        order_groups = _ss_pnl_group_orders(fills)

        latest_oid = str(rows[0]['order_id']) if rows else None
        latest_symbol = rows[0]['symbol'] if rows else None

        # Pair oldest→newest so each exit is consumed once (avoids wrong pairs)
        used_exit_oids = set()
        pair_cache = {}  # entry_oid -> (entry, exit_or_None)

        chronological = sorted(
            rows,
            key=lambda r: (r.get('entry_time') or r.get('created_at') or '', str(r['order_id']))
        )
        for r in chronological:
            oid = str(r['order_id'])
            entry, exit_ord = _ss_pnl_find_exit_for_entry(order_groups, oid)
            if exit_ord and exit_ord['order_id'] in used_exit_oids:
                # find next unused opposite-side exit after entry
                exit_ord = None
                if entry:
                    for cand_oid, grp in sorted(
                        order_groups.items(),
                        key=lambda kv: kv[1].get('timestamp') or ''
                    ):
                        if cand_oid == oid:
                            continue
                        if cand_oid in used_exit_oids:
                            continue
                        if grp.get('symbol') and entry.get('symbol') and grp['symbol'] != entry['symbol']:
                            continue
                        if grp['side'] == entry['side']:
                            continue
                        if not grp['timestamp'] or not entry['timestamp']:
                            continue
                        if grp['timestamp'] < entry['timestamp']:
                            continue
                        exit_ord = grp
                        break
            if exit_ord:
                used_exit_oids.add(exit_ord['order_id'])
            pair_cache[oid] = (entry, exit_ord)

        live_flag = False
        if latest_oid:
            entry, exit_ord = pair_cache.get(latest_oid, (None, None))
            no_exit = entry is not None and exit_ord is None
            exchange_live = _ss_pnl_position_is_live(latest_symbol)
            entry_missing = entry is None and (
                BOT_STATE.get('last_placed_order_id') is not None
                and str(BOT_STATE.get('last_placed_order_id')) == latest_oid
                and not BOT_STATE.get('order_completed', True)
            )
            live_flag = bool(no_exit or exchange_live or entry_missing)

        results = {}
        for r in rows:
            oid = str(r['order_id'])
            symbol = r['symbol']
            entry, exit_ord = pair_cache.get(oid, (None, None))
            # UI-only metadata (does not affect PnL math)
            entry_time = r.get('entry_time') or r.get('created_at')

            if oid == latest_oid and live_flag:
                results[oid] = {
                    'status': 'live',
                    'pnl': None,
                    'tag': 'POSITION IS CURRENTLY LIVE',
                    'entry_order_id': oid,
                    'exit_order_id': None,
                    'entry_price': entry['avg_price'] if entry else None,
                    'exit_price': None,
                    'lot_size': float(r['lot_size']) if r.get('lot_size') is not None else None,
                    'entry_time': entry_time,
                    'had_break_even': False,
                }
                continue

            if not entry:
                results[oid] = {
                    'status': 'not_found',
                    'pnl': None,
                    'tag': 'fills not found',
                    'entry_order_id': oid,
                    'exit_order_id': None,
                    'entry_price': None,
                    'exit_price': None,
                    'lot_size': float(r['lot_size']) if r.get('lot_size') is not None else None,
                    'entry_time': entry_time,
                    'had_break_even': False,
                }
                continue

            if not exit_ord:
                results[oid] = {
                    'status': 'pending',
                    'pnl': None,
                    'tag': 'exit pending',
                    'entry_order_id': oid,
                    'exit_order_id': None,
                    'entry_price': entry['avg_price'],
                    'exit_price': None,
                    'lot_size': float(r['lot_size']) if r.get('lot_size') is not None else None,
                    'entry_time': entry_time,
                    'had_break_even': False,
                }
                continue

            lot_size = min(entry['total_size'], exit_ord['total_size'])
            pnl = _ss_pnl_formula(
                entry['side'], entry['avg_price'], exit_ord['avg_price'], lot_size, symbol
            )
            results[oid] = {
                'status': 'closed',
                'pnl': round(float(pnl), 8),
                'tag': 'PROFIT' if pnl > 0 else 'LOSS',
                'entry_order_id': oid,
                'exit_order_id': exit_ord['order_id'],
                'entry_price': entry['avg_price'],
                'exit_price': exit_ord['avg_price'],
                'entry_side': entry['side'],
                'lot_size': lot_size,
                'entry_time': entry_time,
                'had_break_even': False,
            }

        # UI-only: attach had_break_even from closed_positions (no PnL / pairing change)
        try:
            closed_oids = [
                oid for oid, info in results.items()
                if info.get('status') == 'closed'
            ]
            if closed_oids:
                placeholders = ','.join(['%s'] * len(closed_oids))
                be_rows = execute_mysql_query(
                    f'''
                    SELECT entry_order_id, had_break_even
                    FROM closed_positions
                    WHERE entry_order_id IN ({placeholders})
                    ''',
                    tuple(closed_oids),
                    fetch_all=True,
                ) or []
                be_map = {
                    str(br['entry_order_id']): bool(br.get('had_break_even'))
                    for br in be_rows
                    if br.get('entry_order_id') is not None
                }
                for oid, info in results.items():
                    if oid in be_map:
                        info['had_break_even'] = be_map[oid]
        except Exception as be_err:
            log_error(f"[SS PnL] BE enrich (UI only) error: {be_err}")

        return jsonify({
            'success': True,
            'fills_fetched': len(fills),
            'orders_grouped': len(order_groups),
            'pnl_by_order_id': results,
        })
    except Exception as e:
        log_error(f"[SS PnL] endpoint error: {e}")
        return jsonify({'success': False, 'message': str(e), 'pnl_by_order_id': {}})


@app.route('/api/guardian-config', methods=['GET'])
def api_get_guardian_config():
    """TP/SL guardian + break-even settings. Independent of bot ON/OFF."""
    try:
        return jsonify({'success': True, 'config': get_guardian_config()})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/guardian-config', methods=['POST'])
def api_save_guardian_config():
    """Save guardian settings permanently (survives bot stop/start + process restart)."""
    try:
        data = request.get_json() or {}
        ok, result = apply_guardian_config(data)
        if not ok:
            return jsonify({'success': False, 'message': result}), 400
        return jsonify({'success': True, 'message': 'Guardian settings saved', 'config': result})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/lot-steps-config', methods=['GET'])
def api_get_lot_steps_config():
    """Start lot + streak → martingale LOT_STEPS. Independent of bot ON/OFF."""
    try:
        return jsonify({'success': True, 'config': get_lot_steps_config()})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/lot-steps-config', methods=['POST'])
def api_save_lot_steps_config():
    """Rebuild LOT_STEPS from start_lot + streak. Does not change win/loss reset logic."""
    try:
        data = request.get_json() or {}
        start_lot = data.get('start_lot', LOT_STEPS_META['start_lot'])
        streak = data.get('streak', LOT_STEPS_META['streak'])
        ok, result = apply_lot_steps_config(start_lot, streak)
        if not ok:
            return jsonify({'success': False, 'message': result}), 400
        save_lot_steps_config_to_db(result)
        return jsonify({'success': True, 'message': 'Lot steps saved', 'config': result})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/bot-config', methods=['GET'])
def api_get_bot_config():
    """Trading pair, leverage, TP/SL — persisted in MySQL."""
    try:
        return jsonify({'success': True, 'config': get_bot_config()})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/bot-config', methods=['POST'])
def api_save_bot_config():
    """Save bot UI config without starting engine."""
    try:
        data = request.get_json() or {}
        ok, result = apply_bot_config(data)
        if not ok:
            return jsonify({'success': False, 'message': result}), 400
        return jsonify({'success': True, 'message': 'Bot config saved', 'config': result})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ========== DECISION TIMEFRAMES (UI-editable, DB-persisted) ==========
# Which TFs must align for final BUY/SELL. Bot ON/OFF se reset NAHI.
# Sirf generate_smart_signal decision gate pe lagta hai — placement/bot loop untouched.
# Default = current hardcoded confluence (4H + 15M + 30M + 1H + 2H).
DECISION_TF_KEYS = ('1m', '5m', '15m', '30m', '1h', '2h', '4h', '1d')
# Highest → lowest (master = first enabled with BUY/SELL)
DECISION_TF_PRIORITY = ('1d', '4h', '2h', '1h', '30m', '15m', '5m', '1m')
DECISION_TF_DEFAULTS = {
    '1m': False,
    '5m': False,
    '15m': True,
    '30m': True,
    '1h': True,
    '2h': True,
    '4h': True,
    '1d': False,
}
DECISION_TF_CONFIG = dict(DECISION_TF_DEFAULTS)
_decision_tf_lock = Lock()


def get_decision_tf_config():
    with _decision_tf_lock:
        return dict(DECISION_TF_CONFIG)


def save_decision_tf_config_to_db(cfg=None):
    try:
        data = cfg if cfg is not None else get_decision_tf_config()
        payload = {k: bool(data.get(k, False)) for k in DECISION_TF_KEYS}
        payload['saved_at'] = datetime.now().isoformat()
        upsert_query = '''
            INSERT INTO bot_state (state_key, state_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE state_value = VALUES(state_value), updated_at = NOW()
        '''
        execute_mysql_query(upsert_query, ('decision_tf_config', json.dumps(payload)), commit=True)
        enabled = [k.upper() for k in DECISION_TF_PRIORITY if payload.get(k)]
        log_system(f"DECISION TF SAVED | enabled={enabled or ['(none)']}")
        return True
    except Exception as e:
        log_error(f"Saving decision TF config: {e}")
        return False


def load_decision_tf_config_from_db():
    global DECISION_TF_CONFIG
    try:
        result = execute_mysql_query(
            "SELECT state_value FROM bot_state WHERE state_key = %s",
            ('decision_tf_config',),
            fetch_one=True
        )
        if not result or not result.get('state_value'):
            log_system("No saved decision TF config — using defaults, writing to DB")
            save_decision_tf_config_to_db(DECISION_TF_DEFAULTS)
            return False

        saved = json.loads(result['state_value'])
        with _decision_tf_lock:
            for key in DECISION_TF_KEYS:
                if key in saved:
                    val = saved[key]
                    if isinstance(val, str):
                        DECISION_TF_CONFIG[key] = val.strip().lower() in ('1', 'true', 'yes', 'y', 'on')
                    else:
                        DECISION_TF_CONFIG[key] = bool(val)
        enabled = [k.upper() for k in DECISION_TF_PRIORITY if DECISION_TF_CONFIG.get(k)]
        log_system(f"DECISION TF LOADED | enabled={enabled or ['(none)']}")
        return True
    except Exception as e:
        log_error(f"Loading decision TF config: {e}")
        return False


def apply_decision_tf_config(data: dict):
    """Validate + apply UI payload. At least one TF required. Returns (ok, cfg_or_msg)."""
    try:
        new_cfg = {}
        for key in DECISION_TF_KEYS:
            val = data.get(key, DECISION_TF_CONFIG.get(key, False))
            if isinstance(val, str):
                new_cfg[key] = val.strip().lower() in ('1', 'true', 'yes', 'y', 'on')
            else:
                new_cfg[key] = bool(val)
        if not any(new_cfg.values()):
            return False, 'Select at least one timeframe for final decision'
        with _decision_tf_lock:
            DECISION_TF_CONFIG.update(new_cfg)
        save_decision_tf_config_to_db(new_cfg)
        return True, new_cfg
    except Exception as e:
        return False, f'Invalid values: {e}'


def _configure_bot_runtime():
    """Give strategy bots access to app helpers without importing app.py."""
    bot_manager.set_runtime_factory(lambda: {
        'get_symbol': lambda: BOT_STATE.get('symbol', 'PAXGUSD'),
        'make_api_request': make_api_request,
        'get_decision_tf_config': get_decision_tf_config,
        'decision_tf_priority': DECISION_TF_PRIORITY,
    })


_configure_bot_runtime()


@app.route('/api/decision-tf-config', methods=['GET'])
def api_get_decision_tf_config():
    try:
        return jsonify({'success': True, 'config': get_decision_tf_config()})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@app.route('/api/decision-tf-config', methods=['POST'])
def api_save_decision_tf_config():
    try:
        data = request.get_json() or {}
        ok, result = apply_decision_tf_config(data)
        if not ok:
            return jsonify({'success': False, 'message': result}), 400
        return jsonify({'success': True, 'message': 'Decision timeframes saved', 'config': result})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ========== TP/SL + BREAK-EVEN CONFIG (UI-editable, DB-persisted) ==========
# Defaults — UI/API se change hote hain. Bot ON/OFF se reset NAHI hote.
# Sirf jab UI se Save ho tab update. Process restart pe DB se load.
GUARDIAN_CONFIG_DEFAULTS = {
    'live_tp_percentage': 2.0,
    'live_sl_percentage': 1.0,
    'liquidation_protection': 'Y',
    'liquidation_buffer': 0.2,
    'break_even_enabled': True,
    'break_even_trigger_percent': 0.5,   # TP ka kitna % travel hone pe BE activate
    'break_even_trigger_buffer': 4.0,    # Half-TP se itna pehle trigger (USD)
    'break_even_profit_offset': 3.5,     # Entry ke upar/niche SL offset (USD) — BE ONLY, fees se ab alag
    'break_even_retry_same_step': False, # BE SL hit → same step/lot (no double); PnL history unchanged
    'trading_fees': 3.5,                 # Per-side fee jo PnL formula me use hoti hai (replaces hardcoded `fees`)
    # UI-only: flip final BUY↔SELL at order time. Not in bot scripts — works for any selected bot.
    # Signal Scores History still stores the bot's original decision (never reversed).
    'reverse_decision': False,
}
GUARDIAN_CONFIG = dict(GUARDIAN_CONFIG_DEFAULTS)
_guardian_config_lock = Lock()


def get_guardian_config():
    with _guardian_config_lock:
        return dict(GUARDIAN_CONFIG)


def save_guardian_config_to_db(cfg=None):
    try:
        data = cfg if cfg is not None else get_guardian_config()
        payload = dict(data)
        payload['saved_at'] = datetime.now().isoformat()
        upsert_query = '''
            INSERT INTO bot_state (state_key, state_value)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE state_value = VALUES(state_value), updated_at = NOW()
        '''
        execute_mysql_query(upsert_query, ('guardian_config', json.dumps(payload)), commit=True)
        log_system(
            f"GUARDIAN CONFIG SAVED | TP={payload.get('live_tp_percentage')}% "
            f"SL={payload.get('live_sl_percentage')}% "
            f"Liq={payload.get('liquidation_protection')} "
            f"Buf={payload.get('liquidation_buffer')} "
            f"BE={payload.get('break_even_enabled')} "
            f"BE%={payload.get('break_even_trigger_percent')} "
            f"BE_buf={payload.get('break_even_trigger_buffer')} "
            f"BE_offset={payload.get('break_even_profit_offset')} "
            f"BE_retry={payload.get('break_even_retry_same_step')} "
            f"Fees={payload.get('trading_fees')} "
            f"Reverse={payload.get('reverse_decision')}"
        )
        return True
    except Exception as e:
        log_error(f"Saving guardian config: {e}")
        return False


def load_guardian_config_from_db():
    global GUARDIAN_CONFIG, fees
    try:
        result = execute_mysql_query(
            "SELECT state_value FROM bot_state WHERE state_key = %s",
            ('guardian_config',),
            fetch_one=True
        )
        if not result or not result.get('state_value'):
            log_system("No saved guardian config — using defaults, writing to DB")
            save_guardian_config_to_db(GUARDIAN_CONFIG_DEFAULTS)
            fees = GUARDIAN_CONFIG_DEFAULTS['trading_fees']
            return False

        saved = json.loads(result['state_value'])
        with _guardian_config_lock:
            for key in GUARDIAN_CONFIG_DEFAULTS:
                if key in saved:
                    GUARDIAN_CONFIG[key] = saved[key]
            # type-normalize
            GUARDIAN_CONFIG['live_tp_percentage'] = float(GUARDIAN_CONFIG['live_tp_percentage'])
            GUARDIAN_CONFIG['live_sl_percentage'] = float(GUARDIAN_CONFIG['live_sl_percentage'])
            GUARDIAN_CONFIG['liquidation_protection'] = str(GUARDIAN_CONFIG['liquidation_protection']).strip().upper()
            GUARDIAN_CONFIG['liquidation_buffer'] = float(GUARDIAN_CONFIG['liquidation_buffer'])
            GUARDIAN_CONFIG['break_even_enabled'] = bool(GUARDIAN_CONFIG['break_even_enabled'])
            GUARDIAN_CONFIG['break_even_trigger_percent'] = float(GUARDIAN_CONFIG['break_even_trigger_percent'])
            GUARDIAN_CONFIG['break_even_trigger_buffer'] = float(GUARDIAN_CONFIG['break_even_trigger_buffer'])
            GUARDIAN_CONFIG['break_even_profit_offset'] = float(GUARDIAN_CONFIG['break_even_profit_offset'])
            GUARDIAN_CONFIG['break_even_retry_same_step'] = bool(
                GUARDIAN_CONFIG.get('break_even_retry_same_step', False)
            )
            GUARDIAN_CONFIG['trading_fees'] = float(GUARDIAN_CONFIG.get('trading_fees', 3.5))
            rev = GUARDIAN_CONFIG.get('reverse_decision', False)
            if isinstance(rev, str):
                GUARDIAN_CONFIG['reverse_decision'] = rev.strip().lower() in (
                    '1', 'true', 'yes', 'y', 'on'
                )
            else:
                GUARDIAN_CONFIG['reverse_decision'] = bool(rev)

        # PnL formula (exit-(entry+fees)) bilkul unchanged hai — sirf fees value ka source
        # hardcoded 3.5 se hatakar is config field pe move kiya hai.
        fees = GUARDIAN_CONFIG['trading_fees']

        log_system(
            f"GUARDIAN CONFIG LOADED | TP={GUARDIAN_CONFIG['live_tp_percentage']}% "
            f"SL={GUARDIAN_CONFIG['live_sl_percentage']}% "
            f"BE_offset={GUARDIAN_CONFIG['break_even_profit_offset']} "
            f"BE_retry={GUARDIAN_CONFIG['break_even_retry_same_step']} "
            f"Fees={GUARDIAN_CONFIG['trading_fees']} "
            f"Reverse={GUARDIAN_CONFIG['reverse_decision']}"
        )
        return True
    except Exception as e:
        log_error(f"Loading guardian config: {e}")
        return False


def apply_guardian_config(data: dict):
    """Validate + apply UI/API payload into memory and DB. Returns (ok, message_or_cfg)."""
    global fees
    try:
        tp = float(data.get('live_tp_percentage', GUARDIAN_CONFIG['live_tp_percentage']))
        sl = float(data.get('live_sl_percentage', GUARDIAN_CONFIG['live_sl_percentage']))
        liq = str(data.get('liquidation_protection', GUARDIAN_CONFIG['liquidation_protection'])).strip().upper()
        liq_buf = float(data.get('liquidation_buffer', GUARDIAN_CONFIG['liquidation_buffer']))
        be_en = data.get('break_even_enabled', GUARDIAN_CONFIG['break_even_enabled'])
        if isinstance(be_en, str):
            be_en = be_en.strip().lower() in ('1', 'true', 'yes', 'y', 'on')
        else:
            be_en = bool(be_en)
        be_pct = float(data.get('break_even_trigger_percent', GUARDIAN_CONFIG['break_even_trigger_percent']))
        be_buf = float(data.get('break_even_trigger_buffer', GUARDIAN_CONFIG['break_even_trigger_buffer']))
        be_off = float(data.get('break_even_profit_offset', GUARDIAN_CONFIG['break_even_profit_offset']))
        be_retry = data.get('break_even_retry_same_step', GUARDIAN_CONFIG['break_even_retry_same_step'])
        if isinstance(be_retry, str):
            be_retry = be_retry.strip().lower() in ('1', 'true', 'yes', 'y', 'on')
        else:
            be_retry = bool(be_retry)
        trading_fees = float(data.get('trading_fees', GUARDIAN_CONFIG.get('trading_fees', 3.5)))
        rev = data.get('reverse_decision', GUARDIAN_CONFIG.get('reverse_decision', False))
        if isinstance(rev, str):
            rev = rev.strip().lower() in ('1', 'true', 'yes', 'y', 'on')
        else:
            rev = bool(rev)

        if not (0.01 <= tp <= 50):
            return False, 'TP % must be between 0.01 and 50'
        if not (0.01 <= sl <= 50):
            return False, 'SL % must be between 0.01 and 50'
        if liq not in ('Y', 'N'):
            return False, 'Liquidation protection must be Y or N'
        if not (0 <= liq_buf <= 50):
            return False, 'Liquidation buffer must be between 0 and 50'
        if not (0.05 <= be_pct <= 1.0):
            return False, 'BE trigger % must be between 0.05 and 1.0'
        if not (0 <= be_buf <= 500):
            return False, 'BE trigger buffer must be between 0 and 500'
        if not (0 <= be_off <= 500):
            return False, 'BE profit offset must be between 0 and 500'
        if not (0 <= trading_fees <= 50):
            return False, 'Trading fees must be between 0 and 50'

        new_cfg = {
            'live_tp_percentage': tp,
            'live_sl_percentage': sl,
            'liquidation_protection': liq,
            'liquidation_buffer': liq_buf,
            'break_even_enabled': be_en,
            'break_even_trigger_percent': be_pct,
            'break_even_trigger_buffer': be_buf,
            'break_even_profit_offset': be_off,
            'break_even_retry_same_step': be_retry,
            'trading_fees': trading_fees,
            'reverse_decision': rev,
        }
        with _guardian_config_lock:
            GUARDIAN_CONFIG.update(new_cfg)
        save_guardian_config_to_db(new_cfg)
        # PnL formula unchanged — sirf value ka source ab yahan se aata hai.
        fees = trading_fees
        return True, new_cfg
    except (TypeError, ValueError) as e:
        return False, f'Invalid values: {e}'


# ========== BREAK-EVEN STATE TRACKER ==========
# key = product_id, value = be_new_sl price (float) or None
_break_even_applied      = {}
# product_id -> BE SL price (survives position-close cleanup of _break_even_applied)
_positions_had_be_sl     = {}
# entry_order_id -> BE SL price (for history UI badge + DB had_break_even)
_orders_had_be_sl        = {}
_be_retry_pending        = {}      # symbol -> True after BE SL close (pre-order verify reads this)
_break_even_applied_lock = Lock()


def _mark_current_order_had_be(be_new_sl):
    """When guardian moves SL to BE, tag the live entry order for history."""
    oid = BOT_STATE.get('last_placed_order_id')
    if not oid or oid == 'RESUMED':
        return
    with _break_even_applied_lock:
        _orders_had_be_sl[str(oid)] = float(be_new_sl)


def _peek_order_had_be(entry_order_id):
    if not entry_order_id:
        return False
    with _break_even_applied_lock:
        return str(entry_order_id) in _orders_had_be_sl


def _consume_order_had_be(entry_order_id):
    if not entry_order_id:
        return False
    with _break_even_applied_lock:
        return _orders_had_be_sl.pop(str(entry_order_id), None) is not None


def _exit_matches_be_sl(exit_price, be_sl_price, entry_price=None, side=None, tolerance=None):
    """
    True only when fill exit is at/near the BE SL stop (not TP).
    BE SL = entry ± UI offset; TP is much farther — a TP fill must NOT match.
    """
    if exit_price is None or be_sl_price is None:
        return False
    try:
        exit_p = float(exit_price)
        be_sl = float(be_sl_price)
    except (TypeError, ValueError):
        return False

    if tolerance is None:
        # Cover market-order slip on BE SL without reaching TP distance
        ref = abs(be_sl) if be_sl else abs(exit_p)
        tolerance = max(2.0, ref * 0.0005)

    return abs(exit_p - be_sl) <= tolerance


def _resolve_be_retry_on_close(symbol, exit_price=None, entry_price=None, side=None):
    """
    BE SL hit + setting ON → same step retry.
    Requires: position had BE SL applied AND exit fill matches that BE SL price.
    TP after BE → normal win, no same-lot retry.
    """
    gcfg = get_guardian_config()
    if not gcfg.get('break_even_retry_same_step'):
        return False
    product_id = get_product_id(symbol)
    if not product_id:
        return False
    pid = str(product_id)
    with _break_even_applied_lock:
        if pid not in _positions_had_be_sl:
            return False
        # Prefer price stored at BE apply time (survives guardian clear on close)
        be_sl_price = _positions_had_be_sl.pop(pid, None)
        if be_sl_price is None:
            be_sl_price = _break_even_applied.pop(pid, None)
        else:
            _break_even_applied.pop(pid, None)
        if be_sl_price is None and entry_price is not None:
            try:
                offset = float(gcfg.get('break_even_profit_offset', 0))
                ep = float(entry_price)
                side_l = (str(side).lower() if side else '')
                if side_l == 'buy':
                    be_sl_price = ep + offset
                elif side_l == 'sell':
                    be_sl_price = ep - offset
            except (TypeError, ValueError):
                be_sl_price = None

        matched = _exit_matches_be_sl(exit_price, be_sl_price, entry_price, side)
        if matched:
            log_system(
                f"[BE SL RETRY] {symbol}: exit={exit_price} matched BE SL={be_sl_price} "
                f"→ same step/lot"
            )
            return True
        log_system(
            f"[BE SL RETRY] {symbol}: had BE SL but exit={exit_price} != BE SL={be_sl_price} "
            f"(likely TP) → normal step progression"
        )
        return False


def _mark_be_retry_pending(symbol):
    with _break_even_applied_lock:
        _be_retry_pending[symbol] = True


def _pop_be_retry_pending(symbol):
    with _break_even_applied_lock:
        return _be_retry_pending.pop(symbol, False)


def _handle_close_step_progression(symbol, pnl, exit_price=None, entry_price=None, side=None):
    """
    On close:
      - BE SL actually hit (exit ≈ BE SL) + setting ON → same step/lot
      - TP hit (even if BE was applied earlier) → normal win (Step 1)
      - Normal SL / loss without BE → normal loss progression
    """
    be_retry = False
    gcfg = get_guardian_config()
    if gcfg.get('break_even_retry_same_step'):
        product_id = get_product_id(symbol)
        pid = str(product_id) if product_id else None

        if exit_price is not None and pid:
            # Decide from exit price vs stored BE SL — not from pnl alone
            be_retry = _resolve_be_retry_on_close(
                symbol, exit_price=exit_price, entry_price=entry_price, side=side
            )
        else:
            # No exit price → cannot prove BE SL hit; clear flag, normal progression
            if pid:
                with _break_even_applied_lock:
                    _positions_had_be_sl.pop(pid, None)
                    _break_even_applied.pop(pid, None)
            log_system(
                f"[BE SL RETRY] {symbol}: close without exit price to verify BE SL "
                f"(pnl={pnl:.5f}) — normal step progression"
            )
            be_retry = False

    _apply_step_progression(pnl, be_retry=be_retry)
    if be_retry:
        _mark_be_retry_pending(symbol)
    else:
        # Ensure stale pending from a prior close cannot override a TP/normal close
        _pop_be_retry_pending(symbol)

# ========== TP/SL GUARDIAN (with real-time mark price) ==========
# UPDATED VERSION — fixes Break-Even not triggering issue
#
# WHAT CHANGED (only 2 things, everything else is 100% identical to your original):
#   1. BE price fetch: ab REST API se FRESH price pehle try hoti hai (reliable),
#      WS cache sirf backup/fallback hai agar REST fail ho jaye.
#      Pehle ulta tha — WS cache pehle try hota tha jo stale ho sakti thi.
#   2. Debug log line add ki gayi hai (long aur short dono ke liye) jo har
#      2-second cycle mein live_price vs be_trigger dikhayegi. Isse agli baar
#      turant pata chalega ki BE trigger kyun miss ho raha hai (agar phir bhi ho).
#
# Replace your existing auto_tp_sl_guardian function with this ENTIRE function.

# ★ Guardian sirf isi ek market ke TP/SL/BE ko guard karega.
# Baaki kisi bhi symbol ki position ko guardian bilkul ignore karega (touch nahi karega).
GUARDIAN_SYMBOL = "XAUTUSD"


def auto_tp_sl_guardian():
    while True:
        try:
            time.sleep(2)

            positions_response = make_api_request('GET', '/positions/margined')
            if not positions_response or not positions_response.get('success'):
                continue

            # ★ CHANGE: sirf GUARDIAN_SYMBOL ki positions guard hongi, baaki ignore
            active_positions = [
                p for p in positions_response.get('result', [])
                if abs(float(p.get('size', 0))) > 0.0001
                and (p.get("product_symbol") or p.get("symbol")) == GUARDIAN_SYMBOL
            ]

            if not active_positions:
                # Clean up BE tracker when no positions open
                with _break_even_applied_lock:
                    _break_even_applied.clear()
                continue

            # Clean up closed positions from tracker
            active_ids = set(str(p.get('product_id')) for p in active_positions)
            with _break_even_applied_lock:
                for pid in list(_break_even_applied.keys()):
                    if pid not in active_ids:
                        del _break_even_applied[pid]

            # Snapshot config once per cycle — UI changes apply next cycle (≤2s)
            gcfg = get_guardian_config()
            live_tp_pct = float(gcfg['live_tp_percentage'])
            live_sl_pct = float(gcfg['live_sl_percentage'])
            liq_protect = str(gcfg['liquidation_protection']).strip().upper()
            liq_buffer = float(gcfg['liquidation_buffer'])
            be_enabled = bool(gcfg['break_even_enabled'])
            be_trig_pct = float(gcfg['break_even_trigger_percent'])
            be_trig_buf = float(gcfg['break_even_trigger_buffer'])
            be_profit_off = float(gcfg['break_even_profit_offset'])

            for pos in active_positions:
                try:
                    symbol     = pos.get("product_symbol") or pos.get("symbol")
                    size       = float(pos.get("size", 0))
                    entry      = float(pos.get("entry_price", 0))
                    product_id = pos.get("product_id")

                    if not all([symbol, product_id]) or abs(size) < 0.0001 or entry <= 0:
                        continue

                    # ★ CHANGE: double safety — galat symbol aaye to turant skip
                    if symbol != GUARDIAN_SYMBOL:
                        continue

                    pos_key = str(product_id)

                    if size > 0:
                        expected_tp = entry * (1 + live_tp_pct / 100)
                        expected_sl = entry * (1 - live_sl_pct / 100)
                    else:
                        expected_tp = entry * (1 - live_tp_pct / 100)
                        expected_sl = entry * (1 + live_sl_pct / 100)

                    expected_tp = round_price_2dp(expected_tp)
                    expected_sl = round_price_2dp(expected_sl)

                    final_sl = expected_sl

                    # ── Liquidation protection ────────────────────────────
                    if liq_protect == "Y":
                        try:
                            liquidation_price_raw = pos.get("liquidation_price")
                            if liquidation_price_raw is not None:
                                liquidation_price = float(liquidation_price_raw)
                                if size > 0:
                                    if expected_sl <= liquidation_price:
                                        final_sl = round_price_2dp(liquidation_price + liq_buffer)
                                        log_system(
                                            f"[LIQ PROTECT] LONG {symbol}: "
                                            f"original_sl={expected_sl:.6f} is at/below "
                                            f"liquidation={liquidation_price:.6f} -> "
                                            f"protected_sl={final_sl:.6f}"
                                        )
                                else:
                                    if expected_sl >= liquidation_price:
                                        final_sl = round_price_2dp(liquidation_price - liq_buffer)
                                        log_system(
                                            f"[LIQ PROTECT] SHORT {symbol}: "
                                            f"original_sl={expected_sl:.6f} is at/above "
                                            f"liquidation={liquidation_price:.6f} -> "
                                            f"protected_sl={final_sl:.6f}"
                                        )
                        except Exception as liq_err:
                            log_system(f"[LIQ PROTECT] Error for {symbol}: {liq_err} - using original SL")
                            final_sl = expected_sl

                    # ── BREAK-EVEN LOGIC (real-time mark price) ───────────
                    with _break_even_applied_lock:
                        be_already_done = _break_even_applied.get(pos_key) is not None

                    if be_enabled and not be_already_done:
                        try:
                            live_price = None
                            try:
                                ticker_be = make_api_request('GET', f'/tickers/{symbol}')
                                if ticker_be and ticker_be.get('result'):
                                    live_price = float(
                                        ticker_be['result'].get('mark_price')
                                        or ticker_be['result'].get('close')
                                    )
                                    # keep WS cache in sync too
                                    with MARK_PRICES_LOCK:
                                        MARK_PRICES[symbol] = live_price
                            except Exception as rest_err:
                                log_system(f"[BE] REST price fetch failed for {symbol}: {rest_err}")

                            if live_price is None:
                                # REST failed — fall back to WS cache
                                with MARK_PRICES_LOCK:
                                    live_price = MARK_PRICES.get(symbol)
                                if live_price is not None:
                                    log_system(f"[BE] REST failed, using WS cache for {symbol} = {live_price:.4f}")

                            if live_price is not None:
                                if size > 0:  # LONG
                                    tp_distance   = expected_tp - entry
                                    half_distance = tp_distance * be_trig_pct
                                    be_trigger    = round_price_2dp(entry + half_distance - be_trig_buf)
                                    be_new_sl     = round_price_2dp(entry + be_profit_off)

                                    log_throttled(
                                        log_system, f"be_debug_long_{pos_key}",
                                        f"[BE DEBUG] {symbol} LONG | live={live_price:.4f} | "
                                        f"trigger={be_trigger:.4f} | gap={live_price - be_trigger:.4f} | "
                                        f"new_sl_will_be={be_new_sl:.4f}",
                                        min_interval=10
                                    )

                                    if live_price >= be_trigger:
                                        _set_break_even_sl(
                                            symbol, product_id, size,
                                            be_new_sl, pos_key,
                                            direction='long',
                                            live_price=live_price,
                                            be_trigger=be_trigger
                                        )

                                else:  # SHORT
                                    tp_distance   = entry - expected_tp
                                    half_distance = tp_distance * be_trig_pct
                                    be_trigger    = round_price_2dp(entry - half_distance + be_trig_buf)
                                    be_new_sl     = round_price_2dp(entry - be_profit_off)

                                    log_throttled(
                                        log_system, f"be_debug_short_{pos_key}",
                                        f"[BE DEBUG] {symbol} SHORT | live={live_price:.4f} | "
                                        f"trigger={be_trigger:.4f} | gap={be_trigger - live_price:.4f} | "
                                        f"new_sl_will_be={be_new_sl:.4f}",
                                        min_interval=10
                                    )

                                    if live_price <= be_trigger:
                                        _set_break_even_sl(
                                            symbol, product_id, size,
                                            be_new_sl, pos_key,
                                            direction='short',
                                            live_price=live_price,
                                            be_trigger=be_trigger
                                        )
                            else:
                                log_system(f"[BE] No live price available (REST + WS both failed) for {symbol} — skipping BE check this cycle")

                        except Exception as be_err:
                            log_system(f"[BREAK-EVEN] Error for {symbol}: {be_err}")
                    # ── END BREAK-EVEN LOGIC ──────────────────────────────

                    # ── TP/SL existence check ─────────────────────────────
                    # Stop TP/SL on Delta sit in state=pending until triggered.
                    dynamic_tolerance = entry * 0.0005

                    orders_response = make_api_request(
                        'GET',
                        f'/orders?product_ids={product_id}&states=open,pending'
                    )
                    if not orders_response or not orders_response.get('success'):
                        log_throttled(
                            log_system, f"guardian_orders_fail_{pos_key}",
                            f"[GUARDIAN] {symbol}: open/pending orders fetch failed — retry next cycle",
                            min_interval=15
                        )
                        continue

                    orders = orders_response.get("result", [])
                    if isinstance(orders, dict):
                        orders = orders.get("orders") or orders.get("result") or []
                    if not isinstance(orders, list):
                        orders = []

                    def _is_reduce_only(o):
                        v = o.get("reduce_only")
                        return v is True or v == "true" or v == 1 or v == "1"

                    def _stop_type(o):
                        return (o.get("stop_order_type") or "").strip().lower()

                    # Prefer reduce_only stops; fall back to stop_order_type alone
                    # (some bracket legs may omit reduce_only in the payload).
                    tp_orders = [
                        o for o in orders
                        if _stop_type(o) == "take_profit_order"
                        and (_is_reduce_only(o) or o.get("stop_price") is not None)
                    ]
                    sl_orders = [
                        o for o in orders
                        if _stop_type(o) == "stop_loss_order"
                        and (_is_reduce_only(o) or o.get("stop_price") is not None)
                    ]

                    tp_valid        = False
                    sl_valid        = False
                    wrong_tp_orders = []
                    wrong_sl_orders = []

                    for tp_order in tp_orders:
                        stop_price = float(tp_order.get("stop_price", 0) or 0)
                        if abs(stop_price - expected_tp) < dynamic_tolerance:
                            tp_valid = True
                        else:
                            wrong_tp_orders.append(tp_order)

                    with _break_even_applied_lock:
                        be_sl_price = _break_even_applied.get(pos_key)  # None ya BE SL price (float)

                    # After BE: target SL is the BE price, not the original % SL
                    sl_target = float(be_sl_price) if be_sl_price is not None else final_sl

                    for sl_order in sl_orders:
                        stop_price = float(sl_order.get("stop_price", 0) or 0)
                        if abs(stop_price - sl_target) < dynamic_tolerance:
                            sl_valid = True
                        elif be_sl_price is not None and abs(stop_price - be_sl_price) < dynamic_tolerance:
                            sl_valid = True  # BE SL hai, guardian isko overwrite nahi karega
                        else:
                            wrong_sl_orders.append(sl_order)

                    log_throttled(
                        log_system, f"guardian_status_{pos_key}",
                        f"[GUARDIAN] {symbol} | size={size} entry={entry:.4f} "
                        f"expect_tp={expected_tp:.4f} expect_sl={sl_target:.4f} | "
                        f"tp_orders={len(tp_orders)} sl_orders={len(sl_orders)} "
                        f"tp_ok={tp_valid} sl_ok={sl_valid} "
                        f"wrong_tp={len(wrong_tp_orders)} wrong_sl={len(wrong_sl_orders)}",
                        min_interval=20
                    )

                    tp_edited = False
                    sl_edited = False

                    if wrong_tp_orders and not tp_valid:
                        for tp_order in wrong_tp_orders:
                            order_id = tp_order.get("id")
                            log_system(f"[GUARDIAN] EDITING TP order {order_id} → {expected_tp:.2f}")
                            edit_payload = {
                                "id": order_id,
                                "product_id": int(product_id),
                                "order_type": "market_order",
                                "stop_price": "{:.2f}".format(expected_tp),
                                "size": abs(int(size))
                            }
                            edit_body = json.dumps(edit_payload)
                            try:
                                edit_res = requests.put(
                                    BASE_URL + "/v2/orders",
                                    headers=sign_request("PUT", "/v2/orders", edit_body),
                                    data=edit_body,
                                    timeout=10
                                )
                                if edit_res.status_code == 200:
                                    log_system(f"[GUARDIAN] TP EDITED → {expected_tp:.2f}")
                                    tp_edited = True
                                    break
                                log_system(
                                    f"[GUARDIAN] TP EDIT FAILED {order_id}: "
                                    f"HTTP {edit_res.status_code} {edit_res.text[:300]}"
                                )
                            except Exception as edit_err:
                                log_system(f"[GUARDIAN] TP EDIT exception: {edit_err}")

                    if wrong_sl_orders and not sl_valid:
                        for sl_order in wrong_sl_orders:
                            order_id = sl_order.get("id")
                            log_system(f"[GUARDIAN] EDITING SL order {order_id} → {sl_target:.2f}")
                            edit_payload = {
                                "id": order_id,
                                "product_id": int(product_id),
                                "order_type": "market_order",
                                "stop_price": "{:.2f}".format(sl_target),
                                "size": abs(int(size))
                            }
                            edit_body = json.dumps(edit_payload)
                            try:
                                edit_res = requests.put(
                                    BASE_URL + "/v2/orders",
                                    headers=sign_request("PUT", "/v2/orders", edit_body),
                                    data=edit_body,
                                    timeout=10
                                )
                                if edit_res.status_code == 200:
                                    log_system(f"[GUARDIAN] SL EDITED → {sl_target:.2f}")
                                    sl_edited = True
                                    break
                                log_system(
                                    f"[GUARDIAN] SL EDIT FAILED {order_id}: "
                                    f"HTTP {edit_res.status_code} {edit_res.text[:300]}"
                                )
                            except Exception as edit_err:
                                log_system(f"[GUARDIAN] SL EDIT exception: {edit_err}")

                    need_tp = not tp_valid and not tp_edited and len(tp_orders) == 0
                    need_sl = not sl_valid and not sl_edited and len(sl_orders) == 0

                    # Wrong orders still present after failed edit — do NOT place
                    # a second bracket (Delta allows only one position bracket).
                    if (wrong_tp_orders and not tp_valid and not tp_edited) or \
                       (wrong_sl_orders and not sl_valid and not sl_edited):
                        log_throttled(
                            log_system, f"guardian_edit_pending_{pos_key}",
                            f"[GUARDIAN] {symbol}: wrong TP/SL still present after edit "
                            f"attempt — will retry edit next cycle (no delete, no duplicate)",
                            min_interval=15
                        )
                        need_tp = False
                        need_sl = False

                    if need_tp or need_sl:
                        with MARK_PRICES_LOCK:
                            curr_price = MARK_PRICES.get(symbol)
                        if curr_price is None:
                            ticker = make_api_request('GET', f'/tickers/{symbol}')
                            if ticker and ticker.get('result'):
                                curr_price = float(ticker['result']['mark_price']
                                                   or ticker['result']['close'])

                        # Per-leg safety: skip only the leg that would fire immediately
                        place_tp = need_tp
                        place_sl = need_sl
                        if curr_price is not None:
                            if size > 0:  # LONG
                                if place_tp and expected_tp <= curr_price:
                                    log_system(
                                        f"[GUARDIAN] {symbol}: skip TP place — "
                                        f"tp={expected_tp:.4f} <= live={curr_price:.4f}"
                                    )
                                    place_tp = False
                                if place_sl and sl_target >= curr_price:
                                    log_system(
                                        f"[GUARDIAN] {symbol}: skip SL place — "
                                        f"sl={sl_target:.4f} >= live={curr_price:.4f} "
                                        f"(would trigger immediately)"
                                    )
                                    place_sl = False
                            else:  # SHORT
                                if place_tp and expected_tp >= curr_price:
                                    log_system(
                                        f"[GUARDIAN] {symbol}: skip TP place — "
                                        f"tp={expected_tp:.4f} >= live={curr_price:.4f}"
                                    )
                                    place_tp = False
                                if place_sl and sl_target <= curr_price:
                                    log_system(
                                        f"[GUARDIAN] {symbol}: skip SL place — "
                                        f"sl={sl_target:.4f} <= live={curr_price:.4f} "
                                        f"(would trigger immediately)"
                                    )
                                    place_sl = False

                        if not place_tp and not place_sl:
                            continue

                        payload = {"product_id": int(product_id)}
                        if place_tp:
                            payload["take_profit_order"] = {
                                "order_type": "market_order",
                                "stop_price": "{:.2f}".format(expected_tp)
                            }
                        if place_sl:
                            payload["stop_loss_order"] = {
                                "order_type": "market_order",
                                "stop_price": "{:.2f}".format(sl_target)
                            }

                        log_system(
                            f"[GUARDIAN] Placing missing bracket for {symbol} | "
                            f"tp={'Y' if place_tp else 'N'}@{expected_tp:.2f} "
                            f"sl={'Y' if place_sl else 'N'}@{sl_target:.2f}"
                        )
                        body = json.dumps(payload)
                        try:
                            res = requests.post(
                                BASE_URL + "/v2/orders/bracket",
                                headers=sign_request("POST", "/v2/orders/bracket", body),
                                data=body,
                                timeout=10
                            )
                            if res.status_code == 200:
                                log_system(f"[GUARDIAN] Bracket placed for {symbol}")
                            else:
                                log_system(
                                    f"[GUARDIAN] Bracket place FAILED {symbol}: "
                                    f"HTTP {res.status_code} {res.text[:300]}"
                                )
                        except Exception as place_err:
                            log_system(f"[GUARDIAN] Bracket place exception: {place_err}")

                    time.sleep(0.3)

                except Exception as pos_err:
                    log_throttled(
                        log_system, "guardian_pos_exc",
                        f"[GUARDIAN] position-loop error: {pos_err}",
                        min_interval=15
                    )

        except Exception as loop_err:
            log_throttled(
                log_system, "guardian_loop_exc",
                f"[GUARDIAN] main-loop error: {loop_err}",
                min_interval=15
            )
            time.sleep(2)

def _set_break_even_sl(symbol, product_id, size, be_new_sl, pos_key,
                        direction, live_price, be_trigger):
    """
    Helper: edit existing SL order to break-even level, or place new bracket SL.
    CHANGE 1: Stores be_new_sl price (float) instead of True in tracker.
    """
    be_new_sl    = round_price_2dp(be_new_sl)
    # pending = untriggered stops; open alone misses TP/SL brackets
    orders_be    = make_api_request(
        'GET', f'/orders?product_ids={product_id}&states=open,pending'
    )
    sl_orders_be = []
    if orders_be and orders_be.get('success'):
        raw = orders_be.get("result", [])
        if isinstance(raw, dict):
            raw = raw.get("orders") or []
        if not isinstance(raw, list):
            raw = []
        sl_orders_be = [
            o for o in raw
            if (o.get("stop_order_type") or "").strip().lower() == "stop_loss_order"
        ]

    be_sl_set = False

    for sl_o in sl_orders_be:
        order_id_be  = sl_o.get("id")
        edit_payload = {
            "id":         order_id_be,
            "product_id": int(product_id),
            "order_type": "market_order",
            "stop_price": "{:.2f}".format(be_new_sl),
            "size":       abs(int(size))
        }
        edit_body = json.dumps(edit_payload)
        try:
            edit_res = requests.put(
                BASE_URL + "/v2/orders",
                headers=sign_request("PUT", "/v2/orders", edit_body),
                data=edit_body,
                timeout=10
            )
            if edit_res.status_code == 200:
                log_system(
                    f"[BREAK-EVEN] {direction.upper()} {symbol}: "
                    f"live={live_price:.6f} vs trigger={be_trigger:.6f} -> "
                    f"SL moved to {be_new_sl:.6f}"
                )
                # CHANGE 1: True ki jagah be_new_sl price store karo
                with _break_even_applied_lock:
                    _break_even_applied[pos_key] = be_new_sl
                    _positions_had_be_sl[pos_key] = be_new_sl
                _mark_current_order_had_be(be_new_sl)
                be_sl_set = True
                break
        except Exception:
            pass

    if not be_sl_set and not sl_orders_be:
        # No existing SL order — place fresh bracket SL
        payload_be = {
            "product_id": int(product_id),
            "stop_loss_order": {
                "order_type": "market_order",
                "stop_price": "{:.2f}".format(be_new_sl)
            }
        }
        body_be = json.dumps(payload_be)
        try:
            res_be = requests.post(
                BASE_URL + "/v2/orders/bracket",
                headers=sign_request("POST", "/v2/orders/bracket", body_be),
                data=body_be,
                timeout=10
            )
            if res_be.status_code == 200:
                log_system(
                    f"[BREAK-EVEN] {direction.upper()} {symbol}: "
                    f"New SL placed at {be_new_sl:.6f}"
                )
                # CHANGE 1: True ki jagah be_new_sl price store karo
                with _break_even_applied_lock:
                    _break_even_applied[pos_key] = be_new_sl
                    _positions_had_be_sl[pos_key] = be_new_sl
                _mark_current_order_had_be(be_new_sl)
        except Exception:
            pass


# ========== MAIN ==========
if __name__ == '__main__':
    init_database()

    print("Loading guardian config from DB...")
    load_guardian_config_from_db()

    print("Loading lot steps config from DB...")
    load_lot_steps_config_from_db()

    print("Loading bot config from DB...")
    load_bot_config_from_db()

    print("Loading decision timeframe config from DB...")
    load_decision_tf_config_from_db()

    print("Starting keepalive for Render...")
    start_keep_alive()
    print("Keepalive started - will ping every 10 minutes")

    print("Starting signal bot...")
    if start_signal_bot():
        print("Signal bot started successfully")
    else:
        print("Failed to start signal bot")

    guardian_thread = threading.Thread(target=auto_tp_sl_guardian, daemon=True)
    guardian_thread.start()
    print("TP/SL Guardian started in background")

    # ══════════════════════════════════════════════════════════════════
    # AUTO-RESUME AFTER UNEXPECTED PROCESS RESTART (NEW)
    # ══════════════════════════════════════════════════════════════════
    # Ye is baat ka fix hai: "kuch din baad bot khud se band ho gaya tha".
    # Agar hosting platform (Render etc.) process ko kisi bhi wajah se
    # (memory/redeploy/crash) restart kare, to normally BOT_STATE fresh
    # ho jaata hai (running=False) - UI dikhata hai bot band hai, jabki
    # user ne kabhi terminate nahi kiya tha.
    #
    # Ab startup par DB check hoti hai: agar last known state "active"
    # tha (yaani sirf UI ke Force-Stop/Stop-Bot se hi False hota hai),
    # to bot khud-b-khud usi symbol/leverage/TP/SL settings ke saath
    # wapas start ho jaata hai. Agar user ne UI se stop kiya tha, ye
    # flag False hi rahega aur bot restart nahi hoga - jaisa chahiye.
    # ══════════════════════════════════════════════════════════════════
    try:
        session_flag = load_session_active_flag()
        if session_flag and session_flag.get('active'):
            print("🔁 Detected previous session was ACTIVE (not stopped via UI) — auto-resuming bot...")
            BOT_STATE['symbol']     = session_flag.get('symbol', BOT_STATE['symbol'])
            BOT_STATE['leverage']   = session_flag.get('leverage', BOT_STATE['leverage'])
            BOT_STATE['tp_percent'] = session_flag.get('tp_percent', BOT_STATE['tp_percent'])
            BOT_STATE['sl_percent'] = session_flag.get('sl_percent', BOT_STATE['sl_percent'])
            resume_bot = session_flag.get('strategy_bot') or 'bot1'
            ok_bot, bot_msg, _ = bot_manager.activate_bot(resume_bot)
            if not ok_bot:
                print(f"⚠️ Auto-resume strategy activate failed ({bot_msg}) — falling back to bot1")
                ok_bot, bot_msg, _ = bot_manager.activate_bot('bot1')
            if ok_bot:
                print(f"✅ Strategy loaded for resume: {bot_msg}")
            start_websocket_engine()
            time.sleep(1)
            if start_auto_trading_bot():
                print(f"✅ Auto-resumed bot for symbol={BOT_STATE['symbol']} strategy={resume_bot}")
            else:
                print("⚠️ Auto-resume attempted but bot did not start")
        else:
            print("ℹ️ No active session to resume - bot stays OFF until started from UI")
    except Exception as e:
        print(f"⚠️ Auto-resume check failed: {e}")

    try:
        print("👉 http://127.0.0.1:8090")
        app.run(debug=False, host='0.0.0.0', port=8090, use_reloader=False)
    finally:
        print("Stopping signal bot...")
        stop_signal_bot()
        print("Signal bot stopped")