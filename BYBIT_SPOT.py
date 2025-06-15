import os
import time
import sqlite3
import requests
import threading
import pandas as pd
import schedule
import logging
from datetime import datetime
from flask import Flask
from ta.trend import EMAIndicator, MACD
from ta.momentum import RSIIndicator, StochRSIIndicator
from ta.volatility import AverageTrueRange, BollingerBands
from pybit.unified_trading import HTTP
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

# === KONFIGURASI ===
api_key = "R4Nv7r38iigntfHjYe"
api_secret = "aeK29VCgC6dKJrUuvouHAlPSKtzAzSqauUPz"
tg_token = "8153894385:AAFa4kbNHTlDkJ0Fq2BWFk-jyZ0k9rxbk5k"
tg_id = "7153166439"
category = "spot"
timeframes = ["1m", "5m", "15m", "30m", "1h"]
MAX_SYMBOLS = 100
BATCH_SLEEP = 2
MAX_WORKERS = 10

interval_map = {
    "30s": "0.5", "1m": "1", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "4h": "240", "6h": "360", "1d": "D"
}

# === INISIALISASI ===
session = HTTP(api_key=api_key, api_secret=api_secret)
app = Flask(__name__)
symbols = []
message_queue = Queue()
MAX_MESSAGES_PER_SECOND = 1

logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.WARNING
)

# === TELEGRAM ===
def telegram_worker():
    while True:
        if not message_queue.empty():
            msg = message_queue.get()
            send_telegram(msg)
            time.sleep(1 / MAX_MESSAGES_PER_SECOND)
        else:
            time.sleep(0.1)

def send_telegram(message):
    url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    payload = {"chat_id": tg_id, "text": message, "parse_mode": "HTML"}
    try:
        response = requests.post(url, data=payload, timeout=10)
        if response.status_code == 429:
            retry_after = response.json().get("parameters", {}).get("retry_after", 5)
            logging.warning(f"[RATE LIMIT] Retry after {retry_after}s")
            time.sleep(retry_after + 1)
            message_queue.put(message)
        elif response.status_code != 200:
            logging.error("Telegram Error: %s", response.text)
    except Exception as e:
        logging.error("Telegram Exception: %s", e)

# === DATABASE ===
def init_db():
    with sqlite3.connect("signals.db") as conn:
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            timeframe TEXT,
            price REAL,
            signal TEXT,
            timestamp TEXT
        )''')
        conn.commit()

# === FETCH SYMBOLS ===
def fetch_symbols():
    try:
        response = session.get_instruments_info(category=category)
        data = response.get("result", {}).get("list", [])
        return [item["symbol"] for item in data if "USDT" in item["symbol"]][:MAX_SYMBOLS]
    except Exception as e:
        logging.error("Symbol fetch failed: %s", e)
        return []

# === FETCH OHLCV ===
def fetch_ohlcv(symbol, interval, limit=200):
    for attempt in range(3):
        try:
            res = session.get_kline(category=category, symbol=symbol, interval=interval, limit=limit)
            df = pd.DataFrame(res["result"]["list"], columns=[
                "timestamp", "open", "high", "low", "close", "volume", "turnover"
            ])
            df["timestamp"] = pd.to_datetime(pd.to_numeric(df["timestamp"]), unit='ms')
            df = df.astype({
                "open": float, "high": float, "low": float,
                "close": float, "volume": float, "turnover": float
            })
            return df.sort_values("timestamp")
        except Exception as e:
            logging.warning(f"[{symbol}] OHLCV fetch failed ({attempt+1}/3): {e}")
            time.sleep(1 + attempt)
    return pd.DataFrame()

# === CANDLESTICK PATTERN ===
def detect_candlestick_pattern(df):
    last = df.iloc[-1]
    body = abs(last["close"] - last["open"])
    upper = last["high"] - max(last["close"], last["open"])
    lower = min(last["close"], last["open"]) - last["low"]

    if lower > 2 * body and upper < body:
        return "🔨 Hammer"
    elif upper > 2 * body and lower < body:
        return "⭐ Shooting Star"
    return "–"

# === STRATEGI ===
def analyze_symbol(symbol, tf):
    interval = interval_map.get(tf)
    df = fetch_ohlcv(symbol, interval)
    if df.empty or len(df) < 50:
        return

    try:
        df["ema20"] = EMAIndicator(df["close"], window=20).ema_indicator()
        df["ema50"] = EMAIndicator(df["close"], window=50).ema_indicator()
        df["rsi"] = RSIIndicator(df["close"]).rsi()
        df["macd"] = MACD(df["close"]).macd_diff()
        df["atr"] = AverageTrueRange(df["high"], df["low"], df["close"]).average_true_range()
        df["stochrsi"] = StochRSIIndicator(df["close"]).stochrsi()
        bb = BollingerBands(df["close"])
        df["bb_upper"] = bb.bollinger_hband()
        df["bb_lower"] = bb.bollinger_lband()

        latest = df.iloc[-1]
        candle = detect_candlestick_pattern(df)

        signal = ""
        if latest["ema20"] > latest["ema50"] and latest["rsi"] < 70:
            signal = "📈 BUY"
        elif latest["ema20"] < latest["ema50"] and latest["rsi"] > 30:
            signal = "📉 SELL"

        tahan = "⏳ Tidak disarankan ditahan"
        if latest["ema20"] >= latest["ema50"] and latest["rsi"] < 50:
            tahan = "📆 Potensi jangka pendek (1–2 Hari)"
        elif latest["ema20"] >= latest["ema50"] and latest["rsi"] < 70:
            tahan = "📆 Disarankan HOLD 1–3 Hari"
        elif latest["ema20"] < latest["ema50"]:
            tahan = "⚠️ Trend lemah – jangan ditahan"

        sniper_price = latest["close"]
        swing_df = fetch_ohlcv(symbol, interval_map["4h"])
        swing_price = swing_df.iloc[-1]["close"] if not swing_df.empty else 0

        if signal:
            message = (
                f"<b>{signal} Signal</b>\n"
                f"Symbol: <code>{symbol}</code>\n"
                f"Timeframe: <code>{tf}</code>\n"
                f"Price: <code>{sniper_price:.4f}</code>\n"
                f"EMA20: <code>{latest['ema20']:.4f}</code> | "
                f"EMA50: <code>{latest['ema50']:.4f}</code>\n"
                f"Bollinger: <code>{latest['bb_lower']:.4f} - {latest['bb_upper']:.4f}</code>\n"
                f"RSI: <code>{latest['rsi']:.2f}</code> | "
                f"StochRSI: <code>{latest['stochrsi']:.2f}</code>\n"
                f"MACD: <code>{latest['macd']:.4f}</code> | "
                f"ATR: <code>{latest['atr']:.4f}</code>\n"
                f"Candlestick: {candle}\n"
                f"🎯 Sniper Price: <code>{sniper_price:.4f}</code>\n"
                f"🔁 Swing Price: <code>{swing_price:.4f}</code>\n"
                f"{tahan}\n"
                f"🕒 Time: <code>{latest['timestamp']}</code>\n"
                f"✅ Valid by RSIbantaiBot @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )
            message_queue.put(message)

            with sqlite3.connect("signals.db") as conn:
                c = conn.cursor()
                c.execute(
                    "INSERT INTO signals (symbol, timeframe, price, signal, timestamp) VALUES (?, ?, ?, ?, ?)",
                    (symbol, tf, sniper_price, signal, str(latest["timestamp"]))
                )
                conn.commit()
    except Exception as e:
        logging.error(f"[{symbol} - {tf}] Analysis Error: {e}")

# === PEKERJAAN SCAN ===
def job():
    if not symbols:
        return
    for tf in timeframes:
        batch = []
        for i, symbol in enumerate(symbols):
            batch.append(symbol)
            if len(batch) >= MAX_WORKERS or i == len(symbols) - 1:
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(analyze_symbol, sym, tf) for sym in batch]
                    for _ in as_completed(futures):
                        pass
                time.sleep(BATCH_SLEEP)
                batch.clear()

# === MAIN ===
def main():
    global symbols
    init_db()
    symbols_loaded = fetch_symbols()
    if not symbols_loaded:
        logging.error("❌ Symbol list kosong. Program dihentikan.")
        return
    symbols.extend(symbols_loaded)
    logging.warning(f"✅ {len(symbols)} simbol berhasil dimuat.")
    threading.Thread(target=telegram_worker, daemon=True).start()
    schedule.every(60).seconds.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
