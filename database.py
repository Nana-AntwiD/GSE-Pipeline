"""
database.py
-----------
Manages the SQLite database for the GSE pipeline.
Handles schema creation and all insert / query operations.

Schema:
  stocks       — master list of tickers
  daily_prices — one row per ticker per trading day
"""

import sqlite3
import os
from datetime import date
from typing import Optional

from config import DB_PATH, DATA_PROCESSED
from logger import get_logger

logger = get_logger(__name__)


def get_connection() -> sqlite3.Connection:
    os.makedirs(DATA_PROCESSED, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # rows behave like dicts
    conn.execute("PRAGMA journal_mode=WAL")  # safer concurrent writes
    return conn


def init_db() -> None:
    """
    Creates tables if they don't already exist.
    Safe to run every time — idempotent.
    """
    with get_connection() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS stocks (
                ticker      TEXT PRIMARY KEY,
                first_seen  DATE NOT NULL,
                last_seen   DATE NOT NULL
            );

            CREATE TABLE IF NOT EXISTS daily_prices (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                date        DATE    NOT NULL,
                ticker      TEXT    NOT NULL,
                price       REAL,
                change      REAL,
                volume      INTEGER,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, ticker)
            );

            CREATE INDEX IF NOT EXISTS idx_daily_prices_ticker
                ON daily_prices(ticker);

            CREATE INDEX IF NOT EXISTS idx_daily_prices_date
                ON daily_prices(date);
        """)
    logger.info(f"Database initialised → {DB_PATH}")


def upsert_stocks(records: list[dict], today: date) -> None:
    """
    Keeps the stocks master table up to date.
    Inserts new tickers; updates last_seen for existing ones.
    """
    with get_connection() as conn:
        for r in records:
            ticker = r.get("name") or r.get("ticker")
            if not ticker:
                continue
            conn.execute("""
                INSERT INTO stocks (ticker, first_seen, last_seen)
                VALUES (?, ?, ?)
                ON CONFLICT(ticker) DO UPDATE SET last_seen = excluded.last_seen
            """, (ticker, today, today))
    logger.debug(f"Upserted {len(records)} tickers into stocks table.")


def insert_daily_prices(records: list[dict], today: date) -> int:
    """
    Inserts daily price records. Skips duplicates (same date + ticker).
    Returns the number of rows actually inserted.
    """
    inserted = 0
    with get_connection() as conn:
        for r in records:
            ticker = r.get("name") or r.get("ticker")
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO daily_prices
                        (date, ticker, price, change, volume)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    str(today),
                    ticker,
                    r.get("price"),
                    r.get("change"),
                    r.get("volume"),
                ))
                if conn.execute("SELECT changes()").fetchone()[0]:
                    inserted += 1
            except Exception as e:
                logger.warning(f"Skipped {ticker}: {e}")

    logger.info(f"Inserted {inserted} new price records for {today}.")
    return inserted


def query(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    """Generic read query. Returns list of Row objects."""
    with get_connection() as conn:
        return conn.execute(sql, params).fetchall()
