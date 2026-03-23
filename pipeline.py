"""
pipeline.py
-----------
Master runner for the GSE Intelligence Pipeline.
Orchestrates: fetch → validate → store → log

Run daily via Task Scheduler:
    python pipeline.py

Or import and call:
    from pipeline import run
    run()
"""

from datetime import date

from fetch import fetch_gse_live, save_raw
from database import init_db, upsert_stocks, insert_daily_prices
from logger import get_logger

logger = get_logger(__name__)


def validate(data: list[dict]) -> list[dict]:
    """
    Basic validation pass.
    - Removes records missing ticker or price
    - Logs any anomalies
    Returns cleaned list.
    """
    clean = []
    skipped = 0

    for r in data:
        ticker = r.get("name") or r.get("ticker")
        price  = r.get("price")

        if not ticker:
            logger.warning(f"Record missing ticker — skipping: {r}")
            skipped += 1
            continue

        if price is None:
            logger.warning(f"{ticker}: missing price — skipping.")
            skipped += 1
            continue

        if price < 0:
            logger.warning(f"{ticker}: negative price ({price}) — skipping.")
            skipped += 1
            continue

        clean.append(r)

    if skipped:
        logger.warning(f"Validation dropped {skipped} records.")

    logger.info(f"Validation passed {len(clean)} records.")
    return clean


def run() -> None:
    today = date.today()
    logger.info(f"=== GSE Pipeline starting — {today} ===")

    # 1. Initialise DB (safe to run every time)
    init_db()

    # 2. Fetch from API
    raw_data = fetch_gse_live()
    if not raw_data:
        logger.error("Pipeline aborted — fetch returned no data.")
        return

    # 3. Save raw backup
    save_raw(raw_data)

    # 4. Validate
    clean_data = validate(raw_data)
    if not clean_data:
        logger.error("Pipeline aborted — no valid records after validation.")
        return

    # 5. Store in DB
    upsert_stocks(clean_data, today)
    inserted = insert_daily_prices(clean_data, today)

    logger.info(
        f"=== Pipeline complete — {inserted} records stored for {today} ==="
    )


if __name__ == "__main__":
    run()
