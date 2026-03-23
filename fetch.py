"""
fetch.py
--------
Pulls live equity data from the Kwayisi GSE API and saves raw JSON
to data/raw/ with a dated filename.

Run directly:  python fetch.py
Or imported:   from fetch import fetch_gse_live
"""

import requests
import json
import os
from datetime import date, datetime

from config import GSE_LIVE_URL, REQUEST_TIMEOUT, DATA_RAW
from logger import get_logger

logger = get_logger(__name__)


def fetch_gse_live() -> list[dict] | None:
    """
    Fetches live GSE data from the Kwayisi API.
    Returns parsed JSON (list of stock dicts) or None on failure.
    """
    logger.info(f"Fetching GSE live data from {GSE_LIVE_URL}")

    try:
        response = requests.get(GSE_LIVE_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()  # raises on 4xx / 5xx

        data = response.json()

        if not isinstance(data, list) or len(data) == 0:
            logger.warning("API returned empty or unexpected data format.")
            return None

        logger.info(f"Successfully fetched {len(data)} stock records.")
        return data

    except requests.exceptions.Timeout:
        logger.error(f"Request timed out after {REQUEST_TIMEOUT}s.")
    except requests.exceptions.ConnectionError:
        logger.error("Connection error — check your internet connection.")
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error: {e}")
    except json.JSONDecodeError:
        logger.error("Failed to parse API response as JSON.")
    except Exception as e:
        logger.error(f"Unexpected error during fetch: {e}")

    return None


def save_raw(data: list[dict]) -> str | None:
    """
    Saves raw JSON to data/raw/gse_live_YYYY-MM-DD_HHMMSS.json
    Returns the filepath or None on failure.
    """
    os.makedirs(DATA_RAW, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = f"gse_live_{timestamp}.json"
    filepath = os.path.join(DATA_RAW, filename)

    try:
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)
        logger.info(f"Raw data saved → {filepath}")
        return filepath
    except IOError as e:
        logger.error(f"Failed to save raw file: {e}")
        return None


if __name__ == "__main__":
    data = fetch_gse_live()
    if data:
        save_raw(data)
    else:
        logger.error("Fetch failed. No data saved.")
