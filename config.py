import os

# === PATHS ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_RAW     = os.path.join(BASE_DIR, "data", "raw")
DATA_PROCESSED = os.path.join(BASE_DIR, "data", "processed")
LOGS_DIR     = os.path.join(BASE_DIR, "logs")
DB_PATH      = os.path.join(DATA_PROCESSED, "gse.db")

# === API ===
GSE_LIVE_URL   = "https://dev.kwayisi.org/apis/gse/live"
REQUEST_TIMEOUT = 15  # seconds

# === PIPELINE ===
LOG_LEVEL = "INFO"
