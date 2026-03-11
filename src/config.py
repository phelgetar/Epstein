"""
Centralized configuration for the Epstein DOJ Files project.
All paths, ports, and security settings in one place.
"""

import os
from dataclasses import dataclass
from pathlib import Path

# Project root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Directories
STATIC_DIR = PROJECT_ROOT / "static"
DATA_DIR = PROJECT_ROOT / "data"
PDF_DIR = PROJECT_ROOT / "epstein_doj_files"

# Server
SERVER_HOST = "0.0.0.0"  # Bind to all interfaces for LAN access
PREFERRED_PORT = 8000
PORT_RANGE = range(8000, 8100)
BASE_PATH = os.environ.get("BASE_PATH", "").rstrip("/")  # e.g. "/a7f3x9k2m4p8"
GCS_BASE_URL = os.environ.get("GCS_BASE_URL", "").rstrip("/")  # e.g. "https://storage.googleapis.com/epstein-doj-files"

# Data source
SOURCE_URL = "https://www.justice.gov/epstein/doj-disclosures"
SEARCH_URL = "https://www.justice.gov/epstein/search"


# ─── Dataset Registry ────────────────────────────────────────
@dataclass
class DatasetInfo:
    id: int
    name: str           # Display name (e.g. "Data Set 1", "GDrive IMAGES001")
    source_dir: Path    # Directory containing source files
    file_type: str      # "pdf", "image", or "media"
    file_globs: list    # e.g. ["*.pdf"] or ["*.jpg", "*.tif"]


DATASET_REGISTRY: dict[int, DatasetInfo] = {}

# DOJ datasets 1-12 (PDFs)
for _i in range(1, 13):
    DATASET_REGISTRY[_i] = DatasetInfo(
        id=_i, name=f"Data Set {_i}",
        source_dir=PDF_DIR / f"data-set-{_i}",
        file_type="pdf", file_globs=["*.pdf"],
    )

# Google Drive IMAGES 13-24 (JPG/TIF page scans)
GDRIVE_DIR = PDF_DIR / "Google_Drive_Files"
for _i in range(1, 13):
    _ds_id = 12 + _i
    DATASET_REGISTRY[_ds_id] = DatasetInfo(
        id=_ds_id, name=f"GDrive IMAGES{_i:03d}",
        source_dir=GDRIVE_DIR / "IMAGES" / f"IMAGES{_i:03d}",
        file_type="image", file_globs=["*.jpg", "*.tif"],
    )

# Google Drive NATIVES 25-28 (MP4/WAV media)
_NATIVE_MAP = {25: "NATIVE006", 26: "NATIVE008", 27: "NATIVE011", 28: "NATIVE012"}
for _ds_id, _folder in _NATIVE_MAP.items():
    DATASET_REGISTRY[_ds_id] = DatasetInfo(
        id=_ds_id, name=f"GDrive {_folder}",
        source_dir=GDRIVE_DIR / "NATIVES" / _folder,
        file_type="media", file_globs=["*.MP4", "*.WAV", "*.mp4", "*.wav", "*.avi", "*.AVI", "*.mov", "*.MOV"],
    )

NUM_DATASETS = max(DATASET_REGISTRY.keys())  # 28

# JSON output filenames (stored in DATA_DIR)
JSON_FULL = "epstein_pdfs_full.json"
JSON_SEARCH_INDEX = "epstein_pdfs_search_index.json"
JSON_SUMMARY = "epstein_pdfs_summary.json"
JSON_FILE_LIST = "epstein_pdfs_file_list.json"

# SQLite FTS5 search database
SEARCH_DB = DATA_DIR / "epstein_search.db"

# Allowed file extensions the server may serve
ALLOWED_EXTENSIONS = {".html", ".json", ".pdf", ".css", ".js", ".png", ".jpg", ".jpeg", ".ico", ".mp4", ".tif", ".wav", ".avi", ".mov"}

# MySQL analytics database (optional)
DATABASE_URL = os.environ.get("DATABASE_URL", "")  # mysql+pymysql://user:pass@host/db

# Download settings
DOWNLOAD_WORKERS = 10      # Concurrent PDF download threads
DOWNLOAD_BATCH_SIZE = 10   # Pages to scan before downloading (memory management)
PAGE_FETCH_DELAY = 2.0     # Seconds between page requests (Akamai rate limiting)

# Video download settings
VIDEO_DIR = PDF_DIR / "videos"
VIDEO_DOWNLOAD_WORKERS = 10
VIDEO_BATCH_SIZE = 10

# Google Drive download settings
GDRIVE_FOLDER_ID = "1cyc_2BkQQYaocMOYj87lWbA4BAN_URWz"

# Thumbnail settings
THUMB_DIR = DATA_DIR / "thumbnails"
THUMB_WIDTH = 1000          # px wide (height auto-scaled)
THUMB_QUALITY = 85          # JPEG quality (1-100)
THUMB_WORKERS = 4           # Concurrent generation threads

# Classification settings (Gemini Flash)
CLASSIFY_DIR = DATA_DIR / "classifications"
CLASSIFY_MODEL = "gemini-2.0-flash"
CLASSIFY_WORKERS = 10           # Concurrent API calls
CLASSIFY_RPM = 2000             # Requests per minute (paid tier; free = 15)
CLASSIFY_SAVE_INTERVAL = 100    # Save JSON after every N classifications

# Auto-reload settings
WATCH_EXTENSIONS = {".py", ".html", ".css", ".js"}
WATCH_DIRS = [
    str(PROJECT_ROOT / "src"),
    str(PROJECT_ROOT / "static"),
]

# Logging
LOG_DIR = DATA_DIR / "logs"
LOG_FILE = LOG_DIR / "app.jsonl"
