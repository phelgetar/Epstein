"""
Centralized configuration for the Epstein DOJ Files project.
All paths, ports, and security settings in one place.
"""

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

# Data source
SOURCE_URL = "https://www.justice.gov/epstein/doj-disclosures"
NUM_DATASETS = 12

# JSON output filenames (stored in DATA_DIR)
JSON_FULL = "epstein_pdfs_full.json"
JSON_SEARCH_INDEX = "epstein_pdfs_search_index.json"
JSON_SUMMARY = "epstein_pdfs_summary.json"
JSON_FILE_LIST = "epstein_pdfs_file_list.json"

# Allowed file extensions the server may serve
ALLOWED_EXTENSIONS = {".html", ".json", ".pdf", ".css", ".js", ".png", ".jpg", ".ico"}

# Auto-reload settings
WATCH_EXTENSIONS = {".py", ".html", ".css", ".js"}
WATCH_DIRS = [
    str(PROJECT_ROOT / "src"),
    str(PROJECT_ROOT / "static"),
]
