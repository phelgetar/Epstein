# Epstein DOJ Files

Tools for downloading, extracting, searching, and browsing the publicly released DOJ disclosure documents related to the Epstein case from [justice.gov](https://www.justice.gov/epstein/doj-disclosures).

## Features

- **Downloader** — Playwright-based PDF downloader with stealth patches to bypass Akamai CDN, batched pagination, and multithreaded downloads
- **Google Drive Downloader** — Downloads files from shared Google Drive folders using the internal Drive API for complete file listings (no 50-file cap) with auto-discovery of API keys
- **Extractor** — Converts all files (PDFs, images, media) to searchable JSON; PDFs use Poppler (`pdftotext`/`pdfinfo`) with page-level offsets, non-PDF files are indexed by filename
- **Thumbnails** — Batch JPEG thumbnail generator: PDF pages via PyMuPDF, JPG/TIF images via Pillow resize, media files skipped
- **Classifier** — AI image classification using Google Gemini 2.0 Flash for tagging and person recognition
- **CLI Search** — Full-text search with AND/OR/NOT operators, NEAR/N proximity, quoted phrases, and page references
- **Web Interface** — Browser-based search UI with highlighted results and inline PDF viewing
- **Gallery** — Thumbnail gallery with lightbox viewer, tag autocomplete, content type and person filtering
- **Log Viewer** — Searchable structured log viewer with level/module filtering
- **GCS Sync** — Upload new/changed files to Google Cloud Storage for public hosting
- **MySQL Analytics** — Optional search query and page view logging to MySQL
- **Deployment** — Docker-based deployment to Google Cloud Run

## Architecture

All data files (PDFs, thumbnails, classifications, Google Drive files) are hosted on **Google Cloud Storage** (`gs://epstein-doj-files-jarheads`). The web server only handles search API requests and serves the HTML/CSS/JS frontend — file URLs point directly to GCS.

```
Local Machine                    GCS Bucket                      Cloud Run
┌────────────────┐    gcs_sync     ┌───────────────────┐             ┌──────────────┐
│  PDFs (176 GB) │ ──────────────> │ epstein_doj_files/│ <── browser │ src/server.py│
│   Thumbnails   │                 │ thumbnails/       │             │ static/      │
│ Classifications│                 │ classifications/  │             │ search index │
└────────────────┘                 └───────────────────┘             └──────────────┘
                                  epstein.jarheads.net → Cloud Run service
```

## Prerequisites

- Python 3.8+
- [Poppler](https://poppler.freedesktop.org/) (for PDF text extraction)
- Playwright + Chromium (for downloading)
- [Google Cloud SDK](https://cloud.google.com/sdk) (for GCS sync)

```bash
# macOS
brew install poppler google-cloud-sdk

# Ubuntu/Debian
sudo apt install poppler-utils

# Python dependencies
pip install -r requirements.txt

# Install Playwright browsers
playwright install chromium

# Authenticate with GCP (one-time)
gcloud auth login
```

## Quick Start

### 1. Download PDFs

```bash
python -m src.downloader                     # Download all 12 datasets
python -m src.downloader --dataset 1         # Dataset 1 only
python -m src.downloader --dataset 1 3 5     # Specific datasets
python -m src.downloader --workers 10        # 10 concurrent threads
python -m src.downloader --batch-size 20     # 20 pages per batch
python -m src.downloader --dry-run           # Count files without downloading
python -m src.downloader --headless          # Headless mode (page 0 only)
```

Downloads PDFs from justice.gov into `epstein_doj_files/data-set-N/`. Output shows only errors and actual downloads; the summary includes per-dataset failure counts.

### 2. Download Google Drive Files

```bash
python -m src.gdrive_downloader                    # Download everything
python -m src.gdrive_downloader --dry-run          # List folders + file counts
python -m src.gdrive_downloader --folder IMAGES    # IMAGES only
python -m src.gdrive_downloader --folder NATIVES   # NATIVES only
python -m src.gdrive_downloader --workers 5        # Concurrent downloads
python -m src.gdrive_downloader --delay 0.5        # Rate limit (seconds between downloads)
python -m src.gdrive_downloader --verbose          # Show file names during listing
```

Downloads files from a shared Google Drive folder into `epstein_doj_files/Google_Drive_Files/`, preserving the folder structure. The shared folder contains 33,655 files across 16 subfolders:

- **IMAGES/** — 12 subfolders (IMAGES001–012) with `.jpg` and `.tif` scanned document images
- **NATIVES/** — 4 subfolders (NATIVE006/008/011/012) with `.MP4` video and `.WAV` audio files

Uses Google Drive's internal API for file listing (no browser, no authentication, no 50-file cap) and handles Google's virus-scan confirmation for large file downloads. API keys are auto-discovered if the embedded key rotates.

### 3. Extract text to JSON

```bash
python -m src.extractor
```

Processes all files across all 28 datasets (DOJ PDFs, Google Drive images, and media) and creates searchable JSON files in `data/`. Non-PDF files are indexed by filename for searchability. Only errors are printed during extraction; the summary shows total files, pages, size, and per-dataset failures.

### 4. Generate thumbnails

```bash
python -m src.thumbnails                     # All datasets
python -m src.thumbnails --dataset 1         # Dataset 1 only
python -m src.thumbnails --dataset 1 3 5     # Specific datasets
python -m src.thumbnails --workers 4         # Concurrent threads
python -m src.thumbnails --width 800         # Custom width (px)
python -m src.thumbnails --force             # Regenerate existing
```

Generates thumbnails for all datasets into `data/thumbnails/`. PDF pages are rendered via PyMuPDF, JPG/TIF images are resized via Pillow, and media files (MP4/WAV) are skipped. Datasets 1-12 are DOJ PDFs, 13-24 are Google Drive images, 25-28 are media. Only errors are printed; the summary includes per-dataset failure counts.

### 5. Classify images (optional)

```bash
python -m src.classifier                     # All datasets
python -m src.classifier --dataset 1         # Dataset 1 only
python -m src.classifier --dataset 1,3,7-11  # Ranges and lists
python -m src.classifier --workers 10        # Concurrent API calls
python -m src.classifier --rpm 2000          # Rate limit (requests/min)
python -m src.classifier --max-cost 50       # Stop after $50 spent
python -m src.classifier --force             # Reclassify existing
python -m src.classifier --dry-run           # Count and estimate cost
```

Uses Google Gemini 2.0 Flash to classify thumbnail images with description, tags, content type, and recognized people. Requires `GOOGLE_API_KEY` environment variable. Results stored in `data/classifications/data-set-N.json`.

### 6. Sync to Google Cloud Storage

```bash
python -m src.gcs_sync                       # Sync everything to GCS
python -m src.gcs_sync --pdfs                # PDFs/videos/GDrive files only
python -m src.gcs_sync --thumbnails          # Thumbnails only
python -m src.gcs_sync --classifications     # Classification JSONs only
python -m src.gcs_sync --dry-run             # Preview without uploading
```

Uploads new or changed files to the GCS bucket (`gs://epstein-doj-files-jarheads`). Uses checksum-based diffing so only modified files are transferred. Run this after downloading new files, generating thumbnails, or running the classifier.

### 7. Build search index

```bash
python -m src.build_index                    # Build SQLite FTS5 index
python -m src.build_index --force            # Rebuild from scratch
```

Creates the SQLite FTS5 full-text search index from extracted JSON data. The index is saved to `data/epstein_search.db` and used by the web server for search queries.

### 8. Initialize MySQL database (optional)

```bash
python -m src.init_db --host HOST --user USER --password PASS --database DB
python -m src.init_db --drop                 # Drop and recreate tables
python -m src.init_db --populate-files       # Populate file inventory
```

Creates MySQL tables for analytics: `search_queries`, `page_views`, and `files`. When the server runs with `DATABASE_URL` set, search queries and page views are logged automatically.

### 9. Deploy to production

```bash
gcloud run deploy epstein-server --source . \
  --project=epstein-doj-files --region=us-central1 \
  --allow-unauthenticated
```

Builds a Docker image via Cloud Build (downloads SQLite DB and classifications from GCS during build) and deploys to Cloud Run. The service is available at `https://epstein.jarheads.net`. PDFs and thumbnails are served directly from GCS.

### 10. Search

**Web interface:**

```bash
python -m src.server
```

Opens at `http://localhost:8000`. The server auto-reloads when source files change. Pages available:

- **Search** — Full-text search with highlighted results and inline PDF viewer
- **Gallery** — Thumbnail browser with tag autocomplete, content type, and person filters
- **Logs** — Structured log viewer with search and level/module filtering

**Command line:**

```bash
python -m src.search "Maxwell"                   # Single term
python -m src.search "Maxwell AND island"        # Both terms
python -m src.search "Maxwell OR Epstein"        # Either term
python -m src.search "Maxwell NOT flight"        # Exclusion
python -m src.search '"grand jury"'              # Quoted phrase
python -m src.search "Epstein NEAR/5 island"     # Proximity search
python -m src.search "Maxwell" --dataset 1       # Filter by dataset
python -m src.search "Maxwell" --sort relevance  # Sort results
python -m src.search "Epstein" --export csv      # Export as CSV/JSON
python -m src.search                             # Interactive mode
```

## Operations Guide

The pipeline has a strict dependency order. Always run steps in the order shown — later steps depend on output from earlier ones.

### Pipeline Overview

```
Download → Extract → Build Index → Thumbnails → Classify → GCS Sync → Deploy
   (1)       (2)        (3)          (4)          (5)        (6)        (7)
```

- **Steps 1-3 are required** for search to work (extract produces JSON, build_index creates SQLite DB)
- **Step 4** is required for gallery/image browsing
- **Step 5** is optional (AI classification costs ~$0.20/1000 images)
- **Steps 6-7** push changes to production

### Scenario A: New DOJ Dataset Released

When a new dataset appears on [justice.gov/epstein](https://www.justice.gov/epstein/doj-disclosures):

```bash
# 1. Download the new dataset (replace N with the dataset number)
python -m src.downloader --dataset N

# 2. Re-extract all files (rebuilds JSON with new dataset included)
python -m src.extractor

# 3. Rebuild search index (must run after extractor)
python -m src.build_index --force

# 4. Generate thumbnails for the new dataset
python -m src.thumbnails --dataset N

# 5. Classify new thumbnails (optional, requires GOOGLE_API_KEY)
python -m src.classifier --dataset N

# 6. Upload new files to GCS
python -m src.gcs_sync --pdfs              # Upload PDFs
python -m src.gcs_sync --thumbnails        # Upload thumbnails
python -m src.gcs_sync --classifications   # Upload classifications (if step 5 ran)
gcloud storage cp data/epstein_search.db gs://epstein-doj-files-jarheads/data/epstein_search.db

# 7. Deploy updated server
gcloud run deploy epstein-server --source . \
  --project=epstein-doj-files --region=us-central1 \
  --allow-unauthenticated
```

### Scenario B: New Google Drive Files Added

When new files appear in the shared Google Drive folder:

```bash
# 1. Download new files
python -m src.gdrive_downloader

# 2. Re-extract (includes new GDrive files in JSON)
python -m src.extractor

# 3. Rebuild search index
python -m src.build_index --force

# 4. Generate thumbnails for image datasets (13-24)
python -m src.thumbnails --dataset 13 14 15 16 17 18 19 20 21 22 23 24

# 5. Classify new thumbnails (optional)
python -m src.classifier --dataset 13-24

# 6. Upload to GCS
python -m src.gcs_sync                     # Sync all file types
gcloud storage cp data/epstein_search.db gs://epstein-doj-files-jarheads/data/epstein_search.db

# 7. Deploy
gcloud run deploy epstein-server --source . \
  --project=epstein-doj-files --region=us-central1 \
  --allow-unauthenticated
```

### Scenario C: Code-Only Changes (No New Files)

When you've modified source code or HTML but haven't added new documents:

```bash
# Just redeploy — no pipeline steps needed
gcloud run deploy epstein-server --source . \
  --project=epstein-doj-files --region=us-central1 \
  --allow-unauthenticated
```

### Scenario D: Re-classify Existing Images

To re-run classification (e.g., after improving the prompt or switching models):

```bash
# Reclassify specific datasets (--force overwrites existing classifications)
python -m src.classifier --dataset 1-12 --force

# Upload updated classifications
python -m src.gcs_sync --classifications

# Redeploy (Docker build downloads fresh classifications from GCS)
gcloud run deploy epstein-server --source . \
  --project=epstein-doj-files --region=us-central1 \
  --allow-unauthenticated
```

### Scenario E: Check What's Currently Downloaded

```bash
# Count local PDFs per dataset
for i in $(seq 1 12); do echo "data-set-$i: $(ls epstein_doj_files/data-set-$i/*.pdf 2>/dev/null | wc -l) files"; done

# Count Google Drive files
find epstein_doj_files/Google_Drive_Files -type f | wc -l

# Check search index stats
python -c "import sqlite3; c=sqlite3.connect('data/epstein_search.db'); print(c.execute('SELECT COUNT(*) FROM documents').fetchone()[0], 'documents indexed')"

# Dry-run the downloader to see if justice.gov has new files
python -m src.downloader --dry-run
```

### Quick Reference: Step Dependencies

| Step | Command | Depends On | Produces |
|------|---------|------------|----------|
| Download DOJ | `python -m src.downloader` | — | `epstein_doj_files/data-set-N/*.pdf` |
| Download GDrive | `python -m src.gdrive_downloader` | — | `epstein_doj_files/Google_Drive_Files/` |
| Extract | `python -m src.extractor` | Downloaded files | `data/*.json` |
| Build Index | `python -m src.build_index --force` | `data/*.json` | `data/epstein_search.db` |
| Thumbnails | `python -m src.thumbnails` | Downloaded files | `data/thumbnails/data-set-N/` |
| Classify | `python -m src.classifier` | Thumbnails | `data/classifications/data-set-N.json` |
| GCS Sync | `python -m src.gcs_sync` | Any of the above | Files on GCS bucket |
| Deploy | `gcloud run deploy ...` | GCS sync (for DB + classifications) | Live Cloud Run service |

## Project Structure

```
src/
  config.py            — Centralized paths, ports, and settings
  downloader.py        — Playwright-based PDF downloader
  gdrive_downloader.py — Google Drive shared folder downloader
  extractor.py         — PDF to JSON converter (Poppler)
  extractor_plumber.py — Alternative extractor using pdfplumber
  classifier.py        — AI image classification (Gemini Flash)
  thumbnails.py        — Batch PDF thumbnail generator (PyMuPDF)
  search.py            — CLI search with AND/OR/NOT/NEAR and page references
  server.py            — FastAPI server with security headers and auto-reload
  build_index.py       — SQLite FTS5 index builder
  init_db.py           — MySQL schema creation and file inventory population
  deploy.py            — rsync-based deployment to remote host
  gcs_sync.py          — Google Cloud Storage file sync
  logging_setup.py     — Structured JSONL logging configuration
static/
  search.html          — Web search interface
  gallery.html         — Thumbnail gallery with lightbox
  logs.html            — Log viewer interface
  dashboard.html       — Admin dashboard with command runner
scripts/
  start.sh             — Shell launcher (macOS/Linux)
  start.bat            — Shell launcher (Windows)
docs/                  — Documentation guides
data/                  — Generated JSON, thumbnails, classifications, logs (gitignored)
epstein_doj_files/     — Downloaded PDFs, videos, and Google Drive files (gitignored)
```

## Logging

All modules log to `data/logs/app.jsonl` in structured JSON Lines format. Logs include timestamps, levels, module names, and contextual data. Each tool logs `_started` and `_complete` lifecycle events with run configuration and results. The log file rotates at 50 MB with 5 backups. View logs via the web interface at `/logs.html` or search the JSONL file directly.

## Security

The local server includes several hardening measures:

- CORS restricted to localhost and private network origins
- Content-Security-Policy, X-Frame-Options, X-Content-Type-Options headers
- Path traversal protection (realpath validation)
- File extension allowlist (`.html`, `.json`, `.pdf`, `.css`, `.js`, `.png`, `.jpg`, `.jpeg`, `.ico`, `.mp4`, `.tif`, `.wav`)
- Sentry error tracking (optional, via `SENTRY_DSN` env var)

## Environment Variables

| Variable | Description |
|----------|-------------|
| `GCS_BASE_URL` | GCS public URL for file serving (e.g. `https://storage.googleapis.com/epstein-doj-files-jarheads`) |
| `DATABASE_URL` | MySQL connection string for analytics logging (`mysql+pymysql://user:pass@host/db`) |
| `SENTRY_DSN` | Sentry error tracking DSN |
| `BASE_PATH` | URL prefix for reverse proxy (e.g. `/epstein-DOJ-files`) |
| `DEPLOY_HOST` | Remote SSH host for deployment (default: `jarheads@162.241.218.175`) |
| `DEPLOY_DIR` | Remote deployment directory (default: `~/epstein_server`) |
| `GOOGLE_API_KEY` | Google API key for Gemini classifier |

## License

This project is a tool for accessing publicly released government documents. The documents themselves are public records from the U.S. Department of Justice.
