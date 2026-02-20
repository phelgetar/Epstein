# Epstein DOJ Files

Tools for downloading, extracting, searching, and browsing the publicly released DOJ disclosure documents related to the Epstein case from [justice.gov](https://www.justice.gov/epstein/doj-disclosures).

## Features

- **Downloader** — Playwright-based PDF downloader with stealth patches to bypass Akamai CDN, batched pagination, and multithreaded downloads
- **Video Downloader** — Downloads video files from the DOJ search page, saving as `.mp4`
- **MP4 Checker** — Scans existing PDF datasets for corresponding `.mp4` companion files on the server
- **Google Drive Downloader** — Downloads files from shared Google Drive folders using the internal Drive API for complete file listings (no 50-file cap) with auto-discovery of API keys
- **Extractor** — Converts downloaded PDFs to searchable JSON using Poppler (`pdftotext`/`pdfinfo`), with page-level offsets
- **Thumbnails** — Batch JPEG thumbnail generator for all PDF pages using PyMuPDF
- **Classifier** — AI image classification using Google Gemini 2.0 Flash for tagging and person recognition
- **CLI Search** — Full-text search with AND/OR/NOT operators, NEAR/N proximity, quoted phrases, and page references
- **Web Interface** — Browser-based search UI with highlighted results and inline PDF viewing
- **Gallery** — Thumbnail gallery with lightbox viewer, tag autocomplete, content type and person filtering
- **Log Viewer** — Searchable structured log viewer with level/module filtering

## Prerequisites

- Python 3.8+
- [Poppler](https://poppler.freedesktop.org/) (for PDF text extraction)
- Playwright + Chromium (for downloading)

```bash
# macOS
brew install poppler

# Ubuntu/Debian
sudo apt install poppler-utils

# Python dependencies
pip install -r requirements.txt
# or manually:
pip install fastapi uvicorn watchdog playwright playwright-stealth requests beautifulsoup4 pymupdf google-genai pydantic sentry-sdk

# Install Playwright browsers
playwright install chromium
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

### 2. Download Videos

```bash
python -m src.video_downloader                                # Download all (default query)
python -m src.video_downloader --query "No Images Produced"   # Custom search query
python -m src.video_downloader --workers 10                   # Concurrent threads
python -m src.video_downloader --batch-size 20                # Pages per batch
python -m src.video_downloader --dry-run                      # Count only
```

Searches the DOJ search page for video files linked as `.pdf`, downloads them, and saves as `.mp4` in `epstein_doj_files/videos/`.

### 3. Download Google Drive Files

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

### 4. Check for MP4 Companions

```bash
python -m src.mp4_checker                    # Check datasets 8, 9, 10
python -m src.mp4_checker --dataset 8        # Dataset 8 only
python -m src.mp4_checker --dataset 8 9 10   # Explicit datasets
python -m src.mp4_checker --workers 5        # Concurrent threads
python -m src.mp4_checker --delay 1          # 1s between probe requests
python -m src.mp4_checker --dry-run          # Check without downloading
```

Scans dataset pages for PDF URLs and checks whether a corresponding `.mp4` version exists on the DOJ server. Downloads any found into the dataset directory alongside the PDFs.

### 5. Extract text to JSON

```bash
python -m src.extractor
```

Processes all PDFs and creates searchable JSON files in `data/`. Only errors are printed during extraction; the summary shows total files, pages, size, and per-dataset failures.

### 6. Generate thumbnails

```bash
python -m src.thumbnails                     # All datasets
python -m src.thumbnails --dataset 1         # Dataset 1 only
python -m src.thumbnails --dataset 1 3 5     # Specific datasets
python -m src.thumbnails --workers 4         # Concurrent threads
python -m src.thumbnails --width 800         # Custom width (px)
python -m src.thumbnails --force             # Regenerate existing
```

Renders every page of every PDF as a JPEG thumbnail into `data/thumbnails/`. Only errors are printed; the summary includes per-dataset failure counts.

### 7. Classify images (optional)

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

### 8. Search

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

## Project Structure

```
src/
  config.py            — Centralized paths, ports, and settings
  downloader.py        — Playwright-based PDF downloader
  video_downloader.py  — Video downloader from DOJ search page
  mp4_checker.py       — MP4 companion file checker/downloader
  gdrive_downloader.py — Google Drive shared folder downloader
  extractor.py         — PDF to JSON converter (Poppler)
  extractor_plumber.py — Alternative extractor using pdfplumber
  classifier.py        — AI image classification (Gemini Flash)
  thumbnails.py        — Batch PDF thumbnail generator (PyMuPDF)
  search.py            — CLI search with AND/OR/NOT/NEAR and page references
  server.py            — FastAPI server with security headers and auto-reload
  logging_setup.py     — Structured JSONL logging configuration
static/
  search.html          — Web search interface
  gallery.html         — Thumbnail gallery with lightbox
  logs.html            — Log viewer interface
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
- File extension allowlist (`.html`, `.json`, `.pdf`, `.css`, `.js`, `.png`, `.jpg`, `.ico`, `.mp4`)
- Sentry error tracking (optional, via `SENTRY_DSN` env var)

## License

This project is a tool for accessing publicly released government documents. The documents themselves are public records from the U.S. Department of Justice.
