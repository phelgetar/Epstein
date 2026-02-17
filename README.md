# Epstein DOJ Files

Tools for downloading, extracting, searching, and browsing the publicly released DOJ disclosure documents related to the Epstein case from [justice.gov](https://www.justice.gov/epstein/doj-disclosures).

## Features

- **Downloader** — Playwright-based PDF downloader with stealth patches to bypass Akamai CDN, batched pagination, and multithreaded downloads
- **Extractor** — Converts downloaded PDFs to searchable JSON using Poppler (`pdftotext`/`pdfinfo`), with page-level offsets
- **Thumbnails** — Batch JPEG thumbnail generator for all PDF pages using PyMuPDF
- **Classifier** — AI image classification using OpenAI GPT-4o for tagging and person recognition
- **CLI Search** — Full-text search with AND/OR operators, regex support, and page number references
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
pip install fastapi uvicorn watchdog playwright playwright-stealth requests pymupdf python-dotenv sentry-sdk openai

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

### 2. Extract text to JSON

```bash
python -m src.extractor
```

Processes all PDFs and creates searchable JSON files in `data/`. Only errors are printed during extraction; the summary shows total files, pages, size, and per-dataset failures.

### 3. Generate thumbnails

```bash
python -m src.thumbnails                     # All datasets
python -m src.thumbnails --dataset 1         # Dataset 1 only
python -m src.thumbnails --dataset 1 3 5     # Specific datasets
python -m src.thumbnails --workers 4         # Concurrent threads
python -m src.thumbnails --width 800         # Custom width (px)
python -m src.thumbnails --force             # Regenerate existing
```

Renders every page of every PDF as a JPEG thumbnail into `data/thumbnails/`. Only errors are printed; the summary includes per-dataset failure counts.

### 4. Classify images (optional)

```bash
python -m src.classifier                     # All datasets
python -m src.classifier --dataset 1         # Dataset 1 only
python -m src.classifier --cost-cap 5.00     # Max spend in USD
```

Uses OpenAI GPT-4o to tag thumbnail images with content type, tags, and recognized people. Requires `OPENAI_API_KEY` environment variable.

### 5. Search

**Web interface:**

```bash
python -m src.server
```

Opens at `http://127.0.0.1:8000`. The server auto-reloads when source files change. Pages available:

- **Search** — Full-text search with highlighted results and inline PDF viewer
- **Gallery** — Thumbnail browser with tag autocomplete, content type, and person filters
- **Logs** — Structured log viewer with search and level/module filtering

**Command line:**

```bash
python -m src.search "query"           # single term
python -m src.search "term1 AND term2" # both terms
python -m src.search "term1 OR term2"  # either term
python -m src.search                   # interactive mode
```

## Project Structure

```
src/
  config.py            — Centralized paths, ports, and settings
  downloader.py        — Playwright-based PDF downloader
  extractor.py         — PDF to JSON converter (Poppler)
  extractor_plumber.py — Alternative extractor using pdfplumber
  classifier.py        — AI image classification (OpenAI GPT-4o)
  thumbnails.py        — Batch PDF thumbnail generator (PyMuPDF)
  search.py            — CLI search with AND/OR and page references
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
epstein_doj_files/     — Downloaded PDFs (gitignored)
```

## Logging

All modules log to `data/logs/app.jsonl` in structured JSON Lines format. Logs include timestamps, levels, module names, and contextual data. The log file rotates at 50 MB with 5 backups. View logs via the web interface at `/logs.html` or search the JSONL file directly.

## Security

The local server includes several hardening measures:

- Binds to 127.0.0.1 only
- CORS restricted to localhost and private network origins
- Content-Security-Policy, X-Frame-Options, X-Content-Type-Options headers
- Path traversal protection (realpath validation)
- File extension allowlist
- Sentry error tracking (optional, via `SENTRY_DSN` env var)

## License

This project is a tool for accessing publicly released government documents. The documents themselves are public records from the U.S. Department of Justice.
