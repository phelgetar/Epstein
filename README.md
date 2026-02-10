# Epstein DOJ Files

Tools for downloading, extracting, and searching the publicly released DOJ disclosure documents related to the Epstein case from [justice.gov](https://www.justice.gov/epstein/doj-disclosures).

## Features

- **Downloader** — Automated browser-based PDF downloader with barrier handling (age verification, cookie prompts)
- **Extractor** — Converts downloaded PDFs to searchable JSON using Poppler (`pdftotext`/`pdfinfo`), with page-level offsets
- **CLI Search** — Full-text search with AND/OR operators, regex support, and page number references
- **Web Interface** — Browser-based search UI with highlighted results and inline PDF viewing

## Prerequisites

- Python 3.8+
- [Poppler](https://poppler.freedesktop.org/) (for PDF text extraction)
- Selenium + Chrome (for downloading only)

```bash
# macOS
brew install poppler

# Ubuntu/Debian
sudo apt install poppler-utils

# Python dependencies
pip install watchdog          # auto-reload for dev server
pip install selenium requests # only needed for downloading
```

## Quick Start

### 1. Download PDFs

```bash
python -m src.downloader
```

Downloads all 12 data sets from justice.gov into `epstein_doj_files/`. Requires Chrome and Selenium.

### 2. Extract text to JSON

```bash
python -m src.extractor
```

Processes all PDFs and creates searchable JSON files in `data/`.

### 3. Search

**Web interface:**

```bash
python -m src.server
```

Opens a browser to the search page at `http://127.0.0.1:8000`. Accessible on your LAN.

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
  config.py      — Centralized paths, ports, and settings
  downloader.py  — Selenium-based PDF downloader
  extractor.py   — PDF to JSON converter (Poppler)
  search.py      — CLI search with AND/OR and page references
  server.py      — HTTP server with security headers and auto-reload
static/
  search.html    — Web search interface
scripts/
  start.sh       — Shell launcher (macOS/Linux)
  start.bat      — Shell launcher (Windows)
docs/            — Documentation guides
```

## Security

The local server includes several hardening measures:

- CORS restricted to localhost and private network origins
- Content-Security-Policy, X-Frame-Options, X-Content-Type-Options headers
- Path traversal protection (realpath validation)
- File extension allowlist

## License

This project is a tool for accessing publicly released government documents. The documents themselves are public records from the U.S. Department of Justice.