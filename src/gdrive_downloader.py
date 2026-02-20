#!/usr/bin/env python3
"""
Epstein DOJ Files — Google Drive Downloader.

Downloads files from a shared Google Drive folder, preserving the folder
structure. Uses Google Drive's internal API for complete file listings
(no browser needed), then downloads each file individually via gdown.

The shared folder structure:
    IMAGES/
        IMAGES001/ ... IMAGES012/
    NATIVES/
        NATIVE006, NATIVE008, NATIVE011, NATIVE012

Usage:
    python -m src.gdrive_downloader                    # Download everything
    python -m src.gdrive_downloader --dry-run          # List folders + file counts
    python -m src.gdrive_downloader --folder IMAGES    # Download IMAGES only
    python -m src.gdrive_downloader --folder NATIVES   # Download NATIVES only
    python -m src.gdrive_downloader --workers 5        # Concurrent downloads
"""

import argparse
import json
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import GDRIVE_DIR, GDRIVE_FOLDER_ID

logger = logging.getLogger(__name__)

DRIVE_FOLDER_URL = "https://drive.google.com/drive/folders"

# Google Drive web app's built-in API key (public, embedded in Drive's JS).
# These keys rotate periodically — discover_api_key() extracts fresh ones.
DRIVE_API_KEY = "AIzaSyAWGrfCCr7albM3lmCc937gx4uIphbpeKQ"

FOLDER_MIME = "application/vnd.google-apps.folder"


# ─── API Key Discovery ────────────────────────────────────────

def discover_api_key(folder_id):
    """Extract a working API key from the Google Drive page source.

    Google rotates keys periodically. This function scrapes the Drive
    folder page, extracts all AIzaSy* keys, and tests each one until
    it finds one that returns data for the given folder.

    Returns a working key string, or None if none work.
    """
    try:
        resp = requests.get(
            f"{DRIVE_FOLDER_URL}/{folder_id}",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
            },
            timeout=30,
        )
        if resp.status_code != 200:
            return None

        # Extract unique API keys from page source
        keys = list(dict.fromkeys(
            re.findall(r'"(AIzaSy[A-Za-z0-9_-]{30,})"', resp.text)
        ))

        # Test each key with a minimal query
        for key in keys:
            try:
                test = requests.get(
                    "https://clients6.google.com/drive/v2beta/files",
                    params={
                        "openDrive": "true",
                        "reason": "102",
                        "syncType": "0",
                        "errorRecovery": "false",
                        "q": f"trashed=false and '{folder_id}' in parents",
                        "fields": "items(id)",
                        "maxResults": 1,
                        "key": key,
                        "supportsTeamDrives": "true",
                        "includeTeamDriveItems": "true",
                        "corpora": "default",
                        "retryCount": "0",
                    },
                    headers={
                        "Referer": "https://drive.google.com/",
                        "Origin": "https://drive.google.com",
                    },
                    timeout=10,
                )
                if test.status_code == 200:
                    return key
            except Exception:
                continue

    except Exception:
        pass

    return None


# ─── Folder Discovery (HTML scraping for structure) ──────────

def list_folder_html(folder_id):
    """List immediate children via the embedded folder view.

    Only returns up to ~50 items — used for discovering the folder
    structure (subfolders), not for listing all files.

    Returns list of dicts with 'name', 'id', 'type' ('folder' or 'file').
    """
    url = f"https://drive.google.com/embeddedfolderview?id={folder_id}#list"
    resp = requests.get(url, timeout=30)
    if resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    entries = []
    for entry in soup.select("div.flip-entry"):
        name_el = entry.select_one("div.flip-entry-title")
        if not name_el:
            continue
        name = name_el.text.strip()

        link = entry.select_one("a")
        href = link.get("href", "") if link else ""

        entry_id = ""
        entry_type = "file"
        if "folders/" in href:
            entry_id = href.split("folders/")[-1].split("?")[0]
            entry_type = "folder"
        elif "id=" in href:
            entry_id = href.split("id=")[-1].split("&")[0]

        entries.append({"name": name, "id": entry_id, "type": entry_type})

    return entries


# ─── Complete File Listing (Drive Internal API) ──────────────

def _drive_api_headers():
    """Headers matching what the Google Drive web UI sends."""
    return {
        "Referer": "https://drive.google.com/",
        "Origin": "https://drive.google.com",
        "X-Goog-Drive-Client-Version": "drive.web-frontend_20260210",
        "Accept": "application/json",
    }


def _drive_api_get(folder_id, query, fields, page_token=None, verbose=False):
    """Make a single request to Google Drive's internal v2beta API.

    Returns (data_dict, error_string). On success error is None.
    """
    params = {
        "openDrive": "true",
        "reason": "102",
        "syncType": "0",
        "errorRecovery": "false",
        "q": query,
        "fields": fields,
        "maxResults": 100,
        "key": DRIVE_API_KEY,
        "supportsTeamDrives": "true",
        "includeTeamDriveItems": "true",
        "corpora": "default",
        "orderBy": "folder,title_natural asc",
        "retryCount": "0",
    }
    if page_token:
        params["pageToken"] = page_token

    resp = requests.get(
        "https://clients6.google.com/drive/v2beta/files",
        params=params,
        headers=_drive_api_headers(),
        timeout=30,
    )

    if resp.status_code != 200:
        error_body = ""
        try:
            error_body = resp.text[:500]
        except Exception:
            pass
        msg = f"HTTP {resp.status_code} for {folder_id}"
        if verbose and error_body:
            msg += f"\n        Response: {error_body}"
        return None, msg

    return resp.json(), None


def list_files_api(folder_id, verbose=False):
    """List ALL files in a Google Drive folder via the internal API.

    Uses the same API endpoint + key that the Drive web app uses.
    Handles pagination via nextPageToken so there is no cap on file count.
    Skips subfolder entries (mimeType = folder).

    Returns list of dicts with 'id', 'name'.
    """
    files = []
    page_token = None
    query = f"trashed=false and '{folder_id}' in parents"
    fields = "nextPageToken,items(id,title,fileSize,mimeType)"

    while True:
        try:
            data, error = _drive_api_get(
                folder_id, query, fields, page_token, verbose
            )
            if error:
                logger.warning(f"Drive API: {error}")
                if verbose:
                    print(f"\n        {error}")
                return files

            items = data.get("items", [])
            for item in items:
                mime = item.get("mimeType", "")
                if mime == FOLDER_MIME:
                    continue
                files.append({
                    "id": item["id"],
                    "name": item.get("title", item["id"]),
                })

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        except Exception as e:
            logger.warning(f"Drive API error for {folder_id}: {e}")
            return files

    return files


def list_folders_api(folder_id, verbose=False):
    """List subfolders in a Google Drive folder via the internal API.

    Returns list of dicts with 'id', 'name'.
    """
    folders = []
    page_token = None
    query = (
        f"trashed=false and '{folder_id}' in parents"
        f" and mimeType='{FOLDER_MIME}'"
    )
    fields = "nextPageToken,items(id,title)"

    while True:
        try:
            data, error = _drive_api_get(
                folder_id, query, fields, page_token, verbose
            )
            if error:
                if verbose:
                    print(f"\n        {error}")
                break

            for item in data.get("items", []):
                folders.append({
                    "id": item["id"],
                    "name": item.get("title", item["id"]),
                })

            page_token = data.get("nextPageToken")
            if not page_token:
                break

        except Exception:
            break

    return folders


# ─── File Download ────────────────────────────────────────────

def download_file(file_id, file_name, output_path, session, delay=0):
    """Download a single file from Google Drive.

    For small files, the initial request returns the file directly.
    For large files, Google shows a virus-scan confirmation page — we
    parse the form to extract the uuid token, then re-request with it.

    Returns (filename, success, message).
    """
    if output_path.exists() and output_path.stat().st_size > 0:
        return file_name, True, "skip"

    if delay > 0:
        time.sleep(delay)

    try:
        url = f"https://drive.google.com/uc?id={file_id}&export=download"
        resp = session.get(url, stream=True, timeout=120)

        if resp.status_code != 200:
            return file_name, False, (
                f"  Failed: {file_name} (HTTP {resp.status_code})"
            )

        # Check if we got the actual file or a confirmation page
        ct = resp.headers.get("content-type", "")
        if "text/html" in ct:
            # Large file — parse the confirmation form for uuid
            html = resp.content.decode("utf-8", errors="replace")
            soup = BeautifulSoup(html, "html.parser")
            form = soup.find("form")
            if form:
                action = form.get("action", "")
                fields = {}
                for inp in form.find_all("input"):
                    name = inp.get("name")
                    if name:
                        fields[name] = inp.get("value", "")

                resp = session.get(
                    action, params=fields, stream=True, timeout=300,
                )
                if resp.status_code != 200:
                    return file_name, False, (
                        f"  Failed: {file_name} (confirm HTTP {resp.status_code})"
                    )
            else:
                return file_name, False, f"  Failed: {file_name} (no confirm form)"

        # Stream to disk
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)

        size = output_path.stat().st_size
        if size == 0:
            output_path.unlink()
            return file_name, False, f"  Empty: {file_name}"

        logger.info("gdrive_file_downloaded", extra={"data": {
            "filename": file_name, "file_id": file_id, "size_bytes": size,
        }})
        return file_name, True, f"  Downloaded: {file_name} ({size:,} bytes)"

    except Exception as e:
        # Clean up partial file
        if output_path.exists():
            output_path.unlink()
        logger.info("gdrive_file_error", extra={"data": {
            "filename": file_name, "file_id": file_id, "error": str(e),
        }})
        return file_name, False, f"  Error: {file_name} — {e}"


def download_files(files, output_dir, workers, delay=0):
    """Download a list of files with a thread pool.

    Returns (downloaded, skipped, failed) counts.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({
        "Referer": "https://drive.google.com/",
        "Origin": "https://drive.google.com",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
    })

    downloaded = 0
    skipped = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for f in files:
            output_path = output_dir / f["name"]
            future = pool.submit(
                download_file, f["id"], f["name"], output_path, session, delay
            )
            futures[future] = f["name"]

        for future in as_completed(futures):
            name, success, message = future.result()
            if success:
                if message == "skip":
                    skipped += 1
                else:
                    downloaded += 1
                    print(message)
            else:
                failed += 1
                print(message)

    return downloaded, skipped, failed


# ─── Main ─────────────────────────────────────────────────────

def main():
    from src.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Epstein DOJ Files — Google Drive Downloader",
        epilog="Examples:\n"
               "  python -m src.gdrive_downloader                    # Download all\n"
               "  python -m src.gdrive_downloader --dry-run          # List structure\n"
               "  python -m src.gdrive_downloader --folder IMAGES    # IMAGES only\n"
               "  python -m src.gdrive_downloader --folder NATIVES   # NATIVES only\n"
               "  python -m src.gdrive_downloader --workers 5        # 5 concurrent\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--folder", type=str,
        help="Download only this top-level folder (e.g. IMAGES, NATIVES)",
    )
    parser.add_argument(
        "--workers", type=int, default=5,
        help="Concurrent download threads (default: 5)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="List folder structure and file counts without downloading",
    )
    parser.add_argument(
        "--delay", type=float, default=0,
        help="Seconds to wait between downloads (rate limiting)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Show detailed debug output during file listing",
    )
    args = parser.parse_args()

    print("=" * 70)
    print("Epstein DOJ Files — Google Drive Downloader")
    print("=" * 70)
    print(f"  Source:     drive.google.com/.../{GDRIVE_FOLDER_ID}")
    print(f"  Output:     {GDRIVE_DIR.resolve()}")
    print(f"  Workers:    {args.workers}")
    if args.delay:
        print(f"  Delay:      {args.delay}s")
    if args.folder:
        print(f"  Filter:     {args.folder}")
    if args.dry_run:
        print("  Mode:       DRY RUN")
    print()

    logger.info("gdrive_downloader_started", extra={"data": {
        "folder_id": GDRIVE_FOLDER_ID,
        "output_dir": str(GDRIVE_DIR.resolve()),
        "folder_filter": args.folder,
        "workers": args.workers,
        "dry_run": args.dry_run,
    }})

    # Validate API key (auto-discover if the hardcoded one is stale)
    global DRIVE_API_KEY
    print("  Checking API key...", end="", flush=True)
    test_folders = list_folders_api(GDRIVE_FOLDER_ID, verbose=False)
    if test_folders:
        print(" OK")
    else:
        print(" stale, discovering fresh key...", end="", flush=True)
        new_key = discover_api_key(GDRIVE_FOLDER_ID)
        if new_key:
            DRIVE_API_KEY = new_key
            print(f" found ({new_key[:12]}...)")
        else:
            print(" failed (will use HTML fallback)")

    # Discover folder structure via API (no browser needed)
    print("  Discovering folder structure...")

    # Try API first for folder discovery, fall back to HTML scraping
    top_folders = list_folders_api(GDRIVE_FOLDER_ID, verbose=args.verbose)
    if top_folders:
        print("  (using Drive API)")
    else:
        print("  (API unavailable, falling back to HTML scraping)")
        top_entries = list_folder_html(GDRIVE_FOLDER_ID)
        if not top_entries:
            print("  Error: Could not list folder contents. Check the link.")
            sys.exit(1)
        top_folders = [e for e in top_entries if e["type"] == "folder"]

    # Apply folder filter
    if args.folder:
        top_folders = [f for f in top_folders
                       if f["name"].upper() == args.folder.upper()]
        if not top_folders:
            print(f"  No folder matching '{args.folder}' found.")
            sys.exit(1)

    # Discover subfolders
    download_targets = []  # list of (path_label, folder_id)
    for top in top_folders:
        sub_folders = list_folders_api(top["id"], verbose=args.verbose)
        if not sub_folders:
            # Fall back to HTML scraping
            sub_entries = list_folder_html(top["id"])
            sub_folders = [{"id": e["id"], "name": e["name"]}
                           for e in sub_entries if e["type"] == "folder"]
            sub_has_files = any(e["type"] == "file" for e in sub_entries)
        else:
            # Check if the folder itself has files (not just subfolders)
            direct_files = list_files_api(top["id"], verbose=args.verbose)
            sub_has_files = len(direct_files) > 0

        print(f"\n  {top['name']}/")

        if sub_has_files and not sub_folders:
            # Flat folder with files directly inside
            download_targets.append((top["name"], top["id"]))
            print(f"    (files at root)")
        elif sub_has_files:
            download_targets.append((top["name"], top["id"]))
            print(f"    (files at root)")

        for sf in sorted(sub_folders, key=lambda x: x["name"]):
            download_targets.append((f"{top['name']}/{sf['name']}", sf["id"]))
            print(f"    {sf['name']}/")

    if not download_targets:
        print("  No download targets found.")
        sys.exit(1)

    # List all files in each target folder via API
    print(f"\n  Listing files in {len(download_targets)} folders...")

    folder_data = []  # (path_label, folder_id, files_list)
    total_files = 0

    for path_label, fid in download_targets:
        print(f"    {path_label}...", end="", flush=True)
        files = list_files_api(fid, verbose=args.verbose)
        print(f" {len(files)} files")

        if args.verbose and files:
            for f in files[:5]:
                print(f"      {f['name']}  (id: {f['id'][:12]}...)")
            if len(files) > 5:
                print(f"      ... and {len(files) - 5} more")

        folder_data.append((path_label, fid, files))
        total_files += len(files)

    print(f"\n  Total: {total_files:,} files across {len(folder_data)} folders")

    if args.dry_run:
        print(f"\n{'=' * 70}")
        print("  Dry run complete.")
        print(f"{'=' * 70}")
        return

    # Download all files
    GDRIVE_DIR.mkdir(parents=True, exist_ok=True)

    grand_downloaded = 0
    grand_skipped = 0
    grand_failed = 0

    for path_label, fid, files in folder_data:
        output_dir = GDRIVE_DIR / Path(path_label)

        # Check existing
        existing = 0
        if output_dir.exists():
            existing = len([f for f in output_dir.iterdir() if f.is_file()])

        if existing >= len(files) and len(files) > 0:
            print(f"\n  -- {path_label}: {existing} files already present, "
                  f"skipping --")
            grand_skipped += len(files)
            continue

        print(f"\n  -- {path_label} ({len(files)} files"
              f"{f', {existing} already present' if existing else ''}) --")

        dl, sk, fl = download_files(files, output_dir, args.workers, args.delay)
        grand_downloaded += dl
        grand_skipped += sk
        grand_failed += fl

        logger.info("gdrive_subfolder_complete", extra={"data": {
            "folder": path_label, "downloaded": dl, "skipped": sk, "failed": fl,
        }})

    logger.info("gdrive_downloader_complete", extra={"data": {
        "downloaded": grand_downloaded, "skipped": grand_skipped,
        "failed": grand_failed,
        "output_dir": str(GDRIVE_DIR.resolve()),
    }})

    print(f"\n{'=' * 70}")
    print(f"  Complete!")
    print(f"    Downloaded: {grand_downloaded:,}")
    print(f"    Skipped:    {grand_skipped:,}")
    if grand_failed > 0:
        print(f"    Failed:     {grand_failed:,}")
    print(f"    Output:     {GDRIVE_DIR.resolve()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
