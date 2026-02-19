#!/usr/bin/env python3
"""
Epstein DOJ Files — MP4 Companion Checker.

Scans dataset pages on justice.gov to discover PDF file URLs, then checks
whether a corresponding .mp4 version exists on the server. Downloads any
.mp4 files found into the same dataset directory alongside the PDFs.

Defaults to datasets 8, 9, and 10 (known to have video companions).

Usage:
    python -m src.mp4_checker                    # Check datasets 8, 9, 10
    python -m src.mp4_checker --dataset 8        # Dataset 8 only
    python -m src.mp4_checker --dataset 8 9 10   # Explicit datasets
    python -m src.mp4_checker --workers 5        # Concurrent threads
    python -m src.mp4_checker --dry-run          # Check without downloading
"""

import argparse
import logging
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urljoin

import requests
from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
from playwright_stealth import Stealth

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    PDF_DIR, SOURCE_URL, NUM_DATASETS,
    DOWNLOAD_WORKERS, DOWNLOAD_BATCH_SIZE, PAGE_FETCH_DELAY,
)
from src.downloader import (
    handle_barriers, get_last_page_from_browser,
    extract_pdf_links_from_browser, fetch_page_links,
)

logger = logging.getLogger(__name__)

DEFAULT_DATASETS = [8, 9, 10]


# ─── MP4 Check & Download ────────────────────────────────────

def check_and_download_mp4(pdf_url, dataset_dir, session):
    """Check if an .mp4 version of a PDF URL exists; download if so.

    Returns (mp4_url, status, message) where status is one of:
      "downloaded", "skip", "not_found", "error"
    """
    # Construct .mp4 URL from .pdf URL
    mp4_url = re.sub(r'\.pdf(\?.*)?$', '.mp4', pdf_url, flags=re.IGNORECASE)
    if mp4_url == pdf_url:
        # URL didn't end in .pdf, skip
        return pdf_url, "not_found", None

    mp4_filename = os.path.basename(mp4_url.split("?")[0])
    output_path = dataset_dir / mp4_filename

    # Skip if already downloaded
    if output_path.exists() and output_path.stat().st_size > 0:
        return mp4_url, "skip", None

    try:
        # HEAD request to check existence without downloading the whole file
        head = session.head(mp4_url, timeout=30, allow_redirects=True)

        if head.status_code == 404:
            return mp4_url, "not_found", None

        if head.status_code != 200:
            logger.warning("mp4_check_http_error", extra={"data": {
                "url": mp4_url, "status_code": head.status_code,
            }})
            return mp4_url, "not_found", None

        # MP4 exists — download it
        resp = session.get(mp4_url, timeout=300, stream=True)
        if resp.status_code != 200:
            logger.error("mp4_download_http_error", extra={"data": {
                "url": mp4_url, "status_code": resp.status_code,
            }})
            return mp4_url, "error", f"  HTTP {resp.status_code}: {mp4_filename}"

        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

        size = output_path.stat().st_size
        if size == 0:
            output_path.unlink()
            return mp4_url, "error", f"  Empty file: {mp4_filename}"

        logger.info("mp4_download_success", extra={"data": {
            "url": mp4_url, "filename": mp4_filename, "size_bytes": size,
        }})
        return mp4_url, "downloaded", f"  Downloaded: {mp4_filename} ({size:,} bytes)"

    except requests.exceptions.Timeout:
        logger.error("mp4_download_timeout", extra={"data": {
            "url": mp4_url, "filename": mp4_filename,
        }})
        return mp4_url, "error", f"  Timeout: {mp4_filename}"
    except Exception as e:
        logger.error("mp4_download_error", extra={"data": {
            "url": mp4_url, "filename": mp4_filename,
        }}, exc_info=True)
        return mp4_url, "error", f"  Error: {mp4_filename} — {e}"


def check_batch(pdf_urls, dataset_dir, session, workers, dry_run):
    """Check a batch of PDF URLs for .mp4 companions.

    Returns (found, downloaded, skipped, failed) counts.
    """
    found = 0
    downloaded = 0
    skipped = 0
    failed = 0

    if dry_run:
        # Just do HEAD checks, no downloads
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for url in pdf_urls:
                mp4_url = re.sub(r'\.pdf(\?.*)?$', '.mp4', url, flags=re.IGNORECASE)
                if mp4_url == url:
                    continue
                mp4_filename = os.path.basename(mp4_url.split("?")[0])
                output_path = dataset_dir / mp4_filename
                if output_path.exists() and output_path.stat().st_size > 0:
                    skipped += 1
                    found += 1
                    continue
                future = pool.submit(_head_check, mp4_url, session)
                futures[future] = mp4_url

            for future in as_completed(futures):
                exists = future.result()
                if exists:
                    found += 1
                    mp4_url = futures[future]
                    print(f"  Found: {os.path.basename(mp4_url.split('?')[0])}")
        return found, 0, skipped, 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for url in pdf_urls:
            future = pool.submit(check_and_download_mp4, url, dataset_dir, session)
            futures[future] = url

        for future in as_completed(futures):
            mp4_url, status, message = future.result()
            if status == "downloaded":
                found += 1
                downloaded += 1
                print(message)
            elif status == "skip":
                found += 1
                skipped += 1
            elif status == "error":
                found += 1
                failed += 1
                print(message)
            # "not_found" — no .mp4 exists, silently skip

    return found, downloaded, skipped, failed


def _head_check(mp4_url, session):
    """HEAD request to check if an MP4 exists. Returns bool."""
    try:
        resp = session.head(mp4_url, timeout=30, allow_redirects=True)
        return resp.status_code == 200
    except Exception:
        return False


# ─── Per-Dataset Processing ───────────────────────────────────

def check_dataset(dataset_num, workers, batch_size, dry_run, browser_context):
    """Scan a dataset's pages for PDF URLs and check for .mp4 companions."""
    print(f"\n{'=' * 70}")
    print(f"  Data Set {dataset_num}")
    print(f"{'=' * 70}")

    dataset_dir = PDF_DIR / f"data-set-{dataset_num}"
    if not dataset_dir.exists():
        print(f"  Dataset directory not found: {dataset_dir}")
        return 0, 0, 0, 0

    base_url = f"{SOURCE_URL}/data-set-{dataset_num}-files"
    page = browser_context.new_page()
    Stealth().apply_stealth_sync(page)

    try:
        print(f"  Navigating to: {base_url}")
        page.goto(base_url, wait_until="networkidle", timeout=30000)
        time.sleep(2)
        handle_barriers(page)

        last_page = get_last_page_from_browser(page)
        total_pages = last_page + 1
        logger.info("mp4_check_pagination", extra={"data": {
            "dataset": dataset_num, "total_pages": total_pages,
        }})
        print(f"  Pages: {total_pages} (page 0 to {last_page})")

        # Set up requests session
        session = requests.Session()
        cookies = browser_context.cookies()
        for cookie in cookies:
            session.cookies.set(cookie["name"], cookie["value"],
                                domain=cookie.get("domain", ""))
        session.headers.update({
            "User-Agent": page.evaluate("() => navigator.userAgent"),
        })

        total_found = 0
        total_downloaded = 0
        total_skipped = 0
        total_failed = 0

        for batch_start in range(0, total_pages, batch_size):
            batch_end = min(batch_start + batch_size, total_pages)
            batch_label = f"pages {batch_start}-{batch_end - 1}"

            # Scan pages for PDF links
            batch_links = set()
            for page_num in range(batch_start, batch_end):
                if page_num > 0:
                    time.sleep(PAGE_FETCH_DELAY)
                links = fetch_page_links(page, base_url, page_num)
                batch_links.update(links)

                if page_num == batch_end - 1:
                    print(f"    Scanned {batch_label}: "
                          f"{len(batch_links)} PDF links")

            batch_links = sorted(batch_links)

            # Check each PDF URL for a .mp4 companion
            print(f"    Checking {len(batch_links)} URLs for .mp4 companions...")
            fo, dl, sk, fl = check_batch(
                batch_links, dataset_dir, session, workers, dry_run,
            )
            total_found += fo
            total_downloaded += dl
            total_skipped += sk
            total_failed += fl

            logger.info("mp4_check_batch_complete", extra={"data": {
                "dataset": dataset_num, "batch_label": batch_label,
                "pdf_urls": len(batch_links), "mp4_found": fo,
                "downloaded": dl, "skipped": sk, "failed": fl,
            }})

            del batch_links

        logger.info("mp4_check_dataset_complete", extra={"data": {
            "dataset": dataset_num, "found": total_found,
            "downloaded": total_downloaded, "skipped": total_skipped,
            "failed": total_failed,
        }})

        return total_found, total_downloaded, total_skipped, total_failed

    finally:
        page.close()


# ─── Main ────────────────────────────────────────────────────

def main():
    from src.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Epstein DOJ Files — MP4 Companion Checker",
        epilog="Examples:\n"
               "  python -m src.mp4_checker                   # Datasets 8, 9, 10\n"
               "  python -m src.mp4_checker --dataset 8       # Dataset 8 only\n"
               "  python -m src.mp4_checker --dataset 8 9 10  # Explicit datasets\n"
               "  python -m src.mp4_checker --workers 5       # 5 concurrent threads\n"
               "  python -m src.mp4_checker --dry-run         # Check without downloading\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dataset", nargs="+", type=int, default=DEFAULT_DATASETS,
        help=f"Dataset number(s) to check (default: {DEFAULT_DATASETS})",
    )
    parser.add_argument(
        "--workers", type=int, default=DOWNLOAD_WORKERS,
        help=f"Concurrent check/download threads (default: {DOWNLOAD_WORKERS})",
    )
    parser.add_argument(
        "--batch-size", type=int, default=DOWNLOAD_BATCH_SIZE,
        help=f"Pages to scan per batch (default: {DOWNLOAD_BATCH_SIZE})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Check for .mp4 files without downloading",
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run browser in headless mode (may be blocked by Akamai)",
    )
    args = parser.parse_args()

    for d in args.dataset:
        if d < 1 or d > NUM_DATASETS:
            print(f"Error: Dataset {d} is out of range (1-{NUM_DATASETS})")
            sys.exit(1)

    print("=" * 70)
    print("Epstein DOJ Files — MP4 Companion Checker")
    print("=" * 70)
    print(f"  Datasets:   {', '.join(str(d) for d in args.dataset)}")
    print(f"  Workers:    {args.workers}")
    print(f"  Batch size: {args.batch_size} pages")
    print(f"  Browser:    {'headless' if args.headless else 'headed'}")
    if args.dry_run:
        print("  Mode:       DRY RUN (check only)")
    print()

    logger.info("mp4_checker_started", extra={"data": {
        "datasets": args.dataset, "workers": args.workers,
        "batch_size": args.batch_size, "dry_run": args.dry_run,
        "headless": args.headless,
    }})

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=args.headless)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
        )

        grand_found = 0
        grand_downloaded = 0
        grand_skipped = 0
        grand_failed = 0
        dataset_results = {}

        try:
            for dataset_num in args.dataset:
                fo, dl, sk, fl = check_dataset(
                    dataset_num, args.workers, args.batch_size,
                    args.dry_run, context,
                )
                grand_found += fo
                grand_downloaded += dl
                grand_skipped += sk
                grand_failed += fl
                dataset_results[dataset_num] = (fo, dl, sk, fl)
                time.sleep(1)
        finally:
            context.close()
            browser.close()

    logger.info("mp4_checker_complete", extra={"data": {
        "found": grand_found, "downloaded": grand_downloaded,
        "skipped": grand_skipped, "failed": grand_failed,
        "dry_run": args.dry_run,
        "dataset_results": {str(k): {"found": v[0], "downloaded": v[1],
                                      "skipped": v[2], "failed": v[3]}
                            for k, v in dataset_results.items()},
    }})

    print(f"\n{'=' * 70}")
    if args.dry_run:
        print(f"  Dry run complete!")
        print(f"    MP4 files found:     {grand_found}")
        print(f"    Already downloaded:  {grand_skipped}")
    else:
        print(f"  Complete!")
        print(f"    MP4 found:      {grand_found}")
        print(f"    Downloaded:     {grand_downloaded}")
        print(f"    Already had:    {grand_skipped}")
        print(f"    Failed:         {grand_failed}")
        if grand_failed > 0:
            print(f"\n  Failures by dataset:")
            for ds, (fo, dl, sk, fl) in sorted(dataset_results.items()):
                if fl > 0:
                    print(f"    Data Set {ds}: {fl} failed")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
