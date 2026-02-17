#!/usr/bin/env python3
"""
Epstein DOJ Files Downloader with Pagination Support.

Downloads public PDF documents from justice.gov/epstein/doj-disclosures.
Uses Playwright in headed mode with stealth patches to bypass Akamai CDN
bot detection, then downloads PDFs via requests with multithreading.

Headed mode is required — Akamai blocks headless browsers from accessing
paginated pages (returns 403 Access Denied on ?page=N).

Pages are processed in batches (default 10): scan a batch of pages for
PDF links, download them with a thread pool, free memory, then continue.
This keeps memory usage low even for datasets with thousands of pages.

Usage:
    python -m src.downloader                     # Download all datasets
    python -m src.downloader --dataset 1         # Download dataset 1 only
    python -m src.downloader --dataset 1 3 5     # Download specific datasets
    python -m src.downloader --workers 10        # Use 10 concurrent threads
    python -m src.downloader --batch-size 20     # Scan 20 pages per batch
    python -m src.downloader --dry-run           # Count files without downloading
    python -m src.downloader --headless          # Headless mode (page 0 only)
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

logger = logging.getLogger(__name__)


# ─── Browser Page Fetching ───────────────────────────────────

def handle_barriers(page):
    """Handle robot verification and age verification prompts."""
    # Robot verification — "I am not a robot" button
    try:
        robot_btn = page.get_by_role("button", name="I am not a robot")
        if robot_btn.is_visible(timeout=3000):
            print("  Clicking 'I am not a robot'...")
            robot_btn.click()
            page.wait_for_load_state("networkidle", timeout=10000)
            time.sleep(2)
    except (PwTimeout, Exception):
        pass

    # Age verification — "Yes" button for 18+ check
    try:
        age_heading = page.locator("text=Are you 18 years of age or older?")
        if age_heading.is_visible(timeout=2000):
            yes_btn = page.get_by_role("button", name="Yes")
            if yes_btn.is_visible(timeout=2000):
                print("  Clicking age verification 'Yes'...")
                yes_btn.click()
                time.sleep(1)
    except (PwTimeout, Exception):
        pass


def get_last_page_from_browser(page):
    """Extract the last page number from pagination in the browser DOM."""
    # Look for "Last" link first
    last_link = page.locator("a:has-text('Last')")
    try:
        if last_link.count() > 0:
            href = last_link.first.get_attribute("href")
            if href:
                match = re.search(r"[?&]page=(\d+)", href)
                if match:
                    return int(match.group(1))
    except Exception:
        pass

    # Fallback: find highest page number in pagination links
    max_page = 0
    pagination_links = page.locator("nav[aria-label='Pagination'] a[href*='page=']")
    try:
        count = pagination_links.count()
        for i in range(count):
            href = pagination_links.nth(i).get_attribute("href")
            if href:
                match = re.search(r"[?&]page=(\d+)", href)
                if match:
                    max_page = max(max_page, int(match.group(1)))
    except Exception:
        pass

    return max_page


def extract_pdf_links_from_browser(page, base_url):
    """Extract all PDF download URLs from the current browser page."""
    links = set()
    all_anchors = page.locator("a[href*='.pdf']")
    try:
        count = all_anchors.count()
        for i in range(count):
            href = all_anchors.nth(i).get_attribute("href")
            if href:
                full_url = urljoin(base_url, href)
                links.add(full_url)
    except Exception:
        pass
    return links


def fetch_page_links(page, base_url, page_num, max_retries=3):
    """Navigate to a paginated page and extract PDF links.

    Retries on transient network errors (connection closed, timeout).
    Returns a set of PDF URLs found on the page.
    """
    page_url = f"{base_url}?page={page_num}" if page_num > 0 else base_url

    for attempt in range(1, max_retries + 1):
        try:
            page.goto(page_url, wait_until="networkidle", timeout=30000)
            time.sleep(1)
            break
        except PwTimeout:
            try:
                page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
                time.sleep(2)
                break
            except Exception as e:
                if attempt < max_retries:
                    wait = attempt * 5
                    logger.warning("page_fetch_retry", extra={"data": {
                        "page_num": page_num, "attempt": attempt,
                        "max_retries": max_retries, "error_type": type(e).__name__,
                    }})
                    print(f"    Page {page_num}: retry {attempt}/{max_retries} "
                          f"in {wait}s ({type(e).__name__})")
                    time.sleep(wait)
                else:
                    logger.error("page_fetch_failed", extra={"data": {
                        "page_num": page_num, "max_retries": max_retries, "url": page_url,
                    }})
                    print(f"    Page {page_num}: FAILED after {max_retries} attempts")
                    return set()
        except Exception as e:
            if attempt < max_retries:
                wait = attempt * 5
                logger.warning("page_fetch_retry", extra={"data": {
                    "page_num": page_num, "attempt": attempt,
                    "max_retries": max_retries, "error_type": type(e).__name__,
                }})
                print(f"    Page {page_num}: retry {attempt}/{max_retries} "
                      f"in {wait}s ({type(e).__name__})")
                time.sleep(wait)
            else:
                logger.error("page_fetch_failed", extra={"data": {
                    "page_num": page_num, "max_retries": max_retries, "url": page_url,
                }})
                print(f"    Page {page_num}: FAILED after {max_retries} attempts")
                return set()

    # Extract links before any barrier clicks — PDF links are already
    # in the DOM, and clicking age verification triggers a Drupal AJAX
    # reload that clears the content.
    return extract_pdf_links_from_browser(page, page_url)


# ─── PDF Download ────────────────────────────────────────────

def is_valid_pdf(filepath):
    """Check if a file exists and starts with the PDF magic bytes."""
    try:
        if filepath.exists() and filepath.stat().st_size > 0:
            with open(filepath, "rb") as f:
                return f.read(5).startswith(b"%PDF-")
    except Exception:
        pass
    return False


def download_pdf(url, output_path, session):
    """Download a single PDF file. Returns (url, success, message)."""
    filename = output_path.name

    if is_valid_pdf(output_path):
        return url, True, "skip"

    try:
        response = session.get(url, timeout=60)
        if response.status_code != 200:
            logger.error("download_http_error", extra={"data": {
                "url": url, "filename": filename, "status_code": response.status_code,
            }})
            return url, False, f"  HTTP {response.status_code}: {filename}"

        if not response.content[:5].startswith(b"%PDF-"):
            logger.warning("download_not_pdf", extra={"data": {
                "url": url, "filename": filename,
            }})
            return url, False, f"  Not a PDF: {filename}"

        with open(output_path, "wb") as f:
            f.write(response.content)

        size = output_path.stat().st_size
        logger.info("download_success", extra={"data": {
            "url": url, "filename": filename, "size_bytes": size,
        }})
        return url, True, f"  Downloaded: {filename} ({size:,} bytes)"

    except requests.exceptions.Timeout:
        logger.error("download_timeout", extra={"data": {
            "url": url, "filename": filename,
        }})
        return url, False, f"  Timeout: {filename}"
    except Exception as e:
        logger.error("download_error", extra={"data": {
            "url": url, "filename": filename,
        }}, exc_info=True)
        return url, False, f"  Error: {filename} — {e}"


def download_batch(pdf_links, dataset_dir, session, workers):
    """Download a batch of PDFs using a thread pool.

    Returns (downloaded, skipped, failed) counts.
    """
    downloaded = 0
    skipped = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for url in pdf_links:
            filename = os.path.basename(url.split("?")[0])
            output_path = dataset_dir / filename
            future = pool.submit(download_pdf, url, output_path, session)
            futures[future] = url

        for future in as_completed(futures):
            url, success, message = future.result()
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


# ─── Dataset Download ────────────────────────────────────────

def download_dataset(dataset_num, workers, batch_size, dry_run, browser_context):
    """Download all PDFs for one dataset, processing pages in batches."""
    print(f"\n{'=' * 70}")
    print(f"  Data Set {dataset_num}")
    print(f"{'=' * 70}")

    dataset_dir = PDF_DIR / f"data-set-{dataset_num}"
    if not dry_run:
        dataset_dir.mkdir(parents=True, exist_ok=True)

    base_url = f"{SOURCE_URL}/data-set-{dataset_num}-files"
    page = browser_context.new_page()
    Stealth().apply_stealth_sync(page)

    try:
        # Navigate to first page and handle barriers
        print(f"  Navigating to: {base_url}")
        page.goto(base_url, wait_until="networkidle", timeout=30000)
        time.sleep(2)
        handle_barriers(page)

        # Discover pagination
        last_page = get_last_page_from_browser(page)
        total_pages = last_page + 1
        logger.info("pagination_discovered", extra={"data": {
            "dataset": dataset_num, "total_pages": total_pages,
            "last_page": last_page, "base_url": base_url,
        }})
        print(f"  Pages: {total_pages} (page 0 to {last_page})")

        # Set up requests session for downloads (reused across batches)
        session = None
        if not dry_run:
            session = requests.Session()
            cookies = browser_context.cookies()
            for cookie in cookies:
                session.cookies.set(cookie["name"], cookie["value"],
                                    domain=cookie.get("domain", ""))
            session.headers.update({
                "User-Agent": page.evaluate("() => navigator.userAgent"),
            })

        # Process pages in batches
        total_downloaded = 0
        total_skipped = 0
        total_failed = 0
        total_links = 0

        for batch_start in range(0, total_pages, batch_size):
            batch_end = min(batch_start + batch_size, total_pages)
            batch_label = f"pages {batch_start}-{batch_end - 1}"
            print(f"\n  ── Batch: {batch_label} ──")

            # Scan this batch of pages for PDF links
            batch_links = set()
            for page_num in range(batch_start, batch_end):
                if page_num > 0:
                    time.sleep(PAGE_FETCH_DELAY)

                links = fetch_page_links(page, base_url, page_num)
                batch_links.update(links)

                if page_num == batch_end - 1:
                    print(f"    Scanned {batch_label}: "
                          f"{len(batch_links)} links found")

            batch_links = sorted(batch_links)
            total_links += len(batch_links)

            if dry_run:
                existing = sum(
                    1 for url in batch_links
                    if is_valid_pdf(dataset_dir / os.path.basename(url.split("?")[0]))
                )
                print(f"    Batch links: {len(batch_links)} "
                      f"(already downloaded: {existing})")
            else:
                # Download this batch with thread pool
                dl, sk, fl = download_batch(
                    batch_links, dataset_dir, session, workers,
                )
                total_downloaded += dl
                total_skipped += sk
                total_failed += fl
                logger.info("batch_complete", extra={"data": {
                    "dataset": dataset_num, "batch_label": batch_label,
                    "downloaded": dl, "skipped": sk, "failed": fl,
                }})

            # Clear batch from memory
            del batch_links

        # Summary
        logger.info("dataset_download_complete", extra={"data": {
            "dataset": dataset_num, "total_links": total_links,
            "downloaded": total_downloaded, "skipped": total_skipped,
            "failed": total_failed, "dry_run": dry_run,
        }})
        print(f"\n  Data Set {dataset_num} complete:")
        print(f"    Total PDF links: {total_links}")
        if dry_run:
            print(f"    (dry run — no downloads)")
        else:
            print(f"    Downloaded:      {total_downloaded}")
            print(f"    Skipped:         {total_skipped}")
            print(f"    Failed:          {total_failed}")

        return total_downloaded, total_skipped, total_failed

    finally:
        page.close()


# ─── Main ────────────────────────────────────────────────────

def main():
    from src.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Epstein DOJ Files Downloader",
        epilog="Examples:\n"
               "  python -m src.downloader                   # All datasets\n"
               "  python -m src.downloader --dataset 1       # Dataset 1 only\n"
               "  python -m src.downloader --dataset 1 3 5   # Specific datasets\n"
               "  python -m src.downloader --workers 10      # 10 threads\n"
               "  python -m src.downloader --batch-size 20   # 20 pages per batch\n"
               "  python -m src.downloader --dry-run         # Count only\n"
               "  python -m src.downloader --headless        # Headless (page 0 only)\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dataset", nargs="+", type=int,
        help="Dataset number(s) to download (default: all 1-12)",
    )
    parser.add_argument(
        "--workers", type=int, default=DOWNLOAD_WORKERS,
        help=f"Concurrent download threads (default: {DOWNLOAD_WORKERS})",
    )
    parser.add_argument(
        "--batch-size", type=int, default=DOWNLOAD_BATCH_SIZE,
        help=f"Pages to scan per download batch (default: {DOWNLOAD_BATCH_SIZE})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count files and pages without downloading",
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run browser in headless mode (Akamai blocks pagination in headless)",
    )
    args = parser.parse_args()

    datasets = args.dataset or list(range(1, NUM_DATASETS + 1))

    for d in datasets:
        if d < 1 or d > NUM_DATASETS:
            print(f"Error: Dataset {d} is out of range (1-{NUM_DATASETS})")
            sys.exit(1)

    logger.info("downloader_started", extra={"data": {
        "datasets": datasets, "workers": args.workers,
        "batch_size": args.batch_size, "dry_run": args.dry_run,
        "headless": args.headless,
        "browser": "headless" if args.headless else "headed",
        "output_dir": str(PDF_DIR.resolve()),
    }})

    print("=" * 70)
    print("Epstein DOJ Files Downloader")
    print("=" * 70)
    print(f"  Datasets:   {', '.join(str(d) for d in datasets)}")
    print(f"  Workers:    {args.workers}")
    print(f"  Batch size: {args.batch_size} pages")
    print(f"  Output:     {PDF_DIR.resolve()}")
    print(f"  Browser:    {'headless' if args.headless else 'headed'}")
    if args.dry_run:
        print("  Mode:       DRY RUN (no downloads)")
    print()

    if not args.dry_run:
        PDF_DIR.mkdir(exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=args.headless)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
        )

        grand_downloaded = 0
        grand_skipped = 0
        grand_failed = 0
        dataset_results = {}
        try:
            for dataset_num in datasets:
                dl, sk, fl = download_dataset(
                    dataset_num, args.workers, args.batch_size,
                    args.dry_run, context,
                )
                grand_downloaded += dl
                grand_skipped += sk
                grand_failed += fl
                dataset_results[dataset_num] = (dl, sk, fl)
                time.sleep(1)
        finally:
            context.close()
            browser.close()

    logger.info("downloader_complete", extra={"data": {
        "downloaded": grand_downloaded, "skipped": grand_skipped,
        "failed": grand_failed, "dry_run": args.dry_run,
        "dataset_failures": dataset_failures,
    }})

    print(f"\n{'=' * 70}")
    if args.dry_run:
        print(f"  Dry run complete.")
    else:
        print(f"  Complete!")
        print(f"    Downloaded: {grand_downloaded}")
        print(f"    Skipped:    {grand_skipped}")
        print(f"    Failed:     {grand_failed}")
        if grand_failed > 0:
            print(f"\n  Failures by dataset:")
            for ds, (dl, sk, fl) in sorted(dataset_results.items()):
                if fl > 0:
                    print(f"    Data Set {ds}: {fl} failed")
        print(f"\n  Files saved in: {PDF_DIR.resolve()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
