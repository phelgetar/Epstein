#!/usr/bin/env python3
"""
Epstein DOJ Files Downloader with Pagination Support.

Downloads public PDF documents from justice.gov/epstein/doj-disclosures.
Uses Playwright in headed mode with stealth patches to bypass Akamai CDN
bot detection, then downloads PDFs via requests.

Headed mode is required — Akamai blocks headless browsers from accessing
paginated pages (returns 403 Access Denied on ?page=N).

Usage:
    python -m src.downloader                     # Download all datasets
    python -m src.downloader --dataset 1         # Download dataset 1 only
    python -m src.downloader --dataset 1 3 5     # Download specific datasets
    python -m src.downloader --workers 10        # Use 10 concurrent threads
    python -m src.downloader --dry-run           # Count files without downloading
    python -m src.downloader --headless          # Headless mode (page 0 only)
"""

import argparse
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
    DOWNLOAD_WORKERS, PAGE_FETCH_DELAY,
)


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
    return sorted(links)


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
        return url, True, f"  Skipped (exists): {filename}"

    try:
        response = session.get(url, timeout=60)
        if response.status_code != 200:
            return url, False, f"  HTTP {response.status_code}: {filename}"

        if not response.content[:5].startswith(b"%PDF-"):
            return url, False, f"  Not a PDF: {filename}"

        with open(output_path, "wb") as f:
            f.write(response.content)

        size = output_path.stat().st_size
        return url, True, f"  Downloaded: {filename} ({size:,} bytes)"

    except requests.exceptions.Timeout:
        return url, False, f"  Timeout: {filename}"
    except Exception as e:
        return url, False, f"  Error: {filename} — {e}"


# ─── Dataset Download ────────────────────────────────────────

def download_dataset(dataset_num, workers, dry_run, browser_context):
    """Download all PDFs for one dataset across all paginated pages."""
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
        # Navigate to dataset page
        print(f"  Navigating to: {base_url}")
        page.goto(base_url, wait_until="networkidle", timeout=30000)
        time.sleep(2)

        # Handle verification barriers
        handle_barriers(page)

        # Discover pagination
        last_page = get_last_page_from_browser(page)
        total_pages = last_page + 1
        print(f"  Pages: {total_pages} (page 0 to {last_page})")

        # Collect PDF links from all pages
        all_pdf_links = []

        # Page 0 (current page)
        links = extract_pdf_links_from_browser(page, base_url)
        all_pdf_links.extend(links)
        print(f"  Page 0: {len(links)} PDF links")

        # Remaining pages
        for page_num in range(1, total_pages):
            time.sleep(PAGE_FETCH_DELAY)
            page_url = f"{base_url}?page={page_num}"

            try:
                page.goto(page_url, wait_until="networkidle", timeout=30000)
                time.sleep(1)
            except PwTimeout:
                print(f"  Page {page_num}: timeout, retrying...")
                try:
                    page.goto(page_url, wait_until="domcontentloaded", timeout=30000)
                    time.sleep(2)
                except Exception:
                    print(f"  Page {page_num}: FAILED")
                    continue

            # Extract links BEFORE handling barriers — the PDF links are
            # already in the DOM, and clicking the age verification "Yes"
            # triggers a Drupal AJAX reload that clears the content.
            links = extract_pdf_links_from_browser(page, page_url)
            all_pdf_links.extend(links)

            if page_num % 10 == 0 or page_num == last_page:
                print(f"  Page {page_num}/{last_page}: {len(links)} links "
                      f"(total so far: {len(all_pdf_links)})")

        # Deduplicate
        all_pdf_links = sorted(set(all_pdf_links))
        print(f"\n  Total unique PDF links: {len(all_pdf_links)}")

        if dry_run:
            existing = sum(
                1 for url in all_pdf_links
                if is_valid_pdf(dataset_dir / os.path.basename(url.split("?")[0]))
            )
            print(f"  Already downloaded: {existing}")
            print(f"  Remaining: {len(all_pdf_links) - existing}")
            return len(all_pdf_links)

        # Transfer browser cookies to requests session for downloads
        session = requests.Session()
        cookies = browser_context.cookies()
        for cookie in cookies:
            session.cookies.set(cookie["name"], cookie["value"],
                                domain=cookie.get("domain", ""))
        session.headers.update({
            "User-Agent": page.evaluate("() => navigator.userAgent"),
        })

        # Download PDFs with thread pool
        downloaded = 0
        skipped = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {}
            for url in all_pdf_links:
                filename = os.path.basename(url.split("?")[0])
                output_path = dataset_dir / filename
                future = pool.submit(download_pdf, url, output_path, session)
                futures[future] = url

            for future in as_completed(futures):
                url, success, message = future.result()
                if success:
                    if "Skipped" in message:
                        skipped += 1
                    else:
                        downloaded += 1
                        print(message)
                else:
                    failed += 1
                    print(message)

        print(f"\n  Data Set {dataset_num} complete:")
        print(f"    Downloaded: {downloaded}")
        print(f"    Skipped:    {skipped}")
        print(f"    Failed:     {failed}")
        print(f"    Total:      {downloaded + skipped + failed}")

        return downloaded + skipped

    finally:
        page.close()


# ─── Main ────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Epstein DOJ Files Downloader",
        epilog="Examples:\n"
               "  python -m src.downloader                   # All datasets\n"
               "  python -m src.downloader --dataset 1       # Dataset 1 only\n"
               "  python -m src.downloader --dataset 1 3 5   # Specific datasets\n"
               "  python -m src.downloader --workers 10      # 10 threads\n"
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

    print("=" * 70)
    print("Epstein DOJ Files Downloader")
    print("=" * 70)
    print(f"  Datasets:  {', '.join(str(d) for d in datasets)}")
    print(f"  Workers:   {args.workers}")
    print(f"  Output:    {PDF_DIR.resolve()}")
    print(f"  Browser:   {'headless' if args.headless else 'headed'}")
    if args.dry_run:
        print("  Mode:      DRY RUN (no downloads)")
    print()

    if not args.dry_run:
        PDF_DIR.mkdir(exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=args.headless)
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
        )

        grand_total = 0
        try:
            for dataset_num in datasets:
                count = download_dataset(
                    dataset_num, args.workers, args.dry_run, context,
                )
                grand_total += count
                time.sleep(1)
        finally:
            context.close()
            browser.close()

    print(f"\n{'=' * 70}")
    if args.dry_run:
        print(f"  Dry run complete. Total PDF links found: {grand_total}")
    else:
        print(f"  Complete! Total PDFs processed: {grand_total}")
        print(f"  Files saved in: {PDF_DIR.resolve()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
