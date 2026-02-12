#!/usr/bin/env python3
"""
Epstein DOJ Files Downloader with Browser Automation.
Downloads public PDF documents from justice.gov/epstein/doj-disclosures.
Handles age verification, cookie prompts, and other barriers.
"""

import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import PDF_DIR, SOURCE_URL, NUM_DATASETS

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import NoSuchElementException
    import requests
except ImportError:
    print("Error: Required packages not installed")
    print("Install with: pip install selenium requests")
    sys.exit(1)


def setup_driver():
    """Setup Chrome driver with download preferences."""
    options = webdriver.ChromeOptions()
    download_dir = str(PDF_DIR.resolve())
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
    }
    options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome(options=options)
    return driver


def handle_barriers(driver):
    """Handle age verification, cookie consent, etc."""
    time.sleep(2)

    barrier_texts = [
        "I am 18 or older", "I agree", "Accept", "Continue",
        "Confirm", "I understand", "Proceed", "Yes", "Enter",
    ]

    for button_text in barrier_texts:
        try:
            selectors = [
                f"//button[contains(text(), '{button_text}')]",
                f"//a[contains(text(), '{button_text}')]",
                f"//input[@value='{button_text}']",
                f"//div[contains(@class, 'button')][contains(text(), '{button_text}')]",
            ]
            for selector in selectors:
                try:
                    element = driver.find_element(By.XPATH, selector)
                    if element.is_displayed():
                        print(f"  Found barrier button: '{button_text}'")
                        element.click()
                        time.sleep(2)
                        return True
                except NoSuchElementException:
                    continue
        except Exception:
            continue

    try:
        cookie_button = driver.find_element(By.ID, "cookie-accept")
        cookie_button.click()
        time.sleep(1)
        return True
    except Exception:
        pass

    return False


def extract_pdf_links(driver):
    """Extract all PDF links from current page."""
    links = set()
    elements = driver.find_elements(By.TAG_NAME, "a")
    for element in elements:
        try:
            href = element.get_attribute("href")
            if href and ".pdf" in href.lower():
                links.add(href)
        except Exception:
            continue
    return links


def download_dataset(dataset_num):
    """Download PDFs for a specific dataset using Selenium."""
    print(f"\n{'=' * 60}")
    print(f"Processing Data Set {dataset_num}")
    print(f"{'=' * 60}")

    dataset_dir = PDF_DIR / f"data-set-{dataset_num}"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    driver = setup_driver()

    try:
        page_url = f"{SOURCE_URL}/data-set-{dataset_num}-files"
        print(f"Navigating to: {page_url}")

        driver.get(page_url)
        time.sleep(3)

        print("Checking for age verification or barriers...")
        if handle_barriers(driver):
            print("  Clicked through barrier")
            time.sleep(2)

        print("Extracting PDF links...")
        pdf_links = extract_pdf_links(driver)

        if not pdf_links:
            print("  No PDF links found")
            with open(dataset_dir / f"page_source_{dataset_num}.html", "w") as f:
                f.write(driver.page_source)
            return 0

        print(f"  Found {len(pdf_links)} PDF link(s)")
        downloaded = 0

        for link in sorted(pdf_links):
            filename = os.path.basename(link.split("?")[0])
            output_path = dataset_dir / filename

            if output_path.exists() and output_path.stat().st_size > 0:
                # Verify existing file is actually a PDF
                with open(output_path, "rb") as f:
                    if f.read(5).startswith(b"%PDF-"):
                        print(f"  Skipping (exists): {filename}")
                        downloaded += 1
                        continue

            print(f"  Downloading: {filename}")
            try:
                cookies = driver.get_cookies()
                session = requests.Session()
                for cookie in cookies:
                    session.cookies.set(cookie["name"], cookie["value"])

                response = session.get(link, timeout=30)
                if response.status_code == 200:
                    with open(output_path, "wb") as f:
                        f.write(response.content)

                    with open(output_path, "rb") as f:
                        if f.read(5).startswith(b"%PDF-"):
                            print(f"    Valid PDF ({output_path.stat().st_size:,} bytes)")
                            downloaded += 1
                        else:
                            print("    Not a PDF, removing")
                            output_path.unlink()
                else:
                    print(f"    HTTP {response.status_code}")
            except Exception as e:
                print(f"    Error: {e}")

        print(f"\nData Set {dataset_num}: Downloaded {downloaded} files")
        return downloaded

    finally:
        driver.quit()


def main():
    print("Epstein DOJ Files Downloader (with barrier handling)")
    print("=" * 60)

    PDF_DIR.mkdir(exist_ok=True)
    total = 0

    for i in range(1, NUM_DATASETS + 1):
        count = download_dataset(i)
        total += count
        time.sleep(2)

    print(f"\n{'=' * 60}")
    print(f"Complete! Total PDFs downloaded: {total}")
    print(f"Files saved in: {PDF_DIR.resolve()}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
