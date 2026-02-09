#!/usr/bin/env python3
"""
Epstein DOJ Files - PDF to JSON Converter
Extracts text and metadata from all downloaded PDFs into a searchable JSON file.
Uses pdftotext/pdfinfo (Poppler) for text extraction.
"""

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    PROJECT_ROOT, DATA_DIR, PDF_DIR,
    SOURCE_URL, NUM_DATASETS,
    JSON_FULL, JSON_SEARCH_INDEX, JSON_SUMMARY, JSON_FILE_LIST,
)


def extract_pdf_text(pdf_path):
    """Extract text from PDF using pdftotext."""
    try:
        result = subprocess.run(
            ["pdftotext", "-layout", str(pdf_path), "-"],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            return result.stdout
        return ""
    except Exception as e:
        print(f"    Error extracting with pdftotext: {e}")
        return ""


def extract_pdf_info(pdf_path):
    """Get PDF metadata using pdfinfo."""
    info = {}
    try:
        result = subprocess.run(
            ["pdfinfo", str(pdf_path)],
            capture_output=True,
            text=True,
            timeout=10,
        )
        for line in result.stdout.split("\n"):
            if ":" in line:
                key, value = line.split(":", 1)
                info[key.strip()] = value.strip()
        return info
    except Exception as e:
        print(f"    Error getting PDF info: {e}")
        return {}


def process_single_pdf(pdf_path):
    """Process a single PDF file."""
    print(f"  Processing: {pdf_path.name}")

    stats = pdf_path.stat()
    pdf_info = extract_pdf_info(pdf_path)
    full_text = extract_pdf_text(pdf_path)

    pages = pdf_info.get("Pages", "0")
    try:
        pages = int(pages)
    except ValueError:
        pages = 0

    return {
        "filename": pdf_path.name,
        "filepath": str(pdf_path),
        "size_bytes": stats.st_size,
        "size_mb": round(stats.st_size / (1024 * 1024), 2),
        "modified_date": datetime.fromtimestamp(stats.st_mtime).isoformat(),
        "pages": pages,
        "title": pdf_info.get("Title", ""),
        "author": pdf_info.get("Author", ""),
        "subject": pdf_info.get("Subject", ""),
        "creator": pdf_info.get("Creator", ""),
        "producer": pdf_info.get("Producer", ""),
        "creation_date": pdf_info.get("CreationDate", ""),
        "full_text": full_text,
        "text_length": len(full_text),
        "word_count": len(full_text.split()),
    }


def process_all_pdfs():
    """Process all PDFs and create comprehensive JSON."""
    print("Starting PDF extraction...")
    print("=" * 60)

    data = {
        "metadata": {
            "extraction_date": datetime.now(timezone.utc).isoformat(),
            "total_datasets": NUM_DATASETS,
            "source": SOURCE_URL,
            "extraction_tool": "pdftotext/pdfinfo",
            "description": "Epstein DOJ disclosure documents with full text extraction",
        },
        "datasets": [],
    }

    if not PDF_DIR.exists():
        print(f"Error: Directory '{PDF_DIR}' does not exist")
        return None

    total_files = 0
    total_size = 0
    total_pages = 0

    for i in range(1, NUM_DATASETS + 1):
        dataset_dir = PDF_DIR / f"data-set-{i}"

        dataset = {
            "dataset_number": i,
            "dataset_name": f"data-set-{i}",
            "dataset_url": f"{SOURCE_URL}/data-set-{i}-files",
            "files": [],
        }

        if not dataset_dir.exists():
            print(f"Warning: {dataset_dir} does not exist, skipping...")
            data["datasets"].append(dataset)
            continue

        pdf_files = sorted(dataset_dir.glob("*.pdf"))
        if not pdf_files:
            print(f"Data Set {i}: No PDF files found")
            data["datasets"].append(dataset)
            continue

        print(f"\nData Set {i}: Processing {len(pdf_files)} PDFs")
        print("-" * 60)

        for pdf_path in pdf_files:
            try:
                file_data = process_single_pdf(pdf_path)
                dataset["files"].append(file_data)
                total_files += 1
                total_size += file_data["size_mb"]
                total_pages += file_data["pages"]
            except Exception as e:
                print(f"  Error processing {pdf_path.name}: {e}")
                dataset["files"].append({
                    "filename": pdf_path.name,
                    "filepath": str(pdf_path),
                    "error": str(e),
                })

        dataset["file_count"] = len(dataset["files"])
        dataset["total_pages"] = sum(
            f.get("pages", 0) for f in dataset["files"] if "pages" in f
        )
        dataset["total_size_mb"] = round(
            sum(f.get("size_mb", 0) for f in dataset["files"] if "size_mb" in f), 2
        )
        print(
            f"  Completed: {dataset['file_count']} files, "
            f"{dataset['total_pages']} pages, {dataset['total_size_mb']} MB"
        )
        data["datasets"].append(dataset)

    data["metadata"]["total_files"] = total_files
    data["metadata"]["total_pages"] = total_pages
    data["metadata"]["total_size_mb"] = round(total_size, 2)

    return data


def create_search_index(data):
    """Create a simplified flat search index."""
    search_index = []
    for dataset in data["datasets"]:
        for file_info in dataset["files"]:
            if "full_text" in file_info and "error" not in file_info:
                search_index.append({
                    "dataset": dataset["dataset_number"],
                    "filename": file_info["filename"],
                    "filepath": file_info["filepath"],
                    "pages": file_info.get("pages", 0),
                    "text": file_info["full_text"],
                })
    return search_index


def save_json_files(data):
    """Save multiple JSON output formats to data/."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 60)
    print("Saving JSON files...")
    print("=" * 60)

    files_created = []

    # 1. Full JSON
    full_path = DATA_DIR / JSON_FULL
    print(f"Creating: {full_path}")
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    files_created.append(str(full_path))
    print(f"  Size: {full_path.stat().st_size / (1024 * 1024):.1f} MB")

    # 2. Summary JSON (no full text)
    summary_data = json.loads(json.dumps(data))
    for dataset in summary_data["datasets"]:
        for file_info in dataset["files"]:
            if "full_text" in file_info:
                full_text = file_info["full_text"]
                file_info["text_preview"] = (
                    full_text[:500] + "..." if len(full_text) > 500 else full_text
                )
                del file_info["full_text"]

    summary_path = DATA_DIR / JSON_SUMMARY
    print(f"Creating: {summary_path}")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    files_created.append(str(summary_path))
    print(f"  Size: {summary_path.stat().st_size / (1024 * 1024):.1f} MB")

    # 3. Search index (flat)
    search_index = create_search_index(data)
    search_path = DATA_DIR / JSON_SEARCH_INDEX
    print(f"Creating: {search_path}")
    with open(search_path, "w", encoding="utf-8") as f:
        json.dump(search_index, f, indent=2, ensure_ascii=False)
    files_created.append(str(search_path))
    print(f"  Size: {search_path.stat().st_size / (1024 * 1024):.1f} MB")

    # 4. File listing
    file_list = []
    for dataset in data["datasets"]:
        for file_info in dataset["files"]:
            if "error" not in file_info:
                file_list.append({
                    "dataset": dataset["dataset_number"],
                    "filename": file_info["filename"],
                    "pages": file_info.get("pages", 0),
                    "size_mb": file_info.get("size_mb", 0),
                    "word_count": file_info.get("word_count", 0),
                })

    list_path = DATA_DIR / JSON_FILE_LIST
    print(f"Creating: {list_path}")
    with open(list_path, "w", encoding="utf-8") as f:
        json.dump(file_list, f, indent=2, ensure_ascii=False)
    files_created.append(str(list_path))
    print(f"  Size: {list_path.stat().st_size / (1024 * 1024):.1f} MB")

    return files_created


def print_summary(data):
    """Print extraction summary."""
    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"\nTotal Datasets: {data['metadata']['total_datasets']}")
    print(f"Total Files: {data['metadata']['total_files']}")
    print(f"Total Pages: {data['metadata']['total_pages']}")
    print(f"Total Size: {data['metadata']['total_size_mb']:.2f} MB")
    print("\nBreakdown by Dataset:")
    print("-" * 60)
    print(f"{'Dataset':<12} {'Files':<8} {'Pages':<8} {'Size (MB)':<10}")
    print("-" * 60)
    for dataset in data["datasets"]:
        print(
            f"Data Set {dataset['dataset_number']:<3}  "
            f"{dataset.get('file_count', 0):<8} "
            f"{dataset.get('total_pages', 0):<8} "
            f"{dataset.get('total_size_mb', 0):<10.2f}"
        )
    print("-" * 60)


def main():
    print("Epstein DOJ Files - PDF to JSON Converter")
    print("=" * 60)
    print()

    # Check for required tools
    try:
        subprocess.run(["pdftotext", "-v"], capture_output=True, timeout=5)
    except FileNotFoundError:
        print("Error: pdftotext not found")
        print("Install with: brew install poppler")
        sys.exit(1)

    data = process_all_pdfs()
    if data is None:
        print("Error: Could not process PDFs")
        sys.exit(1)

    print_summary(data)
    files_created = save_json_files(data)

    print("\n" + "=" * 60)
    print("COMPLETE!")
    print("=" * 60)
    print("\nFiles created:")
    for f in files_created:
        print(f"  - {f}")

    print("\nUsage:")
    print("  python -m src.extractor   # Re-run extraction")
    print("  python -m src.search      # CLI search")
    print("  python -m src.server      # Start web interface")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
