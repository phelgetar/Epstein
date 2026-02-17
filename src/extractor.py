#!/usr/bin/env python3
"""
Epstein DOJ Files - PDF to JSON Converter
Extracts text and metadata from all downloaded PDFs into a searchable JSON file.
Uses pdftotext/pdfinfo (Poppler) for text extraction.
"""

import json
import logging
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

logger = logging.getLogger(__name__)


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
        logger.error("pdftotext_error", extra={"data": {
            "file": str(pdf_path),
        }}, exc_info=True)
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
        logger.error("pdfinfo_error", extra={"data": {
            "file": str(pdf_path),
        }}, exc_info=True)
        print(f"    Error getting PDF info: {e}")
        return {}


def process_single_pdf(pdf_path):
    """Process a single PDF file."""

    stats = pdf_path.stat()
    pdf_info = extract_pdf_info(pdf_path)
    full_text = extract_pdf_text(pdf_path)

    pages = pdf_info.get("Pages", "0")
    try:
        pages = int(pages)
    except ValueError:
        pages = 0

    # Store path relative to project root with leading / for absolute URL paths
    try:
        relative_path = "/" + str(pdf_path.relative_to(PROJECT_ROOT))
    except ValueError:
        relative_path = str(pdf_path)

    return {
        "filename": pdf_path.name,
        "filepath": relative_path,
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
        logger.error("pdf_dir_not_found", extra={"data": {
            "directory": str(PDF_DIR),
        }})
        print(f"Error: Directory '{PDF_DIR}' does not exist")
        return None

    total_files = 0
    total_size = 0
    total_pages = 0
    total_failed = 0
    dataset_failures = {}

    for i in range(1, NUM_DATASETS + 1):
        dataset_dir = PDF_DIR / f"data-set-{i}"

        dataset = {
            "dataset_number": i,
            "dataset_name": f"data-set-{i}",
            "dataset_url": f"{SOURCE_URL}/data-set-{i}-files",
            "files": [],
        }

        if not dataset_dir.exists():
            data["datasets"].append(dataset)
            continue

        pdf_files = sorted(dataset_dir.glob("*.pdf"))
        if not pdf_files:
            data["datasets"].append(dataset)
            continue

        ds_failed = 0

        for pdf_path in pdf_files:
            try:
                file_data = process_single_pdf(pdf_path)
                dataset["files"].append(file_data)
                total_files += 1
                total_size += file_data["size_mb"]
                total_pages += file_data["pages"]
                logger.info("pdf_extracted", extra={"data": {
                    "filename": pdf_path.name, "dataset": i,
                    "pages": file_data["pages"],
                    "text_length": file_data["text_length"],
                    "size_mb": file_data["size_mb"],
                }})
            except Exception as e:
                ds_failed += 1
                total_failed += 1
                logger.error("pdf_extraction_error", extra={"data": {
                    "filename": pdf_path.name, "dataset": i,
                }}, exc_info=True)
                print(f"  Error: {pdf_path.name} — {e}")
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
        if ds_failed > 0:
            dataset_failures[i] = ds_failed
        data["datasets"].append(dataset)

    data["metadata"]["total_files"] = total_files
    data["metadata"]["total_pages"] = total_pages
    data["metadata"]["total_size_mb"] = round(total_size, 2)
    data["metadata"]["total_failed"] = total_failed

    return data, dataset_failures


def create_search_index(data):
    """Create a simplified flat search index."""
    search_index = []
    for dataset in data["datasets"]:
        for file_info in dataset["files"]:
            if "full_text" in file_info and "error" not in file_info:
                full_text = file_info["full_text"]

                # Split on form-feed characters to find page boundaries
                pages_text = full_text.split('\f')
                page_offsets = []
                offset = 0
                for page_text in pages_text:
                    page_offsets.append(offset)
                    offset += len(page_text) + 1  # +1 for the \f character

                search_index.append({
                    "dataset": dataset["dataset_number"],
                    "filename": file_info["filename"],
                    "filepath": file_info["filepath"],
                    "pages": file_info.get("pages", 0),
                    "text": full_text,
                    "page_offsets": page_offsets,
                })
    return search_index


def save_json_files(data):
    """Save multiple JSON output formats to data/."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    files_created = []

    # 1. Full JSON
    full_path = DATA_DIR / JSON_FULL
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    files_created.append(str(full_path))
    logger.info("json_file_created", extra={"data": {
        "file": str(full_path),
        "size_mb": round(full_path.stat().st_size / (1024 * 1024), 1),
    }})

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
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)
    files_created.append(str(summary_path))
    logger.info("json_file_created", extra={"data": {
        "file": str(summary_path),
        "size_mb": round(summary_path.stat().st_size / (1024 * 1024), 1),
    }})

    # 3. Search index (flat)
    search_index = create_search_index(data)
    search_path = DATA_DIR / JSON_SEARCH_INDEX
    with open(search_path, "w", encoding="utf-8") as f:
        json.dump(search_index, f, indent=2, ensure_ascii=False)
    files_created.append(str(search_path))
    logger.info("json_file_created", extra={"data": {
        "file": str(search_path),
        "size_mb": round(search_path.stat().st_size / (1024 * 1024), 1),
    }})

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
    with open(list_path, "w", encoding="utf-8") as f:
        json.dump(file_list, f, indent=2, ensure_ascii=False)
    files_created.append(str(list_path))
    logger.info("json_file_created", extra={"data": {
        "file": str(list_path),
        "size_mb": round(list_path.stat().st_size / (1024 * 1024), 1),
    }})

    return files_created


def print_summary(data, dataset_failures):
    """Print extraction summary."""
    print("\n" + "=" * 60)
    print("EXTRACTION SUMMARY")
    print("=" * 60)
    print(f"\n  Files:    {data['metadata']['total_files']}")
    print(f"  Pages:    {data['metadata']['total_pages']}")
    print(f"  Size:     {data['metadata']['total_size_mb']:.2f} MB")
    print(f"  Failed:   {data['metadata']['total_failed']}")
    if dataset_failures:
        print(f"\n  Failures by dataset:")
        for ds, count in sorted(dataset_failures.items()):
            print(f"    Data Set {ds}: {count} failed")


def main():
    from src.logging_setup import setup_logging
    setup_logging()

    print("=" * 60)
    print("Epstein DOJ Files — PDF Extractor")
    print("=" * 60)
    print(f"  Datasets:   1-{NUM_DATASETS}")
    print(f"  Tool:       pdftotext/pdfinfo (Poppler)")
    print(f"  Output:     {DATA_DIR.resolve()}")
    print()

    logger.info("extractor_started", extra={"data": {
        "datasets": list(range(1, NUM_DATASETS + 1)),
        "tool": "pdftotext/pdfinfo",
        "output_dir": str(DATA_DIR.resolve()),
    }})

    # Check for required tools
    try:
        subprocess.run(["pdftotext", "-v"], capture_output=True, timeout=5)
    except FileNotFoundError:
        print("Error: pdftotext not found")
        print("Install with: brew install poppler")
        sys.exit(1)

    result = process_all_pdfs()
    if result is None:
        print("Error: Could not process PDFs")
        sys.exit(1)

    data, dataset_failures = result
    print_summary(data, dataset_failures)
    files_created = save_json_files(data)

    logger.info("extraction_complete", extra={"data": {
        "total_files": data["metadata"]["total_files"],
        "total_pages": data["metadata"]["total_pages"],
        "total_size_mb": data["metadata"]["total_size_mb"],
        "total_failed": data["metadata"]["total_failed"],
        "dataset_failures": dataset_failures,
        "files_created": len(files_created),
    }})

    print(f"\n  JSON files written: {len(files_created)}")
    print(f"  Output: {DATA_DIR.resolve()}")
    print("=" * 60)


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
