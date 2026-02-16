#!/usr/bin/env python3
"""
Alternative PDF extractor using pdfplumber (pure Python, no external tools needed).
Use this if poppler (pdftotext/pdfinfo) is not available.
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pdfplumber

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    DATA_DIR, PDF_DIR, NUM_DATASETS, SOURCE_URL,
    JSON_FULL, JSON_SUMMARY,
)

logger = logging.getLogger(__name__)


def process_pdfs(num_datasets=NUM_DATASETS):
    """Process PDFs with pdfplumber for better text extraction."""
    data = {
        "metadata": {
            "extraction_date": datetime.now(timezone.utc).isoformat(),
            "total_datasets": num_datasets,
            "source": SOURCE_URL,
            "extraction_tool": "pdfplumber",
        },
        "datasets": [],
    }

    for i in range(1, num_datasets + 1):
        dataset_dir = PDF_DIR / f"data-set-{i}"

        dataset = {
            "dataset_number": i,
            "dataset_name": f"data-set-{i}",
            "files": [],
        }

        if not dataset_dir.exists():
            data["datasets"].append(dataset)
            continue

        pdf_files = sorted(dataset_dir.glob("*.pdf"))
        print(f"Processing Data Set {i}: {len(pdf_files)} PDFs")

        for pdf_path in pdf_files:
            print(f"  Extracting: {pdf_path.name}")
            try:
                with pdfplumber.open(pdf_path) as pdf:
                    metadata = pdf.metadata
                    full_text = ""
                    pages_data = []

                    for page_num, page in enumerate(pdf.pages, 1):
                        page_text = page.extract_text() or ""
                        full_text += page_text + "\n\n"
                        tables = page.extract_tables()
                        pages_data.append({
                            "page_number": page_num,
                            "text": page_text,
                            "has_tables": len(tables) > 0,
                            "table_count": len(tables),
                            "tables": tables if tables else [],
                        })

                    file_data = {
                        "filename": pdf_path.name,
                        "filepath": str(pdf_path),
                        "size_bytes": pdf_path.stat().st_size,
                        "size_mb": round(pdf_path.stat().st_size / (1024 * 1024), 2),
                        "page_count": len(pdf.pages),
                        "metadata": {
                            "title": metadata.get("Title", ""),
                            "author": metadata.get("Author", ""),
                            "subject": metadata.get("Subject", ""),
                            "creator": metadata.get("Creator", ""),
                            "producer": metadata.get("Producer", ""),
                            "creation_date": str(metadata.get("CreationDate", "")),
                        },
                        "full_text": full_text.strip(),
                        "text_length": len(full_text),
                        "pages": pages_data,
                    }
                    dataset["files"].append(file_data)
                    logger.info("pdf_extracted", extra={"data": {
                        "filename": pdf_path.name, "dataset": i,
                        "pages": len(pdf.pages), "text_length": len(full_text),
                    }})

            except Exception as e:
                logger.error("pdf_extraction_error", extra={"data": {
                    "filename": pdf_path.name, "dataset": i,
                }}, exc_info=True)
                print(f"    Error processing {pdf_path.name}: {e}")
                dataset["files"].append({
                    "filename": pdf_path.name,
                    "error": str(e),
                })

        dataset["file_count"] = len(dataset["files"])
        data["datasets"].append(dataset)

    return data


def main():
    from src.logging_setup import setup_logging
    setup_logging()

    print("Starting PDF extraction with pdfplumber...")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    data = process_pdfs()

    # Save full JSON
    full_path = DATA_DIR / JSON_FULL
    with open(full_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    # Save summary JSON (without full text)
    summary_data = json.loads(json.dumps(data))
    for dataset in summary_data["datasets"]:
        for file in dataset["files"]:
            if "full_text" in file:
                file["text_preview"] = file["full_text"][:500] + "..."
                del file["full_text"]
            if "pages" in file:
                for page in file["pages"]:
                    if "text" in page:
                        del page["text"]

    summary_path = DATA_DIR / JSON_SUMMARY
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_data, f, indent=2, ensure_ascii=False)

    logger.info("extraction_complete", extra={"data": {
        "tool": "pdfplumber",
        "files": [str(full_path), str(summary_path)],
    }})

    print("\nExtraction complete!")
    print("Files created:")
    print(f"  - {full_path} (with full text)")
    print(f"  - {summary_path} (metadata only)")


if __name__ == "__main__":
    main()
