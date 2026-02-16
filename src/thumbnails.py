#!/usr/bin/env python3
"""
Batch thumbnail generator for Epstein DOJ PDF files.

Renders every page of every PDF as a JPEG thumbnail using PyMuPDF (fitz).
Thumbnails are stored in data/thumbnails/data-set-N/{stem}_p{page:03d}.jpg.

Usage:
    python -m src.thumbnails                    # Generate all datasets
    python -m src.thumbnails --dataset 1        # Dataset 1 only
    python -m src.thumbnails --dataset 1 3 5    # Specific datasets
    python -m src.thumbnails --workers 4        # Concurrent threads
    python -m src.thumbnails --width 800        # Custom width (px)
    python -m src.thumbnails --force            # Regenerate existing
"""

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import fitz  # PyMuPDF

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    PDF_DIR, NUM_DATASETS,
    THUMB_DIR, THUMB_WIDTH, THUMB_QUALITY, THUMB_WORKERS,
)

logger = logging.getLogger(__name__)


def render_pdf_pages(pdf_path, output_dir, width, quality, force):
    """Render all pages of a PDF as JPEG thumbnails.

    Returns (generated, skipped, failed) counts.
    """
    generated = 0
    skipped = 0

    try:
        doc = fitz.open(str(pdf_path))
    except Exception as e:
        logger.error("thumbnail_open_error", extra={"data": {
            "filename": pdf_path.name,
        }}, exc_info=True)
        print(f"  Error opening {pdf_path.name}: {e}")
        return 0, 0, 1

    try:
        for page_num in range(len(doc)):
            out_path = output_dir / f"{pdf_path.stem}_p{page_num + 1:03d}.jpg"

            if out_path.exists() and not force:
                skipped += 1
                continue

            try:
                page = doc[page_num]
                zoom = width / page.rect.width
                mat = fitz.Matrix(zoom, zoom)
                pix = page.get_pixmap(matrix=mat)
                pix.save(str(out_path), output="jpeg", jpg_quality=quality)
                generated += 1
            except Exception as e:
                logger.error("thumbnail_render_error", extra={"data": {
                    "filename": pdf_path.name, "page": page_num + 1,
                }}, exc_info=True)
                print(f"  Error rendering {pdf_path.name} page {page_num + 1}: {e}")
                return generated, skipped, 1

        return generated, skipped, 0
    finally:
        doc.close()


def generate_dataset(dataset_num, workers, width, quality, force):
    """Generate thumbnails for all PDFs in a dataset."""
    print(f"\n{'=' * 70}")
    print(f"  Data Set {dataset_num}")
    print(f"{'=' * 70}")

    dataset_dir = PDF_DIR / f"data-set-{dataset_num}"
    if not dataset_dir.exists():
        print(f"  PDF directory not found: {dataset_dir}")
        return 0, 0, 0

    output_dir = THUMB_DIR / f"data-set-{dataset_num}"
    output_dir.mkdir(parents=True, exist_ok=True)

    pdf_files = sorted(dataset_dir.glob("*.pdf"))
    if not pdf_files:
        print(f"  No PDF files found in {dataset_dir}")
        return 0, 0, 0

    print(f"  PDFs: {len(pdf_files)}")
    print(f"  Output: {output_dir}")

    total_generated = 0
    total_skipped = 0
    total_failed = 0
    processed = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for pdf_path in pdf_files:
            future = pool.submit(
                render_pdf_pages, pdf_path, output_dir, width, quality, force,
            )
            futures[future] = pdf_path

        for future in as_completed(futures):
            pdf_path = futures[future]
            processed += 1

            try:
                gen, skip, fail = future.result()
                total_generated += gen
                total_skipped += skip
                total_failed += fail

                if gen > 0:
                    logger.info("thumbnail_pdf_complete", extra={"data": {
                        "filename": pdf_path.name, "dataset": dataset_num,
                        "generated": gen, "skipped": skip,
                    }})
                    print(f"  [{processed}/{len(pdf_files)}] "
                          f"{pdf_path.name}: {gen} generated, {skip} skipped")
                elif processed % 50 == 0 or processed == len(pdf_files):
                    print(f"  [{processed}/{len(pdf_files)}] progress...")
            except Exception as e:
                total_failed += 1
                logger.error("thumbnail_pdf_error", extra={"data": {
                    "filename": pdf_path.name, "dataset": dataset_num,
                }}, exc_info=True)
                print(f"  [{processed}/{len(pdf_files)}] "
                      f"{pdf_path.name}: ERROR — {e}")

    logger.info("thumbnail_dataset_complete", extra={"data": {
        "dataset": dataset_num, "generated": total_generated,
        "skipped": total_skipped, "failed": total_failed,
    }})

    print(f"\n  Data Set {dataset_num} complete:")
    print(f"    Generated: {total_generated}")
    print(f"    Skipped:   {total_skipped}")
    print(f"    Failed:    {total_failed}")

    return total_generated, total_skipped, total_failed


def main():
    from src.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Epstein DOJ Files — Thumbnail Generator",
        epilog="Examples:\n"
               "  python -m src.thumbnails                   # All datasets\n"
               "  python -m src.thumbnails --dataset 1       # Dataset 1 only\n"
               "  python -m src.thumbnails --dataset 1 3 5   # Specific datasets\n"
               "  python -m src.thumbnails --workers 4       # 4 threads\n"
               "  python -m src.thumbnails --width 800       # 800px wide\n"
               "  python -m src.thumbnails --force           # Regenerate all\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dataset", nargs="+", type=int,
        help=f"Dataset number(s) to process (default: all 1-{NUM_DATASETS})",
    )
    parser.add_argument(
        "--workers", type=int, default=THUMB_WORKERS,
        help=f"Concurrent generation threads (default: {THUMB_WORKERS})",
    )
    parser.add_argument(
        "--width", type=int, default=THUMB_WIDTH,
        help=f"Thumbnail width in pixels (default: {THUMB_WIDTH})",
    )
    parser.add_argument(
        "--quality", type=int, default=THUMB_QUALITY,
        help=f"JPEG quality 1-100 (default: {THUMB_QUALITY})",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Regenerate existing thumbnails",
    )
    args = parser.parse_args()

    datasets = args.dataset or list(range(1, NUM_DATASETS + 1))
    for d in datasets:
        if d < 1 or d > NUM_DATASETS:
            print(f"Error: Dataset {d} is out of range (1-{NUM_DATASETS})")
            sys.exit(1)

    start_time = time.time()

    logger.info("thumbnails_started", extra={"data": {
        "datasets": datasets, "workers": args.workers,
        "width": args.width, "quality": args.quality, "force": args.force,
    }})

    print("=" * 70)
    print("Epstein DOJ Files — Thumbnail Generator")
    print("=" * 70)
    print(f"  Datasets:   {', '.join(str(d) for d in datasets)}")
    print(f"  Workers:    {args.workers}")
    print(f"  Width:      {args.width}px")
    print(f"  Quality:    {args.quality}")
    print(f"  Output:     {THUMB_DIR.resolve()}")
    if args.force:
        print("  Mode:       FORCE (regenerating all)")

    THUMB_DIR.mkdir(parents=True, exist_ok=True)

    grand_generated = 0
    grand_skipped = 0
    grand_failed = 0

    for dataset_num in datasets:
        gen, skip, fail = generate_dataset(
            dataset_num, args.workers, args.width, args.quality, args.force,
        )
        grand_generated += gen
        grand_skipped += skip
        grand_failed += fail

    elapsed = time.time() - start_time

    logger.info("thumbnails_complete", extra={"data": {
        "generated": grand_generated, "skipped": grand_skipped,
        "failed": grand_failed, "elapsed_s": round(elapsed, 1),
    }})

    print(f"\n{'=' * 70}")
    print(f"  Complete! ({elapsed:.1f}s)")
    print(f"    Generated: {grand_generated}")
    print(f"    Skipped:   {grand_skipped}")
    print(f"    Failed:    {grand_failed}")
    print(f"    Output:    {THUMB_DIR.resolve()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
