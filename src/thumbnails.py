#!/usr/bin/env python3
"""
Batch thumbnail generator for Epstein DOJ files.

Renders PDF pages as JPEG thumbnails (PyMuPDF), and resizes/converts
JPG/TIF images (Pillow) for Google Drive datasets.

Thumbnails are stored in data/thumbnails/data-set-N/.

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
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    NUM_DATASETS, DATASET_REGISTRY,
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


def resize_image_file(image_path, output_dir, width, quality, force):
    """Resize a JPG or convert+resize a TIF to a JPEG thumbnail.

    Returns (generated, skipped, failed) counts.
    """
    out_path = output_dir / f"{image_path.stem}.jpg"

    if out_path.exists() and not force:
        return 0, 1, 0

    try:
        with Image.open(image_path) as img:
            if img.mode not in ("RGB", "L"):
                img = img.convert("RGB")
            if img.width > width:
                ratio = width / img.width
                new_size = (width, int(img.height * ratio))
                img = img.resize(new_size, Image.LANCZOS)
            img.save(str(out_path), "JPEG", quality=quality)
        return 1, 0, 0
    except Exception as e:
        logger.error("thumbnail_image_error", extra={"data": {
            "filename": image_path.name,
        }}, exc_info=True)
        print(f"  Error: {image_path.name} — {e}")
        return 0, 0, 1


def generate_dataset(dataset_num, workers, width, quality, force):
    """Generate thumbnails for all files in a dataset."""
    ds_info = DATASET_REGISTRY.get(dataset_num)
    if not ds_info:
        return 0, 0, 0

    # Skip media datasets (MP4/WAV — can't generate thumbnails)
    if ds_info.file_type == "media":
        return 0, 0, 0

    dataset_dir = ds_info.source_dir
    if not dataset_dir.exists():
        return 0, 0, 0

    output_dir = THUMB_DIR / f"data-set-{dataset_num}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Collect source files
    source_files = []
    for glob_pattern in ds_info.file_globs:
        source_files.extend(dataset_dir.glob(glob_pattern))
    source_files.sort(key=lambda p: p.name)

    if not source_files:
        return 0, 0, 0

    # Choose the right render function
    if ds_info.file_type == "pdf":
        render_fn = render_pdf_pages
    else:  # "image"
        render_fn = resize_image_file

    total_generated = 0
    total_skipped = 0
    total_failed = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for file_path in source_files:
            future = pool.submit(
                render_fn, file_path, output_dir, width, quality, force,
            )
            futures[future] = file_path

        for future in as_completed(futures):
            file_path = futures[future]

            try:
                gen, skip, fail = future.result()
                total_generated += gen
                total_skipped += skip
                total_failed += fail

                if gen > 0:
                    logger.info("thumbnail_file_complete", extra={"data": {
                        "filename": file_path.name, "dataset": dataset_num,
                        "generated": gen, "skipped": skip,
                    }})
            except Exception as e:
                total_failed += 1
                logger.error("thumbnail_file_error", extra={"data": {
                    "filename": file_path.name, "dataset": dataset_num,
                }}, exc_info=True)
                print(f"  Error: {file_path.name} — {e}")

    logger.info("thumbnail_dataset_complete", extra={"data": {
        "dataset": dataset_num, "generated": total_generated,
        "skipped": total_skipped, "failed": total_failed,
    }})

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

    datasets = args.dataset or sorted(DATASET_REGISTRY.keys())
    for d in datasets:
        if d not in DATASET_REGISTRY:
            print(f"Error: Dataset {d} is not in the registry (valid: {sorted(DATASET_REGISTRY.keys())})")
            sys.exit(1)

    start_time = time.time()

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
    print()

    logger.info("thumbnails_started", extra={"data": {
        "datasets": datasets, "workers": args.workers,
        "width": args.width, "quality": args.quality, "force": args.force,
        "output_dir": str(THUMB_DIR.resolve()),
    }})

    THUMB_DIR.mkdir(parents=True, exist_ok=True)

    grand_generated = 0
    grand_skipped = 0
    grand_failed = 0
    dataset_failures = {}

    for dataset_num in datasets:
        gen, skip, fail = generate_dataset(
            dataset_num, args.workers, args.width, args.quality, args.force,
        )
        grand_generated += gen
        grand_skipped += skip
        grand_failed += fail
        if fail > 0:
            dataset_failures[dataset_num] = fail

    elapsed = time.time() - start_time

    logger.info("thumbnails_complete", extra={"data": {
        "generated": grand_generated, "skipped": grand_skipped,
        "failed": grand_failed, "elapsed_s": round(elapsed, 1),
        "dataset_failures": dataset_failures,
    }})

    print(f"\n{'=' * 70}")
    print(f"  Complete! ({elapsed:.1f}s)")
    print(f"    Generated: {grand_generated}")
    print(f"    Skipped:   {grand_skipped}")
    print(f"    Failed:    {grand_failed}")
    if dataset_failures:
        print(f"\n  Failures by dataset:")
        for ds, count in sorted(dataset_failures.items()):
            print(f"    Data Set {ds}: {count} failed")
    print(f"\n  Output: {THUMB_DIR.resolve()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
