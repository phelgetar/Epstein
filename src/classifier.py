#!/usr/bin/env python3
"""
Batch image classifier for Epstein DOJ PDF thumbnails using Google Gemini Flash.

Sends each page thumbnail to Gemini 2.0 Flash for classification, producing
a description, category tags, and content type for every page image.
Results are stored in data/classifications/data-set-N.json.

Requires GOOGLE_API_KEY environment variable (or --dry-run to skip API calls).

Usage:
    python -m src.classifier                    # Classify all datasets
    python -m src.classifier --dataset 1        # Dataset 1 only
    python -m src.classifier --dataset 1 3 5    # Specific datasets
    python -m src.classifier --workers 10       # Concurrent API calls
    python -m src.classifier --rpm 2000         # Rate limit (requests/min)
    python -m src.classifier --max-cost 50      # Stop after $50 spent
    python -m src.classifier --dry-run          # Count without calling API
    python -m src.classifier --force            # Reclassify existing
"""

import argparse
import json
import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from google import genai
from google.genai import types
from pydantic import BaseModel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    THUMB_DIR, NUM_DATASETS,
    CLASSIFY_DIR, CLASSIFY_MODEL, CLASSIFY_WORKERS, CLASSIFY_RPM,
    CLASSIFY_SAVE_INTERVAL,
)

logger = logging.getLogger(__name__)


# ─── Schema & Prompt ─────────────────────────────────────────

class ImageClassification(BaseModel):
    description: str
    tags: list[str]
    content_type: str
    people: list[str] = []


CLASSIFY_PROMPT = (
    "Classify this scanned document page image.\n"
    "description: 1-2 sentence summary of visible content.\n"
    "tags: 3-8 descriptive keywords (e.g. photograph, letter, FBI, "
    "handwriting, redacted, table, map, signature, envelope, blank).\n"
    "content_type: exactly one of: photograph, document, handwritten, "
    "diagram, map, blank, other.\n"
    "people: list any recognizable public figures visible in the image "
    "(e.g. Bill Clinton, Donald Trump, Ghislaine Maxwell). "
    "Only include names you are confident about. "
    "If no recognizable people are visible, return an empty list []."
)


# ─── Rate Limiter ─────────────────────────────────────────────

class RateLimiter:
    """Thread-safe token-bucket rate limiter."""

    def __init__(self, rpm):
        self.interval = 60.0 / rpm
        self.lock = threading.Lock()
        self.last_request = time.monotonic()

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            wait = self.last_request + self.interval - now
            if wait > 0:
                time.sleep(wait)
            self.last_request = time.monotonic()


# ─── Per-Image Classification ─────────────────────────────────

MAX_RETRIES = 5
INITIAL_BACKOFF = 2.0  # seconds


def classify_image(client, thumb_path, rate_limiter):
    """Classify a single thumbnail image with retry on rate-limit errors.

    Returns (filename, result_dict, tokens_used) or (filename, None, 0).
    """
    filename = thumb_path.name

    try:
        image_bytes = thumb_path.read_bytes()
    except Exception as e:
        logger.error("classify_read_error", extra={"data": {
            "filename": filename,
        }}, exc_info=True)
        print(f"  Error reading {filename}: {e}")
        return filename, None, 0

    for attempt in range(MAX_RETRIES):
        rate_limiter.acquire()

        try:
            response = client.models.generate_content(
                model=CLASSIFY_MODEL,
                contents=[
                    CLASSIFY_PROMPT,
                    types.Part.from_bytes(data=image_bytes, mime_type="image/jpeg"),
                ],
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=ImageClassification,
                ),
            )

            tokens = 0
            if response.usage_metadata:
                tokens = response.usage_metadata.total_token_count or 0

            parsed = json.loads(response.text)
            logger.debug("classify_success", extra={"data": {
                "filename": filename, "tokens": tokens,
                "content_type": parsed.get("content_type"),
            }})
            return filename, parsed, tokens

        except Exception as e:
            err_str = str(e)
            is_rate_limit = "429" in err_str or "RESOURCE_EXHAUSTED" in err_str
            if is_rate_limit and attempt < MAX_RETRIES - 1:
                backoff = INITIAL_BACKOFF * (2 ** attempt)
                logger.warning("classify_rate_limited", extra={"data": {
                    "filename": filename, "attempt": attempt + 1,
                    "max_retries": MAX_RETRIES, "backoff_s": backoff,
                }})
                print(f"  Rate limited on {filename}, retrying in {backoff:.0f}s "
                      f"(attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(backoff)
                continue
            logger.error("classify_error", extra={"data": {
                "filename": filename,
            }}, exc_info=True)
            print(f"  Error classifying {filename}: {e}")
            return filename, None, 0

    return filename, None, 0


# ─── Per-Dataset Processing ───────────────────────────────────

def classify_dataset(dataset_num, client, rate_limiter, workers,
                     force, dry_run, cost_state, interrupted):
    """Classify all thumbnails in a dataset. Returns (classified, skipped, failed)."""
    print(f"\n{'=' * 70}")
    print(f"  Data Set {dataset_num}")
    print(f"{'=' * 70}")

    thumb_dir = THUMB_DIR / f"data-set-{dataset_num}"
    if not thumb_dir.exists():
        print(f"  Thumbnail directory not found: {thumb_dir}")
        print(f"  Run first: python -m src.thumbnails --dataset {dataset_num}")
        return 0, 0, 0

    output_path = CLASSIFY_DIR / f"data-set-{dataset_num}.json"

    # Load existing classifications for resume support
    existing = {}
    metadata = {"model": CLASSIFY_MODEL, "total_classified": 0, "last_updated": ""}
    if output_path.exists() and not force:
        try:
            with open(output_path, "r") as f:
                data = json.load(f)
            existing = data.get("pages", {})
            metadata = data.get("metadata", metadata)
        except Exception:
            pass

    # Enumerate thumbnails
    all_thumbs = sorted(thumb_dir.glob("*.jpg"))
    if not all_thumbs:
        print(f"  No thumbnails found in {thumb_dir}")
        return 0, 0, 0

    # Filter to unclassified (or all if --force)
    if force:
        to_classify = all_thumbs
    else:
        to_classify = [t for t in all_thumbs if t.name not in existing]

    print(f"  Thumbnails:         {len(all_thumbs):,}")
    print(f"  Already classified: {len(existing):,}")
    print(f"  To classify:        {len(to_classify):,}")

    if dry_run:
        estimated_cost = len(to_classify) * 0.0002
        print(f"  Estimated cost:     ${estimated_cost:.2f}")
        return 0, len(existing), 0

    if not to_classify:
        return 0, len(existing), 0

    # Thread-safe state for incremental saves
    results_lock = threading.Lock()
    classified = 0
    failed = 0
    unsaved_count = 0

    def save_progress():
        nonlocal unsaved_count
        metadata["total_classified"] = len(existing)
        metadata["last_updated"] = datetime.now(timezone.utc).isoformat()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump({"metadata": metadata, "pages": existing}, f)
        unsaved_count = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for thumb in to_classify:
            if interrupted.is_set():
                break
            future = pool.submit(classify_image, client, thumb, rate_limiter)
            futures[future] = thumb

        for future in as_completed(futures):
            if interrupted.is_set():
                break

            try:
                filename, result, tokens = future.result()
            except Exception as e:
                failed += 1
                print(f"  ERROR: {e}")
                continue

            with results_lock:
                if result:
                    existing[filename] = result
                    classified += 1
                    unsaved_count += 1
                    cost_state["total_tokens"] += tokens
                else:
                    failed += 1

                processed = classified + failed

                # Progress report every 100
                if processed % 100 == 0 or processed == len(to_classify):
                    est_cost = cost_state["total_tokens"] * 0.00000015
                    logger.info("classify_progress", extra={"data": {
                        "dataset": dataset_num, "processed": processed,
                        "total": len(to_classify), "classified": classified,
                        "failed": failed, "total_tokens": cost_state["total_tokens"],
                        "est_cost": round(est_cost, 4),
                    }})
                    print(f"  [{processed:,}/{len(to_classify):,}] "
                          f"classified={classified:,} failed={failed:,} "
                          f"tokens={cost_state['total_tokens']:,} ~${est_cost:.2f}")

                # Incremental save
                if unsaved_count >= CLASSIFY_SAVE_INTERVAL:
                    save_progress()

                # Cost cap check
                if cost_state.get("max_cost"):
                    est_cost = cost_state["total_tokens"] * 0.00000015
                    if est_cost >= cost_state["max_cost"]:
                        logger.warning("classify_cost_cap_reached", extra={"data": {
                            "dataset": dataset_num, "est_cost": round(est_cost, 4),
                            "max_cost": cost_state["max_cost"],
                        }})
                        print(f"  COST CAP REACHED (~${est_cost:.2f})")
                        interrupted.set()

    # Final save
    save_progress()

    logger.info("classify_dataset_complete", extra={"data": {
        "dataset": dataset_num, "classified": classified,
        "skipped": len(existing) - classified, "failed": failed,
    }})

    print(f"\n  Data Set {dataset_num} complete:")
    print(f"    Classified: {classified:,}")
    print(f"    Skipped:    {len(existing) - classified:,}")
    print(f"    Failed:     {failed:,}")

    return classified, len(existing) - classified, failed


# ─── Dataset Argument Parsing ─────────────────────────────────

def _parse_datasets(spec: str) -> list[int]:
    """Parse dataset spec like '1,3,7-11' into a sorted list of ints."""
    result = set()
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            lo, hi = part.split("-", 1)
            lo, hi = int(lo.strip()), int(hi.strip())
            if lo > hi:
                lo, hi = hi, lo
            for d in range(lo, hi + 1):
                if d < 1 or d > NUM_DATASETS:
                    print(f"Error: Dataset {d} is out of range (1-{NUM_DATASETS})")
                    sys.exit(1)
                result.add(d)
        else:
            d = int(part)
            if d < 1 or d > NUM_DATASETS:
                print(f"Error: Dataset {d} is out of range (1-{NUM_DATASETS})")
                sys.exit(1)
            result.add(d)
    return sorted(result)


# ─── Main ─────────────────────────────────────────────────────

def main():
    from src.logging_setup import setup_logging
    setup_logging()

    parser = argparse.ArgumentParser(
        description="Epstein DOJ Files — Image Classifier (Gemini Flash)",
        epilog="Examples:\n"
               "  python -m src.classifier                        # All datasets\n"
               "  python -m src.classifier --dataset 1            # Dataset 1 only\n"
               "  python -m src.classifier --dataset 1,3,5        # Specific datasets\n"
               "  python -m src.classifier --dataset 7-11         # Range of datasets\n"
               "  python -m src.classifier --dataset 1,3,7-11     # Mix of both\n"
               "  python -m src.classifier --workers 10           # 10 concurrent calls\n"
               "  python -m src.classifier --rpm 15               # Free tier rate limit\n"
               "  python -m src.classifier --max-cost 50          # Stop at $50\n"
               "  python -m src.classifier --force                # Reclassify all\n"
               "  python -m src.classifier --dry-run              # Count and estimate cost\n",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dataset", type=str,
        help=f"Dataset(s) to process: comma-separated and/or ranges "
             f"(e.g. 1,2,3 or 7-11 or 1,3,7-11). Default: all 1-{NUM_DATASETS}",
    )
    parser.add_argument(
        "--workers", type=int, default=CLASSIFY_WORKERS,
        help=f"Concurrent API calls (default: {CLASSIFY_WORKERS})",
    )
    parser.add_argument(
        "--rpm", type=int, default=CLASSIFY_RPM,
        help=f"Rate limit in requests/minute (default: {CLASSIFY_RPM})",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Reclassify already-classified thumbnails",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Count thumbnails and estimate cost without calling API",
    )
    parser.add_argument(
        "--max-cost", type=float, default=None,
        help="Stop after estimated cost reaches this amount in dollars",
    )
    args = parser.parse_args()

    # Validate API key
    api_key = os.environ.get("GOOGLE_API_KEY")
    if not api_key and not args.dry_run:
        print("Error: GOOGLE_API_KEY environment variable is required")
        print("  export GOOGLE_API_KEY='your-api-key-here'")
        sys.exit(1)

    datasets = _parse_datasets(args.dataset) if args.dataset else list(range(1, NUM_DATASETS + 1))

    start_time = time.time()

    logger.info("classifier_started", extra={"data": {
        "model": CLASSIFY_MODEL, "datasets": datasets, "workers": args.workers,
        "rpm": args.rpm, "force": args.force, "dry_run": args.dry_run,
        "max_cost": args.max_cost,
    }})

    print("=" * 70)
    print("Epstein DOJ Files — Image Classifier")
    print("=" * 70)
    print(f"  Model:      {CLASSIFY_MODEL}")
    print(f"  Datasets:   {', '.join(str(d) for d in datasets)}")
    print(f"  Workers:    {args.workers}")
    print(f"  Rate limit: {args.rpm} RPM")
    print(f"  Output:     {CLASSIFY_DIR.resolve()}")
    if args.max_cost:
        print(f"  Cost cap:   ${args.max_cost:.2f}")
    if args.force:
        print("  Mode:       FORCE (reclassifying all)")
    if args.dry_run:
        print("  Mode:       DRY RUN (no API calls)")

    # Initialize client
    client = None
    if not args.dry_run:
        client = genai.Client(api_key=api_key)

    rate_limiter = RateLimiter(args.rpm)
    cost_state = {"total_tokens": 0, "max_cost": args.max_cost}

    CLASSIFY_DIR.mkdir(parents=True, exist_ok=True)

    # Graceful shutdown on Ctrl+C
    interrupted = threading.Event()
    original_sigint = signal.getsignal(signal.SIGINT)

    def handle_sigint(signum, frame):
        print("\n  Interrupt received — finishing current batch and saving...")
        interrupted.set()
        signal.signal(signal.SIGINT, original_sigint)  # second Ctrl+C = hard exit

    signal.signal(signal.SIGINT, handle_sigint)

    grand_classified = 0
    grand_skipped = 0
    grand_failed = 0

    for dataset_num in datasets:
        if interrupted.is_set():
            print(f"\n  Skipping remaining datasets (interrupted)")
            break

        cls, skip, fail = classify_dataset(
            dataset_num, client, rate_limiter,
            args.workers, args.force, args.dry_run, cost_state, interrupted,
        )
        grand_classified += cls
        grand_skipped += skip
        grand_failed += fail

    elapsed = time.time() - start_time
    est_cost = cost_state["total_tokens"] * 0.00000015

    logger.info("classifier_complete", extra={"data": {
        "classified": grand_classified, "skipped": grand_skipped,
        "failed": grand_failed, "total_tokens": cost_state["total_tokens"],
        "est_cost": round(est_cost, 4), "elapsed_s": round(elapsed, 1),
    }})

    print(f"\n{'=' * 70}")
    print(f"  Complete! ({elapsed:.1f}s)")
    print(f"    Classified: {grand_classified:,}")
    print(f"    Skipped:    {grand_skipped:,}")
    print(f"    Failed:     {grand_failed:,}")
    print(f"    Tokens:     {cost_state['total_tokens']:,}")
    print(f"    Est. cost:  ~${est_cost:.2f}")
    print(f"    Output:     {CLASSIFY_DIR.resolve()}")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
