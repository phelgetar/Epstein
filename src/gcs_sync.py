#!/usr/bin/env python3
"""
Sync local files to Google Cloud Storage.

Uploads new or changed PDFs, thumbnails, and classifications to the
GCS bucket configured via GCS_BASE_URL. Uses gcloud storage rsync
with checksum-based diffing so only new/modified files are transferred.

Usage:
    python -m src.gcs_sync                    # Sync everything
    python -m src.gcs_sync --pdfs             # Sync PDFs/videos/GDrive files only
    python -m src.gcs_sync --thumbnails       # Sync thumbnails only
    python -m src.gcs_sync --classifications  # Sync classification JSONs only
    python -m src.gcs_sync --dry-run          # Show what would be synced
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import PROJECT_ROOT, PDF_DIR, THUMB_DIR, CLASSIFY_DIR, GCS_BASE_URL


def _get_bucket():
    """Extract gs:// bucket URL from GCS_BASE_URL."""
    if not GCS_BASE_URL:
        print("ERROR: GCS_BASE_URL is not set. Add it to your .env file.")
        print("  Example: GCS_BASE_URL=https://storage.googleapis.com/epstein-doj-files-jarheads")
        sys.exit(1)
    # https://storage.googleapis.com/bucket-name -> gs://bucket-name
    parsed = urlparse(GCS_BASE_URL)
    bucket = parsed.path.lstrip("/").split("/")[0]
    return f"gs://{bucket}"


def _check_gcloud():
    """Check that gcloud CLI is installed."""
    if not shutil.which("gcloud"):
        print("ERROR: gcloud CLI is not installed.")
        print("  Install: brew install google-cloud-sdk")
        sys.exit(1)


def _rsync(local_path, gcs_path, dry_run=False):
    """Run gcloud storage rsync for a directory."""
    if not local_path.exists():
        print(f"  SKIP (missing): {local_path}")
        return

    cmd = ["gcloud", "storage", "rsync", "-r", str(local_path) + "/", gcs_path + "/", "--checksums-only"]
    if dry_run:
        cmd.append("--dry-run")

    print(f"\n  Syncing {local_path.name} -> {gcs_path}")
    print(f"  $ {' '.join(cmd)}\n")
    subprocess.run(cmd, check=True)


def sync_pdfs(bucket, dry_run=False):
    """Sync PDFs, videos, and Google Drive files to GCS."""
    print("Syncing PDFs/videos/GDrive files...")
    _rsync(PDF_DIR, f"{bucket}/epstein_doj_files", dry_run)


def sync_thumbnails(bucket, dry_run=False):
    """Sync thumbnail images to GCS."""
    print("Syncing thumbnails...")
    _rsync(THUMB_DIR, f"{bucket}/thumbnails", dry_run)


def sync_classifications(bucket, dry_run=False):
    """Sync classification JSON files to GCS."""
    print("Syncing classifications...")
    _rsync(CLASSIFY_DIR, f"{bucket}/classifications", dry_run)


def main():
    parser = argparse.ArgumentParser(description="Sync local files to Google Cloud Storage")
    parser.add_argument("--pdfs", action="store_true", help="Sync PDFs/videos/GDrive files only")
    parser.add_argument("--thumbnails", action="store_true", help="Sync thumbnails only")
    parser.add_argument("--classifications", action="store_true", help="Sync classification JSONs only")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be synced without uploading")
    args = parser.parse_args()

    _check_gcloud()
    bucket = _get_bucket()

    # If no specific flag, sync everything
    sync_all = not (args.pdfs or args.thumbnails or args.classifications)

    print("=" * 60)
    print("GCS Sync")
    print("=" * 60)
    print(f"  Bucket: {bucket}")
    print(f"  Dry run: {args.dry_run}")

    if sync_all or args.pdfs:
        sync_pdfs(bucket, args.dry_run)
    if sync_all or args.thumbnails:
        sync_thumbnails(bucket, args.dry_run)
    if sync_all or args.classifications:
        sync_classifications(bucket, args.dry_run)

    print("\n" + "=" * 60)
    print("  Sync complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
