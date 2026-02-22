#!/usr/bin/env python3
"""
Deploy the Epstein DOJ Files web server to a remote cPanel host.

Rsyncs only web-facing files (no downloaders/extractors), installs
dependencies, writes Apache .htaccess proxy, and starts Gunicorn.

Usage:
    python -m src.deploy                      # Deploy to production
    python -m src.deploy --check              # Validate remote environment only
    python -m src.deploy --restart            # Restart the server process
    python -m src.deploy --stop               # Stop the server process
    python -m src.deploy --data-only          # Sync data files only (SQLite, thumbnails, classifications)
    python -m src.deploy --files-only         # Sync large files only (PDFs, images, videos)
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import PROJECT_ROOT, BASE_PATH

# ─── Remote Configuration ───────────────────────────────────

REMOTE_HOST = os.environ.get("DEPLOY_HOST", "jarheads@162.241.218.175")
REMOTE_DIR = os.environ.get("DEPLOY_DIR", "~/epstein_server")
REMOTE_PORT = int(os.environ.get("DEPLOY_PORT", "8000"))
PUBLIC_HTML = os.environ.get("DEPLOY_PUBLIC_HTML", "~/public_html")
GUNICORN_WORKERS = int(os.environ.get("DEPLOY_WORKERS", "4"))

# Files to deploy (relative to PROJECT_ROOT)
DEPLOY_FILES = [
    "src/__init__.py",
    "src/server.py",
    "src/search.py",
    "src/config.py",
    "src/build_index.py",
    "src/logging_setup.py",
    "src/init_db.py",
    "static/",
    "requirements-server.txt",
]

# Data files to sync (SQLite index, thumbnails, classifications)
DEPLOY_DATA = [
    "data/epstein_search.db",
    "data/thumbnails/",
    "data/classifications/",
]

# Large content files (PDFs, Google Drive images/videos)
DEPLOY_FILES_LARGE = [
    "epstein_doj_files/",  # ~176 GB — all PDFs, images, videos
]


def run(cmd, check=True, capture=False):
    """Run a command and print it."""
    print(f"  $ {cmd}")
    result = subprocess.run(
        cmd, shell=True, check=check,
        capture_output=capture, text=True,
    )
    return result


def ssh(cmd, check=True, capture=False):
    """Run a command on the remote host."""
    return run(f"ssh {REMOTE_HOST} '{cmd}'", check=check, capture=capture)


def check_remote():
    """Validate the remote environment."""
    print("\n  Checking remote environment...")

    r = ssh("python3 --version", capture=True)
    print(f"    Python: {r.stdout.strip()}")

    r = ssh("pip3 --version 2>/dev/null || echo 'NOT FOUND'", capture=True)
    print(f"    pip: {r.stdout.strip()[:60]}")

    r = ssh("df -h ~ | tail -1 | awk '{print $4}'", capture=True)
    print(f"    Free disk: {r.stdout.strip()}")

    r = ssh(f"test -d {REMOTE_DIR} && echo EXISTS || echo NEW", capture=True)
    print(f"    Deploy dir: {r.stdout.strip()}")

    print("  Remote check passed.\n")


def sync_files():
    """Rsync project files to the remote host."""
    print("\n  Syncing project files...")

    # Create remote directory structure
    ssh(f"mkdir -p {REMOTE_DIR}/src {REMOTE_DIR}/static {REMOTE_DIR}/data")

    # Sync source files
    for path in DEPLOY_FILES:
        local = PROJECT_ROOT / path
        if not local.exists():
            print(f"    SKIP (missing): {path}")
            continue

        if local.is_dir():
            run(f"rsync -avz --delete {local}/ {REMOTE_HOST}:{REMOTE_DIR}/{path}")
        else:
            parent = str(Path(path).parent)
            run(f"rsync -avz {local} {REMOTE_HOST}:{REMOTE_DIR}/{parent}/")

    print("  Sync complete.\n")


def sync_data():
    """Rsync data files (SQLite DB, thumbnails, classifications) to the remote host."""
    print("\n  Syncing data files...")

    ssh(f"mkdir -p {REMOTE_DIR}/data/thumbnails {REMOTE_DIR}/data/classifications")

    for path in DEPLOY_DATA:
        local = PROJECT_ROOT / path
        if not local.exists():
            print(f"    SKIP (missing): {path}")
            continue

        if local.is_dir():
            run(f"rsync -avz --progress {local}/ {REMOTE_HOST}:{REMOTE_DIR}/{path}")
        else:
            parent = str(Path(path).parent)
            run(f"rsync -avz --progress {local} {REMOTE_HOST}:{REMOTE_DIR}/{parent}/")

    print("  Data sync complete.\n")


def sync_large_files():
    """Rsync large content files (PDFs, images, videos) to the remote host."""
    print("\n  Syncing large files (this may take a while)...")

    for path in DEPLOY_FILES_LARGE:
        local = PROJECT_ROOT / path
        if not local.exists():
            print(f"    SKIP (missing): {path}")
            continue

        # Create remote directory
        ssh(f"mkdir -p {REMOTE_DIR}/{path}")

        # Use --progress and --partial for large transfers (resume on failure)
        run(f"rsync -avz --progress --partial {local}/ {REMOTE_HOST}:{REMOTE_DIR}/{path}",
            check=True)

    print("  Large file sync complete.\n")


def install_deps():
    """Install Python dependencies on the remote host."""
    print("\n  Installing dependencies...")
    ssh(f"cd {REMOTE_DIR} && pip3 install --user -r requirements-server.txt")
    print("  Dependencies installed.\n")


def write_htaccess():
    """Write Apache .htaccess for reverse proxy."""
    if not BASE_PATH:
        print("  WARNING: BASE_PATH not set. Skipping .htaccess generation.")
        print("  Set BASE_PATH env var (e.g. BASE_PATH=/a7f3x9k2m4p8)")
        return

    prefix = BASE_PATH.lstrip("/")
    htaccess_content = f"""
# Epstein DOJ Files — reverse proxy to Gunicorn
RewriteEngine On
RewriteRule ^{prefix}/(.*) http://127.0.0.1:{REMOTE_PORT}/{BASE_PATH}/$1 [P,L]
""".strip()

    print(f"\n  Writing .htaccess (prefix: /{prefix})...")
    # Append to existing .htaccess (don't overwrite WordPress rules)
    ssh(f"cat >> {PUBLIC_HTML}/.htaccess << 'HTEOF'\n\n{htaccess_content}\nHTEOF")
    print("  .htaccess updated.\n")


def write_env():
    """Write .env file on the remote host."""
    env_vars = {
        "BASE_PATH": BASE_PATH,
        "SENTRY_DSN": os.environ.get("SENTRY_DSN", ""),
        "SENTRY_ENVIRONMENT": "production",
        "DATABASE_URL": os.environ.get("DATABASE_URL", ""),
    }

    env_content = "\n".join(f"{k}={v}" for k, v in env_vars.items() if v)
    print("\n  Writing .env...")
    ssh(f"cat > {REMOTE_DIR}/.env << 'ENVEOF'\n{env_content}\nENVEOF")
    print("  .env written.\n")


def start_server():
    """Start Gunicorn on the remote host."""
    print("\n  Starting server...")
    ssh(f"cd {REMOTE_DIR} && nohup python3 -m gunicorn src.server:app "
        f"-k uvicorn.workers.UvicornWorker "
        f"--workers {GUNICORN_WORKERS} "
        f"--bind 127.0.0.1:{REMOTE_PORT} "
        f"--access-logfile - "
        f"--error-logfile data/gunicorn-error.log "
        f"> data/gunicorn.log 2>&1 &")
    print(f"  Server started on port {REMOTE_PORT}.\n")


def stop_server():
    """Stop Gunicorn on the remote host."""
    print("\n  Stopping server...")
    ssh(f"pkill -f 'gunicorn src.server:app' || echo 'No process found'")
    print("  Server stopped.\n")


def restart_server():
    """Restart Gunicorn on the remote host."""
    stop_server()
    start_server()


def deploy():
    """Full deployment."""
    print("=" * 70)
    print("Epstein DOJ Files — Production Deployment")
    print("=" * 70)
    print(f"\n  Remote:     {REMOTE_HOST}")
    print(f"  Deploy dir: {REMOTE_DIR}")
    print(f"  Port:       {REMOTE_PORT}")
    print(f"  Workers:    {GUNICORN_WORKERS}")
    print(f"  Base path:  {BASE_PATH or '(none)'}")

    check_remote()
    sync_files()
    sync_data()
    install_deps()
    write_env()
    stop_server()
    start_server()

    print("=" * 70)
    print("  Deployment complete!")
    if BASE_PATH:
        print(f"  URL: https://your-domain{BASE_PATH}/static/search.html")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Deploy to production")
    parser.add_argument("--check", action="store_true", help="Check remote only")
    parser.add_argument("--restart", action="store_true", help="Restart server")
    parser.add_argument("--stop", action="store_true", help="Stop server")
    parser.add_argument("--sync-only", action="store_true", help="Sync source files only")
    parser.add_argument("--data-only", action="store_true",
                        help="Sync data files only (SQLite DB, thumbnails, classifications)")
    parser.add_argument("--files-only", action="store_true",
                        help="Sync large files only (PDFs, images, videos — ~176 GB)")
    args = parser.parse_args()

    if args.check:
        check_remote()
    elif args.restart:
        restart_server()
    elif args.stop:
        stop_server()
    elif args.sync_only:
        sync_files()
    elif args.data_only:
        sync_data()
    elif args.files_only:
        sync_large_files()
    else:
        deploy()


if __name__ == "__main__":
    main()
