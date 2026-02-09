#!/usr/bin/env python3
"""
Secure HTTP server for the Epstein DOJ Files search interface.

Security measures:
- Binds to 127.0.0.1 only (not accessible from network)
- CORS restricted to localhost origins
- Content-Security-Policy headers
- Path traversal protection
- File extension allowlist
- Auto-reload on code changes via watchdog
"""

import http.server
import socketserver
import socket
import os
import sys
import webbrowser
import mimetypes
import signal
import time
import threading
from pathlib import Path

# Add project root to path so config can be imported
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    PROJECT_ROOT, STATIC_DIR, DATA_DIR, PDF_DIR,
    SERVER_HOST, PREFERRED_PORT, PORT_RANGE,
    ALLOWED_EXTENSIONS, WATCH_EXTENSIONS, WATCH_DIRS,
)


class SecureHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler with security hardening."""

    # Serve from project root so all subdirectories are accessible
    directory = str(PROJECT_ROOT)

    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        ".pdf": "application/pdf",
        ".json": "application/json",
    }

    def end_headers(self):
        origin = self.headers.get("Origin", "")
        allowed_origins = {
            f"http://127.0.0.1:{self.server.server_address[1]}",
            f"http://localhost:{self.server.server_address[1]}",
        }
        if origin in allowed_origins:
            self.send_header("Access-Control-Allow-Origin", origin)
        # Prevent MIME sniffing
        self.send_header("X-Content-Type-Options", "nosniff")
        # Clickjacking protection
        self.send_header("X-Frame-Options", "SAMEORIGIN")
        # XSS filter
        self.send_header("X-XSS-Protection", "1; mode=block")
        # Referrer policy
        self.send_header("Referrer-Policy", "no-referrer")
        # Content Security Policy - restrict to self and Google Fonts
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src 'self' https://fonts.gstatic.com; "
            "script-src 'self' 'unsafe-inline'; "
            "img-src 'self' data:; "
            "frame-src 'self';"
        )
        # Cache control for development
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        super().end_headers()

    def translate_path(self, path):
        """Override to serve files from project root and prevent path traversal."""
        # Get the translated path from parent
        translated = super().translate_path(path)
        real_path = os.path.realpath(translated)
        root = os.path.realpath(str(PROJECT_ROOT))

        # Block path traversal - resolved path must be under project root
        if not real_path.startswith(root + os.sep) and real_path != root:
            self.send_error(403, "Forbidden")
            return os.devnull

        return translated

    def do_GET(self):
        """Handle GET requests with extension allowlisting."""
        # Redirect root to search page
        if self.path == "/" or self.path == "":
            self.send_response(301)
            self.send_header("Location", "/static/search.html")
            self.end_headers()
            return

        # Check file extension against allowlist
        path = self.path.split("?")[0]  # strip query params
        ext = os.path.splitext(path)[1].lower()

        # Allow directory listings only for specific paths, or files with allowed extensions
        if ext and ext not in ALLOWED_EXTENSIONS:
            self.send_error(403, "File type not allowed")
            return

        super().do_GET()

    def guess_type(self, path):
        """Ensure files are served with correct MIME types."""
        mime_type, _ = mimetypes.guess_type(path)

        if path.endswith(".pdf"):
            mime_type = "application/pdf"
        elif path.endswith(".html"):
            mime_type = "text/html"
        elif path.endswith(".json"):
            mime_type = "application/json"

        if mime_type is None:
            mime_type = "application/octet-stream"

        return mime_type

    def log_message(self, format, *args):
        """Custom log format."""
        print(f"[{self.log_date_time_string()}] {format % args}")


def is_port_available(port):
    """Check if a port is available."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((SERVER_HOST, port))
            return True
    except OSError:
        return False


def find_available_port():
    """Find an available port, starting from the preferred port."""
    if is_port_available(PREFERRED_PORT):
        return PREFERRED_PORT

    for port in PORT_RANGE:
        if port == PREFERRED_PORT:
            continue
        if is_port_available(port):
            return port

    return None


def start_server(port):
    """Start the HTTP server on the given port."""
    # Allow socket reuse to avoid "address already in use" on restart
    socketserver.TCPServer.allow_reuse_address = True
    httpd = socketserver.TCPServer((SERVER_HOST, port), SecureHandler)
    return httpd


def run_with_autoreload(port):
    """Run the server with auto-reload when source files change."""
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
    except ImportError:
        print("watchdog not installed - auto-reload disabled")
        print("Install with: pip install watchdog")
        httpd = start_server(port)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            httpd.shutdown()
        return

    class ReloadHandler(FileSystemEventHandler):
        def __init__(self):
            self.restart_pending = False

        def on_modified(self, event):
            if event.is_directory:
                return
            ext = os.path.splitext(event.src_path)[1]
            if ext in WATCH_EXTENSIONS:
                if not self.restart_pending:
                    self.restart_pending = True
                    print(f"\n  File changed: {os.path.basename(event.src_path)}")
                    print("  Restarting server...")
                    # Restart the process
                    os.execv(sys.executable, [sys.executable] + sys.argv)

    observer = Observer()
    handler = ReloadHandler()
    for watch_dir in WATCH_DIRS:
        if os.path.isdir(watch_dir):
            observer.schedule(handler, watch_dir, recursive=True)
    observer.start()

    httpd = start_server(port)
    try:
        print("  Auto-reload: ENABLED (watching src/ and static/)")
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        observer.stop()
        observer.join()
        httpd.shutdown()


def main():
    # Validate that static/search.html exists
    search_html = STATIC_DIR / "search.html"
    if not search_html.exists():
        print(f"Error: {search_html} not found")
        sys.exit(1)

    # Check for JSON search index (in data/ or project root)
    json_in_data = DATA_DIR / "epstein_pdfs_search_index.json"
    json_in_root = PROJECT_ROOT / "epstein_pdfs_search_index.json"
    if not json_in_data.exists() and not json_in_root.exists():
        print("\nWarning: epstein_pdfs_search_index.json not found")
        print("Run the extractor first: python -m src.extractor")
        print("Continuing anyway - search will not work until the JSON is created.\n")

    # Find available port
    port = find_available_port()
    if port is None:
        print(f"Error: No available ports in range {PORT_RANGE.start}-{PORT_RANGE.stop - 1}")
        sys.exit(1)

    url = f"http://{SERVER_HOST}:{port}/static/search.html"

    print("=" * 70)
    print("Epstein DOJ Files - Secure Search Interface")
    print("=" * 70)
    print()
    print(f"  Bound to:    {SERVER_HOST} (localhost only)")
    if port != PREFERRED_PORT:
        print(f"  Port:        {port} (preferred {PREFERRED_PORT} was busy)")
    else:
        print(f"  Port:        {port}")
    print(f"  URL:         {url}")
    print()
    print("  Security:    CORS restricted, CSP enabled, path traversal blocked")
    print()
    print("  Press Ctrl+C to stop the server")
    print("=" * 70)
    print()

    # Open browser
    try:
        webbrowser.open(url)
        print("  Browser opened automatically")
        print()
    except Exception:
        print("  (Could not open browser - please open the URL above manually)")
        print()

    run_with_autoreload(port)
    print("\nServer stopped.")


if __name__ == "__main__":
    main()
