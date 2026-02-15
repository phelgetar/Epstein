#!/usr/bin/env python3
"""
FastAPI server for the Epstein DOJ Files search interface.

Features:
- Server-side search API (/api/search) — browser no longer loads full JSON
- Security middleware: CORS, CSP, X-Frame-Options, etc.
- Static file serving for HTML, PDFs, and data files
- Auto-reload via uvicorn --reload
"""

import json
import os
import socket
import sys
import webbrowser
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
load_dotenv()

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

sentry_sdk.init(
    dsn=os.environ.get("SENTRY_DSN", ""),
    environment=os.environ.get("SENTRY_ENVIRONMENT", "development"),
    release=os.environ.get("SENTRY_RELEASE"),
    traces_sample_rate=float(os.environ.get("SENTRY_TRACES_SAMPLE_RATE", "0.2")),
    send_default_pii=True,
    integrations=[
        FastApiIntegration(),
        StarletteIntegration(),
    ],
)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import (
    PROJECT_ROOT, STATIC_DIR, DATA_DIR, PDF_DIR, THUMB_DIR, CLASSIFY_DIR,
    SERVER_HOST, PREFERRED_PORT, PORT_RANGE,
    JSON_SEARCH_INDEX, JSON_FULL,
)
from src.search import PDFSearcher, _parse_and_search


# ─── Global State ────────────────────────────────────────────

searcher: Optional[PDFSearcher] = None
doc_stats: dict = {"total_docs": 0, "total_pages": 0}
_classifications: dict = {}  # dataset_num -> {"metadata": {...}, "pages": {...}}
_classifications_mtime: dict = {}  # dataset_num -> last mtime loaded


# ─── Classification Loader ────────────────────────────────────

def _load_classifications(ds: int) -> None:
    """Load classification JSON for a dataset if the file is new or changed."""
    cls_file = CLASSIFY_DIR / f"data-set-{ds}.json"
    if not cls_file.exists():
        return
    try:
        mtime = cls_file.stat().st_mtime
        if ds in _classifications_mtime and _classifications_mtime[ds] == mtime:
            return  # file unchanged
        with open(cls_file, "r") as f:
            _classifications[ds] = json.load(f)
        _classifications_mtime[ds] = mtime
        # Invalidate stats cache for this dataset and the "all" aggregate
        _classification_stats_cache.pop(ds, None)
        _classification_stats_cache.pop(None, None)
    except Exception:
        pass


# ─── Lifespan ────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Load search index and classification data into memory on startup."""
    global searcher, doc_stats

    # Find search index
    candidates = [
        DATA_DIR / JSON_SEARCH_INDEX,
        DATA_DIR / JSON_FULL,
        PROJECT_ROOT / JSON_SEARCH_INDEX,
        PROJECT_ROOT / JSON_FULL,
    ]
    json_file = None
    for candidate in candidates:
        if candidate.exists():
            json_file = candidate
            break

    if json_file:
        searcher = PDFSearcher(str(json_file))
        doc_stats["total_docs"] = len(searcher.data)
        doc_stats["total_pages"] = sum(doc.get("pages", 0) for doc in searcher.data)
    else:
        print("\nWarning: No search index found.")
        print("Run the extractor first: python -m src.extractor\n")

    # Initial classification load
    for ds in range(1, 13):
        _load_classifications(ds)

    cls_count = sum(
        len(d.get("pages", {})) for d in _classifications.values()
    )
    if cls_count:
        print(f"  Classifications loaded: {cls_count:,} images across "
              f"{len(_classifications)} datasets")

    yield


# ─── App ─────────────────────────────────────────────────────

app = FastAPI(title="Epstein DOJ Files", lifespan=lifespan)


# ─── Security Middleware ─────────────────────────────────────

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        path = request.url.path.lower()

        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["X-XSS-Protection"] = "1; mode=block"
        response.headers["Referrer-Policy"] = "no-referrer"

        if path.endswith(".pdf") and response.status_code == 200:
            # PDFs need caching for iOS Safari to render them fully
            # (Safari uses range requests and caches chunks locally)
            response.headers["Cache-Control"] = "public, max-age=86400"
        elif path.endswith((".jpg", ".jpeg", ".png")) and response.status_code == 200:
            response.headers["Cache-Control"] = "public, max-age=604800"
        else:
            response.headers["Content-Security-Policy"] = (
                "default-src 'self'; "
                "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
                "font-src 'self' https://fonts.gstatic.com; "
                "script-src 'self' 'unsafe-inline'; "
                "img-src 'self' data:; "
                "frame-src 'self'; "
                "object-src 'self';"
            )
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"

        return response


app.add_middleware(SecurityHeadersMiddleware)

# CORS — allow localhost and LAN origins
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"^http://(localhost|127\.0\.0\.1|192\.168\.\d+\.\d+|10\.\d+\.\d+\.\d+|172\.\d+\.\d+\.\d+)(:\d+)?$",
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ─── API Routes ──────────────────────────────────────────────

@app.get("/")
async def root():
    return RedirectResponse(url="/static/search.html")


@app.get("/api/stats")
async def stats():
    return doc_stats


@app.get("/api/search")
async def search_api(
    q: str = Query(..., min_length=1, description="Search query"),
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=10000),
    dataset: Optional[int] = Query(None, ge=1, le=12),
    min_pages: int = Query(0, ge=0),
    max_pages: Optional[int] = Query(None, ge=0),
    sort: str = Query("relevance", pattern="^(relevance|filename|dataset)$"),
    case_sensitive: bool = Query(False),
    whole_word: bool = Query(False),
    use_regex: bool = Query(False),
):
    if searcher is None:
        return JSONResponse(
            status_code=503,
            content={"error": "Search index not loaded. Run: python -m src.extractor"},
        )

    # Run the search
    results = _parse_and_search(searcher, q)

    # Apply filters
    if dataset is not None:
        results = [r for r in results if r["dataset"] == dataset]
    results = [r for r in results if r["pages"] >= min_pages]
    if max_pages is not None:
        results = [r for r in results if r["pages"] <= max_pages]

    # Sort
    if sort == "relevance":
        results.sort(key=lambda r: r["match_count"], reverse=True)
    elif sort == "filename":
        results.sort(key=lambda r: r["filename"])
    elif sort == "dataset":
        results.sort(key=lambda r: (r["dataset"], r["filename"]))

    # Totals before pagination
    total = len(results)
    total_matches = sum(r["match_count"] for r in results)

    # Paginate
    start = (page - 1) * per_page
    page_results = results[start:start + per_page]

    # Build response — strip the heavy 'text' field from doc spread
    response_results = []
    for r in page_results:
        response_results.append({
            "dataset": r["dataset"],
            "filename": r["filename"],
            "filepath": r["filepath"],
            "pages": r["pages"],
            "match_count": r["match_count"],
            "contexts": r["contexts"][:50],
        })

    return {
        "results": response_results,
        "total": total,
        "totalMatches": total_matches,
        "page": page,
        "perPage": per_page,
    }


_gallery_cache: dict = {}  # dataset -> sorted list of filenames


def _get_dataset_images(ds: int, content_type: str = None,
                        tags: list[str] = None, person: str = None) -> list[tuple[int, str]]:
    """Return list of (dataset_num, filename) for a dataset, optionally filtered.
    tags: list of tags — image must match ALL of them (AND logic).
    """
    _load_classifications(ds)
    thumb_dir = THUMB_DIR / f"data-set-{ds}"
    if not thumb_dir.exists():
        return []

    if ds not in _gallery_cache:
        _gallery_cache[ds] = sorted(f.name for f in thumb_dir.glob("*.jpg"))

    all_names = _gallery_cache[ds]
    cls_data = _classifications.get(ds, {}).get("pages", {})

    if content_type or tags or person:
        filtered = []
        for name in all_names:
            cls = cls_data.get(name)
            if not cls:
                continue
            if content_type and cls.get("content_type") != content_type:
                continue
            if tags:
                img_tags = [t.lower() for t in cls.get("tags", [])]
                if not all(t.lower() in img_tags for t in tags):
                    continue
            if person:
                people = [p.lower() for p in cls.get("people", [])]
                if person.lower() not in people:
                    continue
            filtered.append(name)
        all_names = filtered

    return [(ds, name) for name in all_names]


@app.get("/api/gallery")
async def gallery_api(
    dataset: Optional[int] = Query(None, ge=1, le=12),
    page: int = Query(1, ge=1),
    per_page: int = Query(100, ge=1, le=10000),
    content_type: Optional[str] = Query(None),
    tag: Optional[str] = Query(None),
    person: Optional[str] = Query(None),
):
    """List thumbnail images, paginated, with optional filters. dataset=None means all.
    tag: comma-separated list of tags — image must match ALL (AND logic).
    """
    datasets_to_query = [dataset] if dataset is not None else list(range(1, 13))
    tags = [t.strip() for t in tag.split(",") if t.strip()] if tag else None

    all_items = []
    for ds in datasets_to_query:
        all_items.extend(_get_dataset_images(ds, content_type, tags, person))

    total = len(all_items)
    start = (page - 1) * per_page
    page_items = all_items[start:start + per_page]

    images_response = []
    for ds_num, name in page_items:
        entry = {
            "src": f"/thumbnails/data-set-{ds_num}/{name}",
            "dataset": ds_num,
        }
        cls_data = _classifications.get(ds_num, {}).get("pages", {})
        cls = cls_data.get(name)
        if cls:
            entry["classification"] = cls
        images_response.append(entry)

    return {
        "images": images_response,
        "total": total,
        "page": page,
        "perPage": per_page,
    }


_classification_stats_cache: dict = {}  # key: dataset int or None (all)

@app.get("/api/classifications/stats")
async def classification_stats(dataset: Optional[int] = Query(None, ge=1, le=12)):
    """Return available content_types, top tags, and top people for dataset(s)."""
    datasets_to_query = [dataset] if dataset is not None else list(range(1, 13))
    for ds in datasets_to_query:
        _load_classifications(ds)

    cache_key = dataset  # None for "all"
    if cache_key in _classification_stats_cache:
        return _classification_stats_cache[cache_key]

    content_types = {}
    tag_counts = {}
    person_counts = {}
    total_classified = 0

    for ds in datasets_to_query:
        cls_data = _classifications.get(ds, {}).get("pages", {})
        for cls in cls_data.values():
            total_classified += 1
            ct = cls.get("content_type", "other")
            content_types[ct] = content_types.get(ct, 0) + 1
            for t in cls.get("tags", []):
                t_lower = t.lower()
                tag_counts[t_lower] = tag_counts.get(t_lower, 0) + 1
            for p in cls.get("people", []):
                p_lower = p.lower()
                person_counts[p_lower] = person_counts.get(p_lower, 0) + 1

    if not total_classified:
        return {"content_types": [], "top_tags": [], "top_people": [], "classified_count": 0}

    result = {
        "content_types": sorted(content_types.keys()),
        "content_type_counts": content_types,
        "top_tags": [t for t, _ in sorted(tag_counts.items(), key=lambda x: -x[1])[:50]],
        "top_people": [p for p, _ in sorted(person_counts.items(), key=lambda x: -x[1])],
        "people_counts": person_counts,
        "classified_count": total_classified,
    }
    _classification_stats_cache[cache_key] = result
    return result


# ─── Static File Mounts ─────────────────────────────────────
# Order matters — mount after API routes so /api/* takes precedence

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

if DATA_DIR.exists():
    app.mount("/data", StaticFiles(directory=str(DATA_DIR)), name="data")

if PDF_DIR.exists():
    app.mount("/epstein_doj_files", StaticFiles(directory=str(PDF_DIR)), name="pdfs")

THUMB_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/thumbnails", StaticFiles(directory=str(THUMB_DIR)), name="thumbnails")


# ─── Main ────────────────────────────────────────────────────

def is_port_available(port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((SERVER_HOST, port))
            return True
    except OSError:
        return False


def find_available_port():
    if is_port_available(PREFERRED_PORT):
        return PREFERRED_PORT
    for port in PORT_RANGE:
        if port == PREFERRED_PORT:
            continue
        if is_port_available(port):
            return port
    return None


def main():
    import uvicorn

    port = find_available_port()
    if port is None:
        print(f"Error: No available ports in range {PORT_RANGE.start}-{PORT_RANGE.stop - 1}")
        sys.exit(1)

    url = f"http://127.0.0.1:{port}/static/search.html"

    # Get LAN IP
    lan_ip = None
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        lan_ip = s.getsockname()[0]
        s.close()
    except Exception:
        pass

    print("=" * 70)
    print("Epstein DOJ Files - FastAPI Search Interface")
    print("=" * 70)
    print()
    print(f"  Bound to:    {SERVER_HOST} (all interfaces)")
    if port != PREFERRED_PORT:
        print(f"  Port:        {port} (preferred {PREFERRED_PORT} was busy)")
    else:
        print(f"  Port:        {port}")
    print(f"  Local:       {url}")
    if lan_ip:
        print(f"  Network:     http://{lan_ip}:{port}/static/search.html")
    print()
    print(f"  API docs:    http://127.0.0.1:{port}/docs")
    print("  Security:    CORS (LAN), CSP enabled")
    print("  Auto-reload: ENABLED (watching src/ and static/)")
    print()
    print("  Press Ctrl+C to stop the server")
    print("=" * 70)
    print()

    # Open browser
    try:
        webbrowser.open(url)
        print("  Browser opened automatically\n")
    except Exception:
        print("  (Could not open browser — open the URL above manually)\n")

    uvicorn.run(
        "src.server:app",
        host=SERVER_HOST,
        port=port,
        reload=True,
        reload_dirs=[str(PROJECT_ROOT / "src"), str(PROJECT_ROOT / "static")],
        log_level="info",
    )


if __name__ == "__main__":
    main()
