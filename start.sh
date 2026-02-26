#!/bin/bash
set -e

GCS="https://storage.googleapis.com/epstein-doj-files-jarheads"

# Download SQLite DB from GCS (public bucket, use curl instead of gcloud CLI)
echo "Downloading search index from GCS..."
curl -fsSL -o data/epstein_search.db "$GCS/data/epstein_search.db"
echo "Search index downloaded ($(du -h data/epstein_search.db | cut -f1))"

# Download classification files
echo "Downloading classifications from GCS..."
for i in 1 2 3 4 5 6 7 8 10 11 12; do
    curl -fsSL -o "data/classifications/data-set-${i}.json" \
        "$GCS/data/classifications/data-set-${i}.json" &
done
wait
echo "Classifications downloaded"

echo "Starting server on port $PORT..."
exec gunicorn src.server:app \
    -k uvicorn.workers.UvicornWorker \
    --workers 2 \
    --bind 0.0.0.0:$PORT \
    --timeout 120
