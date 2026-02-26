FROM python:3.11-slim

WORKDIR /app

# Install curl for downloading data from GCS during build
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements-server.txt .
RUN pip install --no-cache-dir -r requirements-server.txt

# Copy source code and static files
COPY src/__init__.py src/server.py src/search.py src/config.py \
     src/build_index.py src/logging_setup.py src/init_db.py src/
COPY static/ static/

# Create directories and download data from GCS at BUILD time
# (read-only image layers don't count against container memory)
RUN mkdir -p data/logs data/thumbnails data/classifications && \
    echo "Downloading search index from GCS..." && \
    curl -fsSL -o data/epstein_search.db \
        "https://storage.googleapis.com/epstein-doj-files-jarheads/data/epstein_search.db" && \
    echo "Downloading classifications from GCS..." && \
    for i in 1 2 3 4 5 6 7 8 10 11 12; do \
        curl -fsSL -o "data/classifications/data-set-${i}.json" \
            "https://storage.googleapis.com/epstein-doj-files-jarheads/data/classifications/data-set-${i}.json"; \
    done && \
    echo "Data download complete"

# Cloud Run sets PORT env var
ENV PORT=8080

CMD ["gunicorn", "src.server:app", \
     "-k", "uvicorn.workers.UvicornWorker", \
     "--workers", "1", \
     "--bind", "0.0.0.0:8080", \
     "--timeout", "120"]
