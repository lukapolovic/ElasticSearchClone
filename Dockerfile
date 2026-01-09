FROM python:3.11-slim

# Avoid .pyc files and ensure logs flush
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    NLTK_DATA=/app/nltk_data

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
  && rm -rf /var/lib/apt/lists/*

# Install Python deps first for better layer caching
COPY setup.py /app/setup.py
COPY app /app/app
COPY search /app/search

# Data
RUN mkdir -p /app/scripts/data
COPY scripts/data/25kMovies.cleaned.jsonl /app/scripts/data/25kMovies.cleaned.jsonl

RUN pip install --no-cache-dir -e .

# Bake NLTK data into the image (for stable startup)
# ensure_nltk_data.py downloads into ./nltk_data directory in the repo root
RUN python -c "from search.nltk_setup import ensure_nltk_data; ensure_nltk_data()"

COPY docker/entrypoint.sh /app/docker/entrypoint.sh
RUN chmod +x /app/docker/entrypoint.sh

EXPOSE 8000
ENTRYPOINT ["/app/docker/entrypoint.sh"]