FROM python:3.11-slim

# Avoid .pyc files and ensure logs flush
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

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

RUN pip install --no-cache-dir -e .

# Bake NLTK data into the image (for stable startup)
# ensure_nltk_data.py downloads into ./nltk_data directory in the repo root
RUN python -c "from search.nltk_setup import ensure_nltk_data; ensure_nltk_data()"

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "app.router_main:app", "--host", "0.0.0.0", "--port", "8000"]