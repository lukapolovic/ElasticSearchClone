# ElasticSearch Clone - AI Coding Agent Instructions

## Architecture Overview

This project implements a full-text search engine inspired by Elasticsearch, consisting of three layers:

1. **Search Layer** (`search/`): Core indexing & querying logic
   - `Indexer`: Builds inverted index with TF-IDF, tracks document frequency
   - `QueryEngine`: Processes queries with synonym expansion, fuzzy matching, field-weighted scoring
   - `Tokenizer`: Normalizes text (lowercases, removes punctuation)

2. **Application Layer** (`app/core/search_service.py`): Orchestration service
   - Loads movie data from JSON on startup via FastAPI lifespan
   - Delegates to `Indexer` for indexing and `QueryEngine` for searching
   - Applies pagination and returns structured `SearchResponse`

3. **API Layer** (`app/api/routes/`): FastAPI routes
   - `GET /search/`: Query endpoint with pagination and debug mode
   - `GET /search/health`: Index health status
   - Error handling via custom `SearchError` exceptions mapped to HTTP 400

## Key Data Flows

### Index Building (FastAPI lifespan)
- `app/main.py` → `SearchService.load_data()` → loads `app/data/movies.json`
- Extracts 7 fields (title, year, genres, description, cast, director, rating)
- `Indexer.build(documents, fields)` tokenizes each field and builds inverted index
- Tracks `doc_freq` (document frequency) per token for IDF calculation

### Search Processing
- Query → tokenize → synonym expansion (WordNet) → lookup in index
- For each expanded token: exact match OR fuzzy match (rapidfuzz) if not in index
- Score = Σ(field_weight × term_frequency × IDF × similarity)
- **Field weights** in `search/query.py`: title=5.0, cast=4.0, director=3.0, genres=3.0, description=1.0, year=0.5, rating=0.1
- Results paginated; debug mode includes scoring explanations

## Critical Patterns & Conventions

### Inverted Index Structure
```python
# Indexer.index: { token → { doc_id → {"fields": set, "tf": int} } }
# Enables fast token lookup and per-field tracking
```

### Error Handling
- All search domain errors inherit from `SearchError` (code, message, details)
- Specific exceptions: `IndexNotReadyError`, `InvalidQueryError`, `InvalidPageError`
- API routes return `APIResponse[T]` wrapper with status, data, error, meta fields
- Custom handler maps `SearchError` to HTTP 400 with structured error JSON

### Pagination
- Input: page (1-based, ≥1), page_size (1-50, default=10)
- Calculation: `start = (page - 1) * page_size; results = raw_results[start:end]`
- Response includes `total_hits` (before pagination) for client-side pagination info

## Development Workflows

### Running Tests
```bash
pytest tests/
```
Mocked tokenizer via monkeypatch for predictable behavior. Tests verify:
- Inverted index building and lookup
- Synonym expansion and fuzzy matching
- Document ingestion and field handling

### Running the API Server
```bash
uvicorn app.main:app --reload
```
Data loads on startup. Test at: `http://localhost:8000/docs` (Swagger)

### Key Files to Modify By Purpose
- **Add new search features**: `search/query.py` (QueryEngine.search method)
- **Change scoring**: `search/query.py` (FIELD_WEIGHTS, idf calculation)
- **Modify API response**: `app/models/search_response.py`, `app/models/api_response.py`
- **Add fields to index**: Update field list in `SearchService.load_data()` and models
- **Tune tokenization**: `search/tokenizer.py`

## Dependencies & Integration Points

- **FastAPI**: REST framework, lifespan for data loading, dependency injection (SearchQuery)
- **NLTK**: WordNet for synonym expansion (wordnet.synsets, lemma.name)
- **RapidFuzz**: Fuzzy string matching for typo tolerance (fuzz.ratio)
- **Pydantic**: Request/response validation (SearchQuery, SearchResponse, APIResponse)
- **Python 3.11+**: Type hints, asyncio support

## Important Gotchas

1. **Token similarity threshold**: hardcoded at 80 in `QueryEngine.get_closest_token()`. Adjust if fuzzy matching too strict/loose.
2. **Synonym expansion limit**: hardcoded at 5 lemmas per synset. Prevents memory bloat but may miss semantics.
3. **Field weights asymmetry**: title weighted 50x higher than rating. Reflects movie search priorities but may skew numeric fields.
4. **Index state**: Indexer not thread-safe; assume single-threaded startup. No incremental index updates after startup.
