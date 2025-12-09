import pytest
from search.query import QueryEngine
from search.indexer import Indexer

@pytest.fixture
def sample_indexer():
    idx = Indexer()
    documents = [
        {"id": "1", "title": "Inception", "body": "Dreams within dreams"},
        {"id": "2", "title": "The Matrix", "body": "Virtual reality"},
        {"id": "3", "title": "Interstellar", "body": "Space exploration and dreams"},
    ]
    # Build index using both fields
    idx.build(documents, ["title", "body"])
    return idx

@pytest.fixture
def engine(sample_indexer):
    return QueryEngine(sample_indexer)

# 1. Empty query string
def test_empty_query(engine):
    assert engine.search("") == set()

# 2. Query string tokenizes to empty (only punctuation)
def test_only_punctuation(engine):
    assert engine.search("!!!") == set()

# 3. Single token, present in index
def test_single_token_present(engine):
    assert engine.search("inception") == {"1"}

# 4. Single token, not present
def test_single_token_missing(engine):
    assert engine.search("nonexistent") == set()

# 5. Multiple tokens, all present
def test_multiple_tokens_full_overlap(engine):
    result = engine.search("dreams space")
    # Only document 3 has both "dreams" and "space"
    assert result == {"3"}

# 6. Multiple tokens, some missing
def test_multiple_tokens_some_missing(engine):
    result = engine.search("dreams nonexistent")
    assert result == set()

# 7. Multiple tokens, partial overlap
def test_multiple_tokens_partial_overlap(engine):
    result = engine.search("dreams matrix")
    # No single document has both tokens, should return empty
    assert result == set()

# 8. Case-insensitivity
def test_case_insensitivity(engine):
    result1 = engine.search("Inception")
    result2 = engine.search("inception")
    assert result1 == result2 == {"1"}
