import pytest
from search.query import QueryEngine, FIELD_WEIGHTS
from search.indexer import Indexer


# Fake tokenizer: simple lowercase split
def fake_tokenize(text):
    return text.lower().split()


@pytest.fixture
def indexer(monkeypatch):
    monkeypatch.setattr("search.query.tokenize", fake_tokenize)
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    idx = Indexer()

    documents = [
        {
            "id": "1",
            "title": "Mission Impossible",
            "genres": ["Action"],
            "cast": ["Tom Cruise"],
            "director": "John Woo",
        },
        {
            "id": "2",
            "title": "Top Gun",
            "genres": ["Action"],
            "cast": ["Tom Cruise"],
            "director": "Tony Scott",
        },
        {
            "id": "3",
            "title": "The Matrix",
            "genres": ["Sci-Fi"],
            "cast": ["Keanu Reeves"],
            "director": "Wachowski",
        },
    ]

    idx.build(documents, fields=["title", "genres", "cast", "director"])
    return idx


@pytest.fixture
def engine(indexer):
    return QueryEngine(indexer)


# ------------------------------------------------------
# BASIC CASES
# ------------------------------------------------------

def test_empty_query_returns_empty(engine):
    assert engine.search("") == []


def test_punctuation_only(engine, monkeypatch):
    monkeypatch.setattr("search.query.tokenize", lambda x: [])
    assert engine.search("!!!") == []


def test_single_token_match(engine):
    # "mission" appears only in doc 1
    assert engine.search("mission") == ["1"]


def test_no_matches(engine):
    assert engine.search("xyz123") == []


def test_case_insensitivity(engine):
    assert engine.search("MiSsIoN") == ["1"]


# ------------------------------------------------------
# OR LOGIC (NOT AND!)
# ------------------------------------------------------

def test_or_logic_multiple_docs(engine):
    # "tom" matches doc1 and doc2
    result = engine.search("tom")
    assert result == ["1", "2"] or result == ["2", "1"]


def test_or_logic_two_tokens_accumulate(engine):
    # "tom" (docs 1,2) + "action" (docs 1,2) → scores should increase
    result = engine.search("tom action")

    # both docs 1 and 2 should appear, order depends on weight sum
    assert set(result) == {"1", "2"}


# ------------------------------------------------------
# WEIGHTING & RANKING
# ------------------------------------------------------

def test_field_weighting(engine):
    # Searching "mission tom" should give doc1 highest score
    result = engine.search("mission tom")

    # Doc1 contains:
    #   "mission" → title → +5.0
    #   "tom" → cast → +4.0
    # Total: 9.0
    #
    # Doc2 only gets "tom" → +4.0
    assert result[0] == "1"


def test_field_weighting_multiple_fields(engine):
    # "action" appears in genres → weight 3.0
    result = engine.search("action")

    # doc1 & doc2 both match → tied scores → stable ordering guaranteed?
    assert set(result) == {"1", "2"}


def test_unknown_field_weight(engine, indexer):
    # Inject a posting with an unknown field name
    indexer.index.setdefault("weirdtoken", {})["99"] = {"unknownfield"}

    result = engine.search("weirdtoken")

    # weight should be 0 so doc "99" appears with score 0
    assert result == ["99"]


# ------------------------------------------------------
# TOKENIZER EDGE-CASE BEHAVIOR
# ------------------------------------------------------

def test_tokenizer_returns_no_tokens(engine, monkeypatch):
    monkeypatch.setattr("search.query.tokenize", lambda x: [])
    assert engine.search("something") == []


# ------------------------------------------------------
# STABLE SORT / SCORE ORDERING
# ------------------------------------------------------

def test_sorting_descending(engine):
    # doc1 will have higher weight for "mission"
    result = engine.search("mission tom")

    # doc1 must be first due to weight difference
    assert result[0] == "1"


# ------------------------------------------------------
# MULTI-TOKEN SCORE ACCUMULATION
# ------------------------------------------------------

def test_tokens_add_scores_together(engine):
    # mission → title (5)
    # john → director (3)
    # total = 8.0 for doc1
    result = engine.search("mission john")

    assert result[0] == "1"