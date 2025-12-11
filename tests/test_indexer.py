import pytest
from search.indexer import Indexer

# Fake tokenizer for predictable behavior.
def fake_tokenize(text):
    return text.lower().split()


# --------------------------
# add_document() Tests
# --------------------------

def test_add_document_single_field(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    idx = Indexer()
    idx.add_document(1, "title", "Hello world")

    assert "hello" in idx.index
    assert "world" in idx.index

    assert idx.index["hello"] == {1: {"title"}}
    assert idx.index["world"] == {1: {"title"}}


def test_add_document_multiple_fields_same_doc(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    idx = Indexer()
    idx.add_document(1, "title", "hello")
    idx.add_document(1, "description", "hello again")

    assert idx.index["hello"] == {
        1: {"title", "description"}
    }
    assert idx.index["again"] == {
        1: {"description"}
    }


def test_add_document_multiple_docs(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    idx = Indexer()
    idx.add_document(1, "body", "hello")
    idx.add_document(2, "title", "hello world")

    assert idx.index["hello"] == {
        1: {"body"},
        2: {"title"},
    }
    assert idx.index["world"] == {
        2: {"title"},
    }


# --------------------------
# build() Tests
# --------------------------

def test_build_index_fields(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    docs = [
        {"id": 1, "title": "Fast car", "desc": "Red car"},
        {"id": 2, "title": "Blue car", "desc": "Fast bike"},
    ]

    idx = Indexer()
    idx.build(docs, ["title", "desc"])

    # Token → doc_id → fields
    assert idx.index["fast"] == {
        1: {"title"},
        2: {"desc"}
    }

    assert idx.index["car"] == {
        1: {"title", "desc"},
        2: {"title"},
    }


# --------------------------
# lookup Tests
# --------------------------

def test_lookup_existing(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    idx = Indexer()
    idx.add_document(1, "title", "hello world")

    result = idx.lookup("hello")

    assert result == {1: {"title"}}

    # Must be a copy — modifying result must not affect the index
    result[1].add("FAKE_FIELD")
    assert idx.index["hello"] == {1: {"title"}}


def test_lookup_missing(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    idx = Indexer()
    assert idx.lookup("missing") == {}

def test_build_with_list_fields(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    documents = [
        {
            "id": 1,
            "title": "Mission Impossible",
            "genres": ["Action", "Thriller"],
            "cast": ["Tom Cruise", "Simon Pegg"]
        }
    ]

    idx = Indexer()
    idx.build(documents, fields=["title", "genres", "cast"])

    # Genres list becomes "Action Thriller"
    assert "action" in idx.index
    assert "thriller" in idx.index

    # Cast list becomes "Tom Cruise Simon Pegg"
    assert "tom" in idx.index
    assert "cruise" in idx.index
    assert "simon" in idx.index
    assert "pegg" in idx.index

    # Also check field associations
    assert idx.index["cruise"] == {1: {"cast"}}
    assert idx.index["mission"] == {1: {"title"}}
