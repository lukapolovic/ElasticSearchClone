import pytest
from search.indexer import Indexer

# ---- Helper stub tokenizer ----
# We mock the tokenizer so tests don't depend on your tokenizer implementation.
# Pytest monkeypatch will overwrite the tokenize() function during tests.
def fake_tokenize(text):
    return text.lower().split()


def test_add_document_new_tokens(monkeypatch):
    # Patch tokenizer.tokenize
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    indexer = Indexer()
    indexer.add_document(1, "Hello world")

    assert "hello" in indexer.index
    assert "world" in indexer.index
    assert indexer.index["hello"] == {1}
    assert indexer.index["world"] == {1}


def test_add_document_existing_token(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    indexer = Indexer()
    indexer.add_document(1, "hello")
    indexer.add_document(2, "hello")

    assert indexer.index["hello"] == {1, 2}


def test_build_index(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    documents = [
        {"id": 1, "title": "Distributed systems", "body": "are fun"},
        {"id": 2, "title": "Distributed search", "body": "is powerful"},
    ]

    indexer = Indexer()
    indexer.build(documents, fields=["title", "body"])

    # Token presence
    assert "distributed" in indexer.index
    assert indexer.index["distributed"] == {1, 2}

    assert "systems" in indexer.index
    assert indexer.index["systems"] == {1}

    assert "powerful" in indexer.index
    assert indexer.index["powerful"] == {2}


def test_lookup_existing(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    indexer = Indexer()
    indexer.add_document(1, "hello world")

    result = indexer.lookup("hello")
    assert result == {1}

    # ensure it's a copy, not the real set
    result.add(999)
    assert indexer.index["hello"] == {1}


def test_lookup_missing(monkeypatch):
    monkeypatch.setattr("search.indexer.tokenize", fake_tokenize)

    indexer = Indexer()
    assert indexer.lookup("missing") == set()
