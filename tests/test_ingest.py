import logging
import pytest
import json
import tempfile
import os
from ingestion.ingest import load_json_file, ingest_one, ingest_many, save_jsonl
from models.movie import Movie

# -------------------------------
# Sample movie dicts
# -------------------------------
valid_movie = {
    "id": "1",
    "title": "Inception",
    "year": "2010",
    "genres": "Action,Sci-Fi",
    "description": "A thief who steals corporate secrets.",
    "cast": "Leonardo DiCaprio,Joseph Gordon-Levitt",
    "director": "Christopher Nolan",
    "rating": "8.8"
}

invalid_movie_missing_title = {
    "id": "2",
    "year": "2020",
    "genres": "Drama",
    "description": "Missing title",
    "cast": "Actor Name",
    "director": "Director Name",
    "rating": "7.5"
}

invalid_movie_wrong_type = "This is not a dict"

# -------------------------------
# load_json_file tests
# -------------------------------

def test_load_json_file_array():
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
        json.dump([valid_movie, valid_movie], f)
        path = f.name

    try:
        data = list(load_json_file(path))
        assert all(isinstance(d, dict) for d in data)
        assert len(data) == 2
    finally:
        os.remove(path)

def test_load_json_file_jsonl():
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
        for _ in range(2):
            f.write(json.dumps(valid_movie) + "\n")
        path = f.name

    try:
        data = list(load_json_file(path))
        assert all(isinstance(d, dict) for d in data)
        assert len(data) == 2
    finally:
        os.remove(path)

def test_load_json_file_invalid_json(caplog):
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
        f.write('{"id": "1", "title": "Good"}\n')
        f.write('INVALID_JSON\n')
        path = f.name

    try:
        data = list(load_json_file(path))
        assert len(data) == 1
        assert "Skipping invalid JSON line" in caplog.text
    finally:
        os.remove(path)

def test_load_json_file_empty_lines():
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as f:
        f.write("\n")
        f.write(json.dumps(valid_movie) + "\n")
        f.write("\n")
        path = f.name

    try:
        data = list(load_json_file(path))
        assert len(data) == 1
    finally:
        os.remove(path)

# -------------------------------
# ingest_one tests
# -------------------------------

def test_ingest_one_valid():
    result = ingest_one(valid_movie)
    assert result["title"] == "inception" # type: ignore
    assert result["genres"] == ["action", "sci-fi"] # type: ignore

def test_ingest_one_invalid_type(caplog):
    result = ingest_one(invalid_movie_wrong_type) # type: ignore
    assert result is None
    assert "This is not a dict" in caplog.text

def test_ingest_one_missing_required_field(caplog):
    result = ingest_one(invalid_movie_missing_title)
    assert result is None
    assert "Skipping doc id=2" in caplog.text

# -------------------------------
# ingest_many tests
# -------------------------------

def test_ingest_many_all_valid():
    results = ingest_many([valid_movie, valid_movie])
    assert len(results) == 2

def test_ingest_many_some_invalid_continue(caplog):
    results = ingest_many([valid_movie, invalid_movie_missing_title, valid_movie], continue_on_error=True)
    assert len(results) == 2
    assert "Skipping doc id=2" in caplog.text

def test_ingest_many_some_invalid_stop():
    with pytest.raises(Exception):
        ingest_many([valid_movie, invalid_movie_missing_title, valid_movie], continue_on_error=False)

# -------------------------------
# save_jsonl tests
# -------------------------------

def test_save_jsonl_writes_file():
    with tempfile.NamedTemporaryFile(mode="r+", delete=False) as f:
        path = f.name
    try:
        save_jsonl([valid_movie, valid_movie], path)
        with open(path, "r", encoding="utf-8") as f_read:
            lines = f_read.readlines()
            assert len(lines) == 2
            for line in lines:
                data = json.loads(line)
                assert data["title"] == valid_movie["title"]
    finally:
        os.remove(path)

def test_save_jsonl_skips_non_dict(caplog):
    with tempfile.NamedTemporaryFile(mode="r+", delete=False) as f:
        path = f.name
    try:
        save_jsonl([valid_movie, "not a dict"], path) # type: ignore
        with open(path, "r", encoding="utf-8") as f_read:
            lines = f_read.readlines()
            assert len(lines) == 1
        assert "Skipping non-dict object in output" in caplog.text
    finally:
        os.remove(path)

def test_save_jsonl_append_mode():
    with tempfile.NamedTemporaryFile(mode="r+", delete=False) as f:
        path = f.name
    try:
        save_jsonl([valid_movie], path)
        save_jsonl([valid_movie], path, append=True)
        with open(path, "r", encoding="utf-8") as f_read:
            lines = f_read.readlines()
            assert len(lines) == 2
    finally:
        os.remove(path)
