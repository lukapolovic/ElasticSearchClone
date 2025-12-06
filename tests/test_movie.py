import pytest
from models.movie import Movie

valid_movie = {
    "id": "1",
    "title": "Inception",
    "year": "2010",
    "genres": "Action, Sci-Fi",
    "description": "A mind-bending thriller.",
    "cast": "Leonardo DiCaprio, Joseph Gordon-Levitt",
    "director": "Christopher Nolan",
    "rating": "8.8"
}

invalid_movie_missing_title = {
    "id": "2",
    "title": "",
    "year": "2010"
}

invalid_movie_bad_rating = {
    "id": "3",
    "title": "Test Movie",
    "year": "2010",
    "rating": "eleven"  # invalid rating
}

# ------------------------------
# Test: from_dict + normalization
# ------------------------------

def test_from_dict_normalization():
    movie = Movie.from_dict(valid_movie)

    assert movie.title == "inception"
    assert movie.year == 2010
    assert movie.genres == ["action", "sci-fi"]
    assert movie.cast == ["leonardo dicaprio", "joseph gordon-levitt"]
    assert movie.rating == 8.8

# ------------------------------
# Test: invalid movie raises Exception
# ------------------------------

def test_invalid_movie_missing_title():
    with pytest.raises(Exception):
        Movie.from_dict(invalid_movie_missing_title)

def test_invalid_movie_bad_rating():
    with pytest.raises(Exception):
        Movie.from_dict(invalid_movie_bad_rating)

# ------------------------------
# Test: to_dict returns correct normalized dict
# ------------------------------

def test_to_dict_output():
    movie = Movie.from_dict(valid_movie)
    movie_dict = movie.to_dict()
    
    assert movie_dict["title"] == "inception"
    assert isinstance(movie_dict["genres"], list)
    assert isinstance(movie_dict["cast"], list)
    assert movie_dict["rating"] == 8.8

# ------------------------------
# normalize_id edge cases
# ------------------------------

def test_normalize_id_zero():
    with pytest.raises(Exception):
        Movie.normalize_id(0)

def test_normalize_id_negative():
    with pytest.raises(Exception):
        Movie.normalize_id(-5)

def test_normalize_id_non_digit_string():
    with pytest.raises(Exception):
        Movie.normalize_id("abc123")

def test_normalize_id_valid_string():
    assert Movie.normalize_id("123") == 123

def test_normalize_id_valid_int():
    assert Movie.normalize_id(456) == 456

# ------------------------------
# normalize_title edge cases
# ------------------------------

def test_normalize_title_empty():
    with pytest.raises(Exception):
        Movie.normalize_title("")

def test_normalize_title_spaces():
    assert Movie.normalize_title("   Hello World   ") == "hello world"

# ------------------------------
# normalize_year edge cases
# ------------------------------

from datetime import datetime
MIN_YEAR = 1888
MAX_YEAR = datetime.now().year + 5

def test_normalize_year_none():
    assert Movie.normalize_year(None) is None

def test_normalize_year_non_digit_string():
    with pytest.raises(Exception):
        Movie.normalize_year("abcd")

def test_normalize_year_out_of_bounds_low():
    with pytest.raises(Exception):
        Movie.normalize_year(str(MIN_YEAR - 1))

def test_normalize_year_out_of_bounds_high():
    with pytest.raises(Exception):
        Movie.normalize_year(str(MAX_YEAR + 1))

def test_normalize_year_valid_string():
    assert Movie.normalize_year("2000") == 2000

def test_normalize_year_valid_int():
    assert Movie.normalize_year(2000) == 2000


# ------------------------------
# normalize_genres edge cases
# ------------------------------

def test_normalize_genres_none():
    assert Movie.normalize_genres(None) == []

def test_normalize_genres_string_with_duplicates():
    genres = "Action,Action,Sci-Fi"
    assert Movie.normalize_genres(genres) == ["action", "sci-fi"]

def test_normalize_genres_list_with_number():
    with pytest.raises(Exception):
        Movie.normalize_genres(["Action", 42])

def test_normalize_genres_invalid_type():
    with pytest.raises(Exception) as e:
        Movie.normalize_genres(123)  # int is invalid
    assert "InvalidDocumentError" in str(e.value)


# ------------------------------
# normalize_cast edge cases
# ------------------------------

def test_normalize_cast_none():
    assert Movie.normalize_cast(None) == []

def test_normalize_cast_list_with_number():
    with pytest.raises(Exception):
        Movie.normalize_cast(["Actor1", 42])

def test_normalize_cast_string():
    cast = "Leonardo DiCaprio,Joseph Gordon-Levitt"
    assert Movie.normalize_cast(cast) == ["leonardo dicaprio", "joseph gordon-levitt"]

def test_normalize_cast_invalid_type():
    with pytest.raises(Exception) as e:
        Movie.normalize_cast(456)  # int is invalid
    assert "InvalidDocumentError" in str(e.value)

# ------------------------------
# normalize_rating edge cases
# ------------------------------

def test_normalize_rating_non_numeric_string():
    with pytest.raises(Exception):
        Movie.normalize_rating("eleven")

def test_normalize_rating_negative():
    with pytest.raises(Exception):
        Movie.normalize_rating(-1)

def test_normalize_rating_too_high():
    with pytest.raises(Exception):
        Movie.normalize_rating(11)

def test_normalize_rating_string_to_float():
    assert Movie.normalize_rating("8.5") == 8.5

def test_normalize_rating_none():
    assert Movie.normalize_rating(None) is None

def test_normalize_rating_invalid_type():
    with pytest.raises(Exception) as e:
        Movie.normalize_rating({"value": 8})  # dict is invalid
    assert "InvalidDocumentError" in str(e.value)