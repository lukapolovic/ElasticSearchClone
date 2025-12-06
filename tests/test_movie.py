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