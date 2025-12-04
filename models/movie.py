import datetime

class Movie:
    """
    A Movie instance always has a non-empty title and normalized lists for "genres" and "cast" 
    """

    MIN_YEAR = 1888
    MAX_YEAR = datetime.now().year + 5
    RATING_MIN = 0.0
    RATING_MAX = 10.0
    DEFAULT_GENRES = []
    DEFAULT_CAST = []

    def __init__(self, id, title, year, genres, description, cast, director, rating):
        self.id = id
        self.title = title
        self.year = year
        self.genres = genres
        self.description = description
        self.cast = cast
        self.director = director
        self.rating = rating
    


        
