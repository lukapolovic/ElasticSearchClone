import datetime
import math

MIN_YEAR = 1888
MAX_YEAR = datetime.datetime.now().year + 5
RATING_MIN = 0.0
RATING_MAX = 10.0
DEFAULT_GENRES = []
DEFAULT_CAST = []

class Movie:
    """
    A Movie instance always has a non-empty title and normalized lists for "genres" and "cast" 
    """
    def __init__(self, id, title, year, genres, description, cast, director, rating):
        self.id = Movie.normalize_id(id)
        self.title = Movie.normalize_title(title)
        self.year = Movie.normalize_year(year)
        self.genres = Movie.normalize_genres(genres)
        self.description = Movie.normalize_description(description)
        self.cast = Movie.normalize_cast(cast)
        self.director = Movie.normalize_director(director)
        self.rating = Movie.normalize_rating(rating)

    @staticmethod
    def normalize_id(id) :
        if not id:
            raise Exception("InvalidMovieError")
    
        if isinstance(id, str):
            for char in id:
                if not char.isdigit():
                    raise Exception("InvalidMovieError")
            id = int(id)

        if id == 0 or id < 0:
            raise Exception("InvalidMovieError")

        return id

    @staticmethod
    def normalize_title(title):
        if not title:
            raise Exception("InvalidMovieError")
        
        title = " ".join(title.split())
        title = title.lower()
        return title
    
    @staticmethod
    def normalize_year(year):
        if year is None or year == "":
            raise Exception("InvalidDocumentError - missing year")

        if isinstance(year, float):
            if math.isnan(year):
                raise Exception("InvalidDocumentError - missing year")
            if year.is_integer():
                year = int(year)
            else:
                raise Exception("InvalidDocumentError - year is not int")

        if isinstance(year, str):
            year = year.strip()
            if not year or not year.isdigit():
                raise Exception("InvalidDocumentError - found character in numbers")
            year = int(year)

        if not isinstance(year, int):
            raise Exception("InvalidDocumentError - year is not int")

        if MIN_YEAR <= year <= MAX_YEAR:
            return year
        raise Exception("InvalidDocumentError - year out of bounds")
    
    @staticmethod
    def normalize_genres(genres):
        if genres is None:
            return []

        if genres is not None and not isinstance(genres, (str, list)):
            raise Exception("InvalidDocumentError - genres is not None, string or list")
        
        if isinstance(genres, str):
            genres = genres.split(",")
            genres = [g.strip().lower() for g in genres]

        if isinstance(genres, list):
            for genre in genres:
                if not isinstance(genre, str):
                    raise Exception("InvalidDocumentError - found number in strings")
            genres = [g.strip().lower() for g in genres]
        
        safe_list = genres[:]
        safe_list = list(dict.fromkeys(safe_list))
        return safe_list
    
    @staticmethod
    def normalize_description(description):
        if not description:
            description = ""
        
        description = description.strip()
        description = " ".join(description.split())
        return description
    
    @staticmethod
    def normalize_cast(cast):
        if cast is None:
            return []

        if cast is not None and not isinstance(cast, (str, list)):
            raise Exception("InvalidDocumentError - cast is not None, string or list")
        
        if isinstance(cast, str):
            cast = cast.split(",")
            cast = [c.strip().lower() for c in cast]

        if isinstance(cast, list):
            for item in cast:
                if not isinstance(item, str):
                    raise Exception("InvalidDocumentError - found number in strings")
            cast = [c.strip().lower() for c in cast]
        
        safe_list = cast[:]
        safe_list = list(dict.fromkeys(safe_list))
        return safe_list
    
    @staticmethod
    def normalize_director(director):
        if not director:
            director = ""

        director = director.strip()
        return director
    
    @staticmethod
    def normalize_rating(rating):
        if rating is None or rating == "":
            return None

        if rating is not None and not isinstance(rating, (int, float, str)):
            raise Exception("InvalidDocumentError - rating is not float, int or string")
        
        rating = float(rating)

        if RATING_MIN <= rating <= RATING_MAX:
            return rating
        else:
            raise Exception("InvalidDocumentError - rating out of bounds")
        
    def to_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "year": self.year,
            "genres": self.genres[:],
            "description": self.description,
            "cast": self.cast[:],
            "director": self.director,
            "rating": self.rating
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(id=data.get("id", None),
                   title=data.get("title", None),
                   year=data.get("year", None),
                   genres=data.get("genres", None),
                   description=data.get("description", None),
                   cast=data.get("cast", None),
                   director=data.get("director", None),
                   rating=data.get("rating", None))