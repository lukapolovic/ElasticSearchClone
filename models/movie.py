import datetime

MIN_YEAR = 1888
MAX_YEAR = datetime.now().year + 5
RATING_MIN = 0.0
RATING_MAX = 10.0
DEFAULT_GENRES = []
DEFAULT_CAST = []

class Movie:
    """
    A Movie instance always has a non-empty title and normalized lists for "genres" and "cast" 
    """
    def __init__(self, id, title, year, genres, description, cast, director, rating):
        self.id = id
        self.title = title
        self.year = year
        self.genres = genres
        self.description = description
        self.cast = cast
        self.director = director
        self.rating = rating

    def normalize_id(id) :
        if not id:
            raise Exception("InvalidDocumentError")
        
        if isinstance(id, str):
            for char in id:
                if not char.isdigit():
                    raise Exception("InvalidDocumentError - found character in numbers")
            id = int(id)
        return id

    def normalize_title(title):
        if not title:
            raise Exception("InvalidDocumentError")
        
        title = " ".join(title.split())
        title = title.lower()
        return title
    
    def normalize_year(year):
        if not year:
            year = None
        
        if isinstance(year, str):
            for char in year:
                if not char.isdigit():
                    raise Exception("InvalidDocumentError - found character in numbers")
            
            if MIN_YEAR <= year <= MAX_YEAR:
                year = int(year)
            else:
                raise Exception("InvalidDocumentError - year out of bounds")
        return year
    
    def normalize_genres(genres):
        if (not genres is None) | (not isinstance(genres, str) | (not isinstance(genres, list))):
            raise Exception("InvalidDocumentError - genres is not None, string or list")
        
        if isinstance(genres, str):
            genres = genres.split(",")
            for genre in genres:
                genre.strip()

        if isinstance(genres, list):
            for genre in genres:
                if not isinstance(genre, str):
                    raise Exception("InvalidDocumentError - found number in strings")
                genre.strip().lower()
        
        genre = list(dict.fromkeys(genres))
        return genre
    
    def normalize_description(description):
        if not description:
            description = ""
        
        description = description.strip()
        description = " ".join(description.split())
        return description
    
    def normalize_cast(cast):
        if (not cast is None) | (not isinstance(cast, str) | (not isinstance(cast, list))):
            raise Exception("InvalidDocumentError - cast is not None, string or list")
        
        if isinstance(cast, str):
            cast = cast.split(",")
            for item in cast:
                item.strip()

        if isinstance(cast, list):
            for item in cast:
                if not isinstance(cast, str):
                    raise Exception("InvalidDocumentError - found number in strings")
                cast.strip().lower()
        
        cast = list(dict.fromkeys(cast))
        return cast
    
    def normalize_director(director):
        if not director:
            director = ""

        director = director.strip()
        return director
    
    def normalize_rating(rating):
        if not rating:
            rating = None

        if (not isinstance(rating, float) | (not isinstance(rating, int) | (not isinstance(rating, str)))):
            raise Exception("InvalidDocumentError - rating is not float, int or string")
        
        rating = float(rating)

        if RATING_MIN <= rating <= RATING_MAX:
            return rating
        else:
            raise Exception("InvalidDocumentError - rating out of bounds")