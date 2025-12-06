from models.movie import Movie
import logging
from typing import Optional, Iterable, List

def ingest_one(raw: dict) -> Optional[dict]:
    if not isinstance(raw, dict):
        return None
    
    try:
        movie = Movie.from_dict(raw)
    except Exception as e:
        logging.warning("Skipping doc id=%s: %s", raw.get('id', e))
        return None
    
    movie_doc = movie.to_dict()
    return movie_doc
    

def ingest_many(raw_list: Iterable[dict], continue_on_error=True) -> List[dict]:
    results = []
    ok_count = 0
    skipped_count = 0
    error_count = 0

    for raw in raw_list:
        result = ingest_one(raw)

        if result is None:
            if continue_on_error:
                skipped_count += 1
                continue
            else: 
                error_count += 1
                raise Exception("Something when wrong during batch ingestion!")
        else:
            ok_count += 1
            results.append(result)

    logging.warning("OK=%s: %s", ok_count)
    logging.warning("SKIP=%s: %s", skipped_count)
    logging.warning("NOK=%s: %s", error_count)

    return results