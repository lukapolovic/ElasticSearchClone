from models.movie import Movie
import logging
from typing import Optional, Iterable, List
import json

def ingest_one(raw: dict) -> Optional[dict]:
    if not isinstance(raw, dict):
        return None
    
    try:
        movie = Movie.from_dict(raw)
    except Exception as e:
        logging.warning("Skipping doc id=%s: %s", raw.get('id', "<missing>"), e)
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

    logging.warning("OK=%s" % ok_count)
    logging.warning("SKIP=%s" % skipped_count)
    logging.warning("NOK=%s" % error_count)

    return results

def load_json_file(path):
    with open(path, "r", encoding="utf-8") as f:
        first_char = f.read(1)
        f.seek(0)

        # Case 1: JSON array
        if first_char == "[":
            data = json.load(f)

            if not isinstance(data, list):
                raise Exception("Expected a JSON array at top-level")
            
            for item in data:
                if isinstance(item, dict):
                    yield item
                else:
                    logging.warning("Item in JSON file is not a dict!")
                    continue
        
        # Case 2: NDJSON
        else: 
            for lineno, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    logging.warning(f"Skipping invalid JSON line {lineno}: {e}")
                    continue
                    
                if isinstance(obj, dict):
                    yield obj
                else:
                    logging.warning(f"Line {lineno} is not an object, skipping")