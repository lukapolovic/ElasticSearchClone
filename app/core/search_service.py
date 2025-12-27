import json
import time
from pathlib import Path
from search.indexer import Indexer
from search.query import QueryEngine
from app.models.search_response import SearchResponse, SearchResult
from app.core.exceptions import IndexNotReadyError, InvalidQueryError

class SearchService:
    def __init__(self, shard_id: int = 0, num_shards: int = 1):
        self.indexer = Indexer()
        self.engine = QueryEngine(self.indexer)
        self.shard_id = shard_id
        self.num_shards = num_shards

    def _load_movies(self, data_path: Path):
        if data_path.suffix == ".jsonl":
            movies = []
            with open(data_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    movies.append(json.loads(line))
            return movies

        with open(data_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def load_data(self):
        print("Loading data...")
        repo_root = Path(__file__).resolve().parents[2]
        jsonl_path = repo_root / "scripts" / "data" / "25kMovies.cleaned.jsonl"
        json_path = repo_root / "app" / "data" / "movies.json"
        data_path = jsonl_path if jsonl_path.exists() else json_path

        load_start = time.perf_counter()
        movies = self._load_movies(data_path)
        load_end = time.perf_counter()

        print(f"Loaded {len(movies)} movies from {data_path}.")

        fields = [
            "title",
            "year",
            "genres",
            "description",
            "cast",
            "director",
            "rating"
        ]

        if self.num_shards > 1:
            before = len(movies)
            movies = [m for m in movies if (int(m["id"])) % self.num_shards == self.shard_id]
            print(f"Shard {self.shard_id}/{self.num_shards}: kept {len(movies)} of {before} movies.")

        index_start = time.perf_counter()
        self.indexer.build(movies, fields)
        index_end = time.perf_counter()

        print(f"Index built with {self.indexer.total_documents} documents.")
        print(f"Load time: {load_end - load_start:.3f}s | Index time: {index_end - index_start:.3f}s")

    def search(self, query: str, page: int, page_size: int, debug: bool):

        if not self.indexer or self.indexer.total_documents == 0:
            raise IndexNotReadyError()
        
        if not query.strip():
            raise InvalidQueryError(details={"query": query})
        
        if page < 1:
            raise InvalidQueryError(details={"page": page})

        raw_results = self.engine.search(query, debug=debug)

        total_hits = len(raw_results)

        start = (page - 1) * page_size
        end = start + page_size
        page_results = raw_results[start:end]

        results = []
        for r in page_results:
            results.append(
                SearchResult(
                    doc_id=r['doc_id'],
                    title=r['title'],
                    director=r['director'],
                    cast=r['cast'],
                    year=r['year'],
                    rating=r['rating'],
                    score=r.get('score') if debug else None,
                    explanations=r.get('explanations') if debug else None
                )
            )

        return SearchResponse(
            query=query,
            total_hits=total_hits,
            page=page,
            page_size=page_size,
            results=results
        )
    
    def health_check(self):
        return {
            "total_documents": self.indexer.total_documents,
            "vocabulary_size": len(self.indexer.index),
            "status": "ok"
        }
