import json
from pathlib import Path
from search.indexer import Indexer
from search.query import QueryEngine
from app.models.search_response import SearchResponse, SearchResult
from app.core.exceptions import IndexNotReadyError, InvalidQueryError

class SearchService:
    def __init__(self):
        self.indexer = Indexer()
        self.engine = QueryEngine(self.indexer)

    def load_data(self):
        print("Loading data...")
        data_path = Path(__file__).parent.parent / "data" / "movies.json"

        with open(data_path, 'r', encoding='utf-8') as f:
            movies = json.load(f)

        print(f"Loaded {len(movies)} movies.")

        fields = [
            "title",
            "year",
            "genres",
            "description",
            "cast",
            "director",
            "rating"
        ]

        self.indexer.build(movies, fields)

        print(f"Index built with {self.indexer.total_documents} documents.")

    def search(self, query: str, page: int, page_size: int, debug: bool):

        if not self.indexer or self.indexer.total_documents == 0:
            raise IndexNotReadyError()
        
        if not query.strip():
            raise InvalidQueryError(details={"query": query})

        raw_results = self.engine.search(query)

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