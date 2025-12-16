import json
from pathlib import Path
from search.indexer import Indexer
from search.query import QueryEngine

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

    def search(self, query: str):
        return self.engine.search(query)