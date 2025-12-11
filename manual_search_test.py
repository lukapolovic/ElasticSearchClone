from search.tokenizer import tokenize
from search.query import QueryEngine

class MockIndexer:
    def __init__(self):
        self.index = {}

    def add(self, doc_id, field, text):
        for token in tokenize(text):
            if token not in self.index:
                self.index[token] = {}
            if doc_id not in self.index[token]:
                self.index[token][doc_id] = []
            self.index[token][doc_id].append(field)

    def lookup(self, token):
        if token not in self.index:
            return {}
        return {doc_id: fields.copy() for doc_id, fields in self.index[token].items()}

def build_mock_data():
    idx = MockIndexer()

    movies = {
        1: {
            "title": "The Matrix",
            "cast": "Keanu Reeves Laurence Fishburne",
            "director": "Lana Wachowski Lilly Wachowski",
            "genres": "Sci-Fi Action",
            "description": "A hacker discovers reality is a simulation",
            "year": "1999",
            "rating": "8.7"
        },
        2: {
            "title": "John Wick",
            "cast": "Keanu Reeves Ian McShane",
            "director": "Chad Stahelski",
            "genres": "Action Thriller",
            "description": "A retired assassin seeks revenge",
            "year": "2014",
            "rating": "7.4"
        },
        3: {
            "title": "Interstellar",
            "cast": "Matthew McConaughey Anne Hathaway",
            "director": "Christopher Nolan",
            "genres": "Sci-Fi Drama",
            "description": "Explorers travel through a wormhole",
            "year": "2014",
            "rating": "8.6"
        }
    }

    for doc_id, fields in movies.items():
        for field_name, text in fields.items():
            idx.add(doc_id, field_name, text)

    return idx, movies

if __name__ == "__main__":
    print("\n=== Manual Search Testing for QueryEngine ===\n")

    indexer, movies = build_mock_data()
    engine = QueryEngine(indexer)

    print("Test dataset loaded:")
    for doc_id, movie in movies.items():
        print(f"  {doc_id}: {movie['title']}")
    print("\nType a search query (or 'exit' to quit):\n")

    while True:
        query = input("> ")

        if query.lower().strip() in ["exit", "quit"]:
            break

        results = engine.search(query)

        print("\nResults (sorted by score):")
        if not results:
            print("  No results\n")
        else:
            for doc_id in results:
                print(f"  {doc_id}: {movies[doc_id]['title']}")
            print("")