from search.tokenizer import tokenize
from search.query import QueryEngine
import math

# -------------------- Mock Indexer --------------------
class MockIndexer:
    def __init__(self):
        self.index = {}
        self.documents = {}
        self.total_documents = 0
        self.doc_freq = {}

    def add(self, doc_id, field_name, tokens, full_doc=None):
        # Increment total document count for new documents
        if doc_id not in self.documents:
            self.total_documents += 1

        for token in tokens:
            if token not in self.index:
                self.index[token] = {}
            if token not in self.doc_freq:
                self.doc_freq[token] = 0

            if doc_id not in self.index[token]:
                self.index[token][doc_id] = {"fields": set(), "tf": 0}
                self.doc_freq[token] += 1  # Increment DF once per doc

            self.index[token][doc_id]["fields"].add(field_name)
            self.index[token][doc_id]["tf"] += 1

        if full_doc:
            self.documents[doc_id] = full_doc

    def idf(self, token):
        df = self.doc_freq.get(token, 0)
        # Add 1 to denominator to avoid division by zero
        return math.log((self.total_documents) / (df + 1))

    def lookup(self, token):
        postings = self.index.get(token, {})
        return {
            doc_id: {
                "fields": posting["fields"].copy(),
                "tf": posting["tf"]
            }
            for doc_id, posting in postings.items()
        }

# -------------------- Build Mock Dataset --------------------
def build_mock_data():
    idx = MockIndexer()

    movies = {
        1: {"title": "The Matrix", "cast": ["Keanu Reeves", "Laurence Fishburne"], "director": "Lana Wachowski Lilly Wachowski", "genres": "Sci-Fi Action", "description": "A hacker discovers reality is a simulation", "year": "1999", "rating": "8.7"},
        2: {"title": "John Wick", "cast": ["Keanu Reeves", "Ian McShane"], "director": "Chad Stahelski", "genres": "Action Thriller", "description": "A retired assassin seeks revenge", "year": "2014", "rating": "7.4"},
        3: {"title": "Interstellar", "cast": ["Matthew McConaughey", "Anne Hathaway"], "director": "Christopher Nolan", "genres": "Sci-Fi Drama", "description": "Explorers travel through a wormhole", "year": "2014", "rating": "8.6"},
        4: {"title": "Inception", "cast": ["Leonardo DiCaprio", "Joseph Gordon-Levitt"], "director": "Christopher Nolan", "genres": "Sci-Fi Thriller", "description": "A thief enters dreams to steal secrets", "year": "2010", "rating": "8.8"},
        5: {"title": "The Dark Knight", "cast": ["Christian Bale", "Heath Ledger"], "director": "Christopher Nolan", "genres": "Action Crime", "description": "Batman faces the Joker in Gotham City", "year": "2008", "rating": "9.0"},
        6: {"title": "Pulp Fiction", "cast": ["John Travolta", "Samuel L. Jackson"], "director": "Quentin Tarantino", "genres": "Crime Drama", "description": "The lives of criminals intertwine in LA", "year": "1994", "rating": "8.9"},
        7: {"title": "Fight Club", "cast": ["Brad Pitt", "Edward Norton"], "director": "David Fincher", "genres": "Drama", "description": "An insomniac forms an underground fight club", "year": "1999", "rating": "8.8"},
        8: {"title": "Forrest Gump", "cast": ["Tom Hanks", "Robin Wright"], "director": "Robert Zemeckis", "genres": "Drama Romance", "description": "A man with a low IQ recounts his life story", "year": "1994", "rating": "8.8"},
        9: {"title": "The Lord of the Rings: The Fellowship of the Ring", "cast": ["Elijah Wood", "Ian McKellen"], "director": "Peter Jackson", "genres": "Fantasy Adventure", "description": "A hobbit begins a quest to destroy a powerful ring", "year": "2001", "rating": "8.8"},
        10: {"title": "Gladiator", "cast": ["Russell Crowe", "Joaquin Phoenix"], "director": "Ridley Scott", "genres": "Action Drama", "description": "A betrayed Roman general seeks revenge", "year": "2000", "rating": "8.5"},
    }

    # Index each field
    for doc_id, fields in movies.items():
        for field_name, value in fields.items():
            if isinstance(value, list):
                tokens = tokenize(" ".join(value))
            else:
                tokens = tokenize(str(value))
            idx.add(doc_id, field_name, tokens, full_doc=fields)

    return idx, movies

# -------------------- Manual Search Testing --------------------
if __name__ == "__main__":
    print("\n=== Manual Search Testing for QueryEngine (TF-IDF) ===\n")

    indexer, movies = build_mock_data()
    engine = QueryEngine(indexer)

    print("Test dataset loaded:")
    for doc_id, movie in movies.items():
        print(f"  {doc_id}: {movie['title']}")
    print("\nType a search query (or 'exit' to quit):\n")

    while True:
        query = input("> ").strip()
        if query.lower() in ["exit", "quit"]:
            break

        results = engine.search(query)

        print("\nResults (sorted by score):")
        if not results:
            print("  No results\n")
        else:
            for doc in results:
                cast_list = doc.get("cast", [])
                if isinstance(cast_list, str):
                    cast_list = cast_list.split()
                print(
                    f"  Doc ID: {doc['doc_id']}, \n"
                    f"  Title: {doc['title']}, \n"
                    f"  Director: {doc['director']}, \n"
                    f"  Cast: {', '.join(cast_list)}, \n"
                    f"  Year: {doc['year']}, \n"
                    f"  Rating: {doc['rating']}, \n"
                    f"  Score: {doc['score']:.2f}, \n"
                    f"  Explanation: {doc['explanations']}\n"
                )
            print("")