from search.tokenizer import tokenize
from search.query import QueryEngine

class MockIndexer:
    def __init__(self):
        self.index = {}
        self.documents = {}

    def add(self, doc_id, field_name, text, full_doc=None):
        tokens = tokenize(text)

        for token in tokens:
            if token not in self.index:
                self.index[token] = {}
            
            if doc_id not in self.index[token]:
                self.index[token][doc_id] = {
                    "fields": set(),
                    "tf": int(0)
                }
            
            self.index[token][doc_id]["fields"].add(field_name)
            self.index[token][doc_id]["tf"] += 1

        if full_doc:
            self.documents[doc_id] = full_doc

    def lookup(self, token):
        postings = self.index.get(token, {})
        return {
            doc_id: {
                "fields": posting["fields"].copy(),
                "tf": posting["tf"]
                }
                for doc_id, posting in postings.items()
            }

def build_mock_data():
    idx = MockIndexer()

    movies = {
        1: {
            "title": "The Matrix",
            "cast": ["Keanu Reeves", "Laurence Fishburne"],
            "director": "Lana Wachowski Lilly Wachowski",
            "genres": "Sci-Fi Action",
            "description": "A hacker discovers reality is a simulation",
            "year": "1999",
            "rating": "8.7"
        },
        2: {
            "title": "John Wick",
            "cast": ["Keanu Reeves", "Ian McShane"],
            "director": "Chad Stahelski",
            "genres": "Action Thriller",
            "description": "A retired assassin seeks revenge",
            "year": "2014",
            "rating": "7.4"
        },
        3: {
            "title": "Interstellar",
            "cast": ["Matthew McConaughey", "Anne Hathaway"],
            "director": "Christopher Nolan",
            "genres": "Sci-Fi Drama",
            "description": "Explorers travel through a wormhole",
            "year": "2014",
            "rating": "8.6"
        },
        4: {
            "title": "Inception",
            "cast": ["Leonardo DiCaprio", "Joseph Gordon-Levitt"],
            "director": "Christopher Nolan",
            "genres": "Sci-Fi Thriller",
            "description": "A thief enters dreams to steal secrets",
            "year": "2010",
            "rating": "8.8"
        },
        5: {
            "title": "The Dark Knight",
            "cast": ["Christian Bale", "Heath Ledger"],
            "director": "Christopher Nolan",
            "genres": "Action Crime",
            "description": "Batman faces the Joker in Gotham City",
            "year": "2008",
            "rating": "9.0"
        },
        6: {
            "title": "Pulp Fiction",
            "cast": ["John Travolta", "Samuel L. Jackson"],
            "director": "Quentin Tarantino",
            "genres": "Crime Drama",
            "description": "The lives of criminals intertwine in LA",
            "year": "1994",
            "rating": "8.9"
        },
        7: {
            "title": "Fight Club",
            "cast": ["Brad Pitt", "Edward Norton"],
            "director": "David Fincher",
            "genres": "Drama",
            "description": "An insomniac forms an underground fight club",
            "year": "1999",
            "rating": "8.8"
        },
        8: {
            "title": "Forrest Gump",
            "cast": ["Tom Hanks", "Robin Wright"],
            "director": "Robert Zemeckis",
            "genres": "Drama Romance",
            "description": "A man with a low IQ recounts his life story",
            "year": "1994",
            "rating": "8.8"
        },
        9: {
            "title": "The Lord of the Rings: The Fellowship of the Ring",
            "cast": ["Elijah Wood", "Ian McKellen"],
            "director": "Peter Jackson",
            "genres": "Fantasy Adventure",
            "description": "A hobbit begins a quest to destroy a powerful ring",
            "year": "2001",
            "rating": "8.8"
        },
        10: {
            "title": "Gladiator",
            "cast": ["Russell Crowe", "Joaquin Phoenix"],
            "director": "Ridley Scott",
            "genres": "Action Drama",
            "description": "A betrayed Roman general seeks revenge",
            "year": "2000",
            "rating": "8.5"
        }
    }

    # Add each field into the index and store full document
    for doc_id, fields in movies.items():
        for field_name, text in fields.items():
            if field_name == "cast":
                text_for_index = " ".join(text)
            else:
                text_for_index = text
            idx.add(doc_id, field_name, text_for_index, full_doc=fields)

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
            for doc in results:
                # Ensure cast is a list for printing
                cast_list = doc.get("cast", [])
                if isinstance(cast_list, str):
                    cast_list = cast_list.split()
                print(
                    f"  Doc ID: {doc['doc_id']}, \nTitle: {doc['title']}, "
                    f"\nDirector: {doc['director']}, \nCast: {', '.join(cast_list)}, "
                    f"\nYear: {doc['year']}, \nRating: {doc['rating']}, \nScore: {doc['score']:.2f}"
                    f"\nExplanation: {doc['explanations']}\n"
                )
            print("")