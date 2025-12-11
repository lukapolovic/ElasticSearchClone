from search.tokenizer import tokenize

FIELD_WEIGHTS = {
    "title": 5.0,
    "cast": 4.0,
    "director": 3.0,
    "genres": 3.0,
    "description": 1.0,
    "year": 0.5,
    "rating": 0.1
}


class QueryEngine:
    def __init__(self, indexer):
        self.indexer = indexer

    def search(self, query_string):
        if not query_string:
            return []
        
        tokens = tokenize(query_string)
        if not tokens:
            return []
        
        scores = {}

        for token in tokens:
            matches = self.indexer.lookup(token)

            for doc_id, fields in matches.items():
                if doc_id not in scores:
                    scores[doc_id] = 0.0

                for field in fields:
                    weight = FIELD_WEIGHTS.get(field, 0.0)
                    scores[doc_id] += weight
            
        ranked_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        return [doc_id for doc_id, _ in ranked_docs]
            
