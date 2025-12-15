from search.tokenizer import tokenize
from nltk.corpus import wordnet

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

    def synonyms(self, tokens):
        expanded_tokens = set()

        for token in tokens:
            expanded_tokens.add(token)
            
            if token.isdigit():
                continue

            synsets = wordnet.synsets(token)

            added = 0
            for synset in synsets:
                # synset is guaranteed to be a Synset object
                for lemma in synset.lemmas(): # type: ignore
                    raw = lemma.name().replace("_", " ").lower()

                    normalized_tokens = tokenize(raw)

                    for nt in normalized_tokens:
                        expanded_tokens.add(nt)
                        added += 1

                    if added >= 5:
                        break

                if added >= 5:
                    break

        return expanded_tokens

    def search(self, query_string):
        if not query_string:
            return []

        tokens = tokenize(query_string)
        if not tokens:
            return []

        expanded_tokens = self.synonyms(tokens)

        scores = {}
        explanations = {}

        for token in expanded_tokens:
            matches = self.indexer.lookup(token)
            idf = self.indexer.idf(token)

            for doc_id, posting in matches.items():
                scores.setdefault(doc_id, 0.0)
                explanations.setdefault(doc_id, [])

                tf = posting["tf"]
                fields = posting["fields"]

                for field in fields:
                    weight  = FIELD_WEIGHTS.get(field, 0.0)
                    contribution = weight * tf * idf

                    scores[doc_id] += contribution

                    explanations[doc_id].append({
                        "token": token,
                        "field": field,
                        "weight": weight,
                        "tf": tf,
                        "idf": idf,
                        "contribution": contribution
                    })

        ranked_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)

        results = []

        for doc_id, score in ranked_docs:
            doc = self.indexer.documents.get(doc_id, {})
            results.append({
                "doc_id": doc_id,
                "title": doc.get("title", ""),
                "director": doc.get("director", ""),
                "cast": doc.get("cast", [])[:2],
                "year": doc.get("year", ""),
                "rating": doc.get("rating", ""),
                "score": score,
                "explanations": explanations[doc_id]
            })
        
        return results