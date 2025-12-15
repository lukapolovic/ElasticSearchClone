from search.tokenizer import tokenize
from nltk.corpus import wordnet
from rapidfuzz import process, fuzz

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

    def get_closest_token(self, token, index_tokens, limit=3, score_threshold=80):
        """
        Given a token and all tokens in the index, return a list of closest matching tokens
        above a similarity threshold (0-100) and up to a specified limit.
        """
        matches = process.extract(
            token,
            index_tokens,
            scorer=fuzz.ratio,
            limit=limit
        )

        return [(match, score / 100.0) for match, score, _ in matches if score >= score_threshold]

    def search(self, query_string):
            if not query_string:
                return []

            tokens = tokenize(query_string)
            if not tokens:
                return []

            expanded_tokens = self.synonyms(tokens)

            scores = {}
            explanations = {}

            index_tokens = list(self.indexer.index.keys())

            for token in expanded_tokens:
                if token in self.indexer.index:
                    token_matches = [(token, 1.0)]
                else:
                    token_matches = self.get_closest_token(token, index_tokens)

                for match_token, similarity in token_matches:
                    postings = self.indexer.lookup(match_token)
                    idf = self.indexer.idf(match_token) * similarity

                    for doc_id, posting in postings.items():
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
                                "similarity": similarity,
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