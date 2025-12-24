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

# --- Fuzzy guardrails (tune as needed) ---
FUZZY_MIN_TOKEN_LEN = 4
FUZZY_MAX_TOKENS_PER_QUERY = 3
FUZZY_SCORE_THRESHOLD = 80
FUZZY_NON_TITLE_PENALTY = 0.6
FUZZY_DESCRIPTION_PENALTY = 0.8

# --- Synonym guardrails ---
SYN_MAX_PER_BASE_TOKEN = 5
SYN_SKIP_SHORT_TOKENS_LEN = 3 

class QueryEngine:
    def __init__(self, indexer):
        self.indexer = indexer

    def synonyms(self, base_tokens: set[str]) -> tuple[set[str], set[str]]:
        """
        Return (base_tokens, expanded_tokens).
        expanded_tokens includes base tokens + limited WordNet expnasions (synonyms).

        expansions are meant to help recall, but should not trigger fuzzy matching.
        """
        expanded_tokens: set[str] = set(base_tokens)

        for token in base_tokens:
            if token.isdigit():
                continue

            if len(token) <= SYN_SKIP_SHORT_TOKENS_LEN:
                continue

            synsets = wordnet.synsets(token)
            
            added = 0
            for synset in synsets:
                for lemma in synset.lemmas(): # type: ignore
                    raw = lemma.name().replace("_", " ").lower()

                    normalized_tokens = tokenize(raw)
                    for nt in normalized_tokens:
                        if nt == token:
                            continue
                        expanded_tokens.add(nt)
                        added += 1
                        if added >= SYN_MAX_PER_BASE_TOKEN:
                            break
                    
                    if added >= SYN_MAX_PER_BASE_TOKEN:
                        break
                
                if added >= SYN_MAX_PER_BASE_TOKEN:
                    break

        return base_tokens, expanded_tokens

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

    def search(self, query_string, debug=False):
        if not query_string:
            return []

        tokens = tokenize(query_string)
        if not tokens:
            return []
        base_tokens = set(tokens)

        base_tokens, expanded_tokens = self.synonyms(base_tokens)

        scores = {}
        if debug:
            explanations = {}

        fuzzy_budget = FUZZY_MAX_TOKENS_PER_QUERY

        for token in expanded_tokens:
            is_base = token in base_tokens

            if token in self.indexer.index:
                    token_matches = [(token, 1.0)]
            else:
                if not is_base:
                    continue
                if token.isdigit():
                    continue
                if len(token) < FUZZY_MIN_TOKEN_LEN:
                    continue
                if fuzzy_budget <= 0:
                    continue

                candidates = self.indexer.fuzzy_candidates(token, max_candidates=300)
                if not candidates:
                    continue
                token_matches = self.get_closest_token(token, candidates)
                fuzzy_budget -= 1

                if not token_matches:
                    continue

            for match_token, similarity in token_matches:
                postings = self.indexer.lookup(match_token)
                idf = self.indexer.idf(match_token) * similarity

                for doc_id, posting in postings.items():
                    scores.setdefault(doc_id, 0.0)
                    if debug:
                        explanations.setdefault(doc_id, []) # type: ignore

                    tf_by_field = posting.get("tf_by_field", {})
                    fields = posting["fields"]

                    for field in fields:
                        weight  = FIELD_WEIGHTS.get(field, 0.0)
                        field_tf = tf_by_field.get(field, 0)
                        if field_tf <= 0:
                            continue
                        contribution = weight * field_tf * idf

                        if similarity < 1.0:
                            if field == "description":
                                contribution *= FUZZY_DESCRIPTION_PENALTY
                            elif field != "title":
                                contribution *= FUZZY_NON_TITLE_PENALTY

                        scores[doc_id] += contribution

                        if debug:
                            explanations[doc_id].append({ # type: ignore
                                "token": token,
                                "field": field,
                                "weight": weight,
                                "tf_by_field": tf_by_field.get(field, 0),
                                "idf": idf,
                                "similarity": similarity,
                                "contribution": contribution
                            })

        ranked_docs = sorted(scores.items(), key=lambda x: (-x[1], x[0]))

        results = []

        for doc_id, score in ranked_docs:
            doc = self.indexer.documents.get(doc_id, {})
            item = {
                "doc_id": doc_id,
                "title": doc.get("title", ""),
                "director": doc.get("director", ""),
                "cast": doc.get("cast", []),
                "year": doc.get("year", ""),
                "rating": doc.get("rating", ""),
            }
            if debug:
                item["score"] = score
                item["explanations"] = explanations.get(doc_id, []) # type: ignore
            results.append(item)
            
        return results