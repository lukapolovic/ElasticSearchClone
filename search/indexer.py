from search.tokenizer import tokenize
from collections import defaultdict
import math

class Indexer:
    
    def __init__(self):
        self.index = {}
        self.documents = {}
        self.total_documents = 0
        self.doc_freq = {}
        self.index_tokens = []
        self.ngram_index = defaultdict(set)
        self.ngram_n = 3

    def add_tokens(self, doc_id, field_name, tokens, full_doc=None):
        for token in tokens:
            if token not in self.index:
                self.index[token] = {}
            
            if doc_id not in self.index[token]:
                self.index[token][doc_id] = {
                    "fields": set(),
                    "tf": int(0),
                    "tf_by_field": {}
                }
            
            self.index[token][doc_id]["fields"].add(field_name)
            self.index[token][doc_id]["tf"] += 1
            self.index[token][doc_id]["tf_by_field"][field_name] = self.index[token][doc_id]["tf_by_field"].get(field_name, 0) + 1  

        if full_doc:
            self.documents[doc_id] = full_doc

    def build(self, documents, fields):
        for doc in documents:
            self.total_documents += 1
            seen_tokens = set()
            doc_id = doc["id"]

            for field in fields:
                field_text = doc[field]

                if isinstance(field_text, list):
                    field_text = " ".join(field_text)
                else:
                    field_text = str(field_text)

                tokens = tokenize(field_text)
                for token in tokens:
                    if token not in seen_tokens:
                        self.doc_freq[token] = self.doc_freq.get(token, 0) + 1
                        seen_tokens.add(token)
                    
                self.add_tokens(doc_id, field, tokens, full_doc=doc)

        self.index_tokens = list(self.index.keys())
        self.ngram_index.clear()

        n = self.ngram_n
        for tok in self.index_tokens:
            for ng in self._ngrams(tok, n):
                self.ngram_index[ng].add(tok)

    def idf(self, token):
        df = self.doc_freq.get(token, 0)
        return math.log(self.total_documents / (df + 1))
    
    def _ngrams(self, s: str, n: int):
        s = s.strip()
        if not s:
            return []
        if len(s) < n:
            if len(s) < 2:
                return [s]
            return [s[i:i+2] for i in range(len(s)-1)]
        return [s[i:i+n] for i in range(len(s)-n+1)]

    def fuzzy_candidates(self, token: str, max_candidates: int = 400) -> list[str]:
        """
        Return a reduced candidate list fro fuzzy matching using character n-grams.
        """
        n = self.ngram_n
        grams = self._ngrams(token, n)

        counts = {}
        for g in grams:
            for cand in self.ngram_index.get(g, ()):
                counts[cand] = counts.get(cand, 0) + 1
        
        if not counts:
            return []
        
        min_overlap = 2 if len(token) >= 6 else 1
        ranked = [(tok, c) for tok, c in counts.items() if c >= min_overlap]
        ranked.sort(key=lambda x: x[1], reverse=True)
        return [tok for tok, _ in ranked[:max_candidates]]

    def lookup(self, token):
        postings = self.index.get(token, {})
        return {
            doc_id: {
                "fields": posting["fields"].copy(),
                "tf": posting["tf"],
                "tf_by_field": dict(posting.get("tf_by_field", {}))
                }
                for doc_id, posting in postings.items()
            }