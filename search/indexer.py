from search.tokenizer import tokenize
import math

class Indexer:
    
    def __init__(self):
        self.index = {}
        self.documents = {}
        self.total_documents = 0
        self.doc_freq = {}

    def add_tokens(self, doc_id, field_name, tokens, full_doc=None):
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

    def idf(self, token):
        df = self.doc_freq.get(token, 0)
        return math.log(self.total_documents / (df + 1))

    def lookup(self, token):
        postings = self.index.get(token, {})
        return {
            doc_id: {
                "fields": posting["fields"].copy(),
                "tf": posting["tf"]
                }
                for doc_id, posting in postings.items()
            }