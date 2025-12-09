import tokenizer

class Indexer:
    
    def __init__(self):
        self.index = {}

    def add_document(self, doc_id, text):
        tokens = tokenizer.tokenize(text)

        for token in tokens:
            if token not in self.index:
                self.index[token] = set()
            
            self.index[token].add(doc_id)

    def build(self, documents, fields):
        for doc in documents:
            doc_id = doc["id"]

            parts = []
            for field in fields:
                parts.append(doc[field])
            
            combined_text = " ".join(parts)

            self.add_document(doc_id, combined_text)

    def lookup(self, token):
        return self.index.get(token, set())