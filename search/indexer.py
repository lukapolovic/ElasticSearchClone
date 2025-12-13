from search.tokenizer import tokenize

class Indexer:
    
    def __init__(self):
        self.index = {}
        self.documents = {}

    def add_document(self, doc_id, field_name, text, full_doc=None):
        tokens = tokenize(text)

        for token in tokens:
            if token not in self.index:
                self.index[token] = {}
            
            if doc_id not in self.index[token]:
                self.index[token][doc_id] = set()
            
            self.index[token][doc_id].add(field_name)

        if full_doc:
            self.documents[doc_id] = full_doc

    def build(self, documents, fields):
        for doc in documents:
            doc_id = doc["id"]

            for field in fields:
                field_text = doc[field]

                if isinstance(field_text, list):
                    field_text = " ".join(field_text)
                else:
                    field_text = str(field_text)
                    
                self.add_document(doc_id, field, field_text, full_doc=doc)

    def lookup(self, token):
        postings = self.index.get(token, {})
        return {doc_id: fields.copy() for doc_id, fields in postings.items()}