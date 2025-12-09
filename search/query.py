from search.tokenizer import tokenize

class QueryEngine:
    def __init__(self, indexer):
        self.indexer = indexer

    def search(self, query_string):
        if not query_string:
            return set()
        
        tokens = tokenize(query_string)
        if not tokens:
            return set()

        posting_lists = []
        for token in tokens:
            docs = self.indexer.lookup(token)
            if not docs:
                return set()
            posting_lists.append(docs)

        result = posting_lists[0].copy()

        for docs in posting_lists[1:]:
            result &= docs

        return result
            
