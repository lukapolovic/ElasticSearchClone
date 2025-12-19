class SearchError(Exception):
    code = "SEARCH_ERROR"
    message = "Search failed"

    def __init__(self, message: str | None = None, details=None):
        self.message = message or self.message
        self.details = details
        super().__init__(self.message)

class IndexNotReadyError(SearchError):
    code = "INDEX_NOT_READY"
    message = "The search index is not ready yet"

class InvalidQueryError(SearchError):
    code = "INVALID_QUERY"
    message = "The search query is invalid"