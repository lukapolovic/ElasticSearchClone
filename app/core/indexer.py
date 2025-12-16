from search.indexer import Indexer
from search.query import QueryEngine

indexer = Indexer()
engine = QueryEngine(indexer)