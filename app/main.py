from fastapi import FastAPI
from app.api.routes import search, documents

app = FastAPI(title="ElasticSearch Clone")

app.include_router(search.router, prefix="/search", tags=["search"])
app.include_router(documents.router, prefix="/documents", tags=["documents"])