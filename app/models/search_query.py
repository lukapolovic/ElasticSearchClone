from pydantic import BaseModel, Field

class SearchQuery(BaseModel):
    q: str = Field(
        ...,
        min_length=2,
        max_length=100,
        description="Seach query string"
    )

    page: int = Field(
        default=1,
        ge=1,
        description="Page number (1-based)"
    )

    page_size: int = Field(
        default=10,
        ge=1,
        le=50,
        description="Number of results per page"
    )

    debug: bool = Field(
        default=False,
        description="Include scoring explanations"
    )