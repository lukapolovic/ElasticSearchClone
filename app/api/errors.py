from fastapi import Request
from fastapi.responses import JSONResponse
from app.models.api_response import APIResponse
from app.models.error import APIError
from app.core.exceptions import SearchError

async def search_error_handler(request: Request, exc: Exception):
    assert isinstance(exc, SearchError)

    return JSONResponse(
        status_code=400,
        content=APIResponse(
            status="error",
            error=APIError(
                code=exc.code,
                message=exc.message,
                details=exc.details
            )
        ).model_dump()
    )