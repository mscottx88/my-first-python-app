"""
A simple FastAPI application with endpoints for managing products.
"""

from http import HTTPStatus
import sys
from typing import Any, AsyncIterator
import asyncio
import asyncstdlib as a
from fastapi import FastAPI
from fastapi.responses import StreamingResponse, JSONResponse
import uvicorn
from src.py_pg import PyPg
from src.models import Product

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app = FastAPI()
py_pg = PyPg()


async def products(source: AsyncIterator[dict[str, Any]]):
    """
    An async generator that yields products in JSON format.
    """

    yield "[\n"
    async for [index, row] in a.enumerate(source):
        if index > 0:
            yield ",\n"
        yield Product(**row).model_dump_json()
    yield "\n]"


@app.get("/")
def hello_world() -> str:
    """
    A simple endpoint that returns a greeting message.
    """
    return "Hello, World"


@app.get("/health", status_code=HTTPStatus.NO_CONTENT)
def health_check() -> None:
    """
    A health check endpoint.
    """
    return None


@app.get("/products")
async def get_products(limit: int = 100) -> StreamingResponse:
    """
    An endpoint that returns a list of products.
    """

    return StreamingResponse(
        products(py_pg.get_rows("products", limit=limit)),
        media_type=JSONResponse.media_type,
    )


@app.get("/products/{application_number}")
async def get_product_number(application_number: str) -> StreamingResponse:
    """
    An endpoint that returns a specific product.
    """

    return StreamingResponse(
        products(py_pg.get_rows("products", where={"Appl_No": application_number})),
        media_type=JSONResponse.media_type,
    )


@app.delete("/products/{application_number}")
async def delete_product_number(application_number: str) -> StreamingResponse:
    """
    An endpoint that deletes a specific product.
    """

    return StreamingResponse(
        products(py_pg.delete_rows("products", Appl_No=application_number)),
        media_type=JSONResponse.media_type,
    )


@app.post("/products", status_code=HTTPStatus.CREATED)
async def create_product(product: Product) -> Product:
    """
    An endpoint that creates a new product.
    """

    return Product(**await py_pg.add_row("products", None, **product.model_dump()))


if __name__ == "__main__":
    uvicorn.run("fast_api:app", host="0.0.0.0", port=8000, reload=True)
