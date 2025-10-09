"""
A simple Flask API with multiple endpoints.
"""

import sys
import asyncio
from typing import Any
from flask import Flask, request
from src.py_pg import PyPg

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


app = Flask(__name__)
py_pg = PyPg()


@app.get("/")
def hello_world():
    """
    A simple endpoint that returns a greeting message.
    """
    return "Hello, World!"


@app.get("/health")
def health_check():
    """
    A health check endpoint.
    """
    return {"status": "ok"}, 200


@app.get("/products")
async def get_products():
    """
    An endpoint that returns a list of products.
    """

    return await py_pg.list_get_rows(
        "products", limit=request.args.get("limit", 100, type=int)
    )


@app.get("/products/<string:application_number>")
async def get_product_number(application_number: str):
    """
    An endpoint that returns a specific product.
    """

    rows = await py_pg.list_get_rows("products", where={"Appl_No": application_number})
    if len(rows) == 0:
        return {"message": "Product not found"}, 404
    return rows, 200


@app.delete("/products/<string:application_number>")
async def delete_product_number(application_number: str) -> tuple[dict[str, Any], int]:
    """
    An endpoint that deletes a specific product.
    """

    rows = await py_pg.delete_many_rows("products", Appl_No=application_number)
    if len(rows) == 0:
        return {"message": "Product not found"}, 404
    return {"message": f"{len(rows)} product(s) deleted", "rows": rows}, 200


@app.post("/products")
async def create_product() -> tuple[list[dict[str, Any]], int]:
    """
    An endpoint that creates a new product.
    """

    return await py_pg.list_add_rows("products", None, request.get_json()), 201


if __name__ == "__main__":
    app.run(debug=True)
