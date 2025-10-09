"""
Data models for the application.
"""

from pydantic import BaseModel


class Product(BaseModel):
    """
    A model representing a product.
    """

    Appl_No: str | None = None
    Appl_Type: str | None = None
    Applicant: str | None = None
    Applicant_Full_Name: str | None = None
    Approval_Date: str | None = None
    DF_Route: str | None = None
    Ingredient: str | None = None
    Product_No: str | None = None
    RLD: str | None = None
    RS: str | None = None
    Strength: str | None = None
    TE_Code: str | None = None
    Trade_Name: str | None = None
    Type: str | None = None
