"""
Data models for the application.
"""

from typing import Any, Callable, Literal, Union
from psycopg import sql
from pydantic import (
    BaseModel,
    ConfigDict,
    computed_field,
    field_validator,
    model_validator,
)


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


type Parser = Callable[
    [Any, sql.Composable, list[Any]], tuple[sql.Composable, list[Any]]
]

InfixOperators = Literal[
    "=",
    "!=",
    "<>",
    ">",
    "<",
    ">=",
    "<=",
    "*",
    "/",
    "%",
    "+",
    "-",
    "AND",
    "OR",
]

MixedOperators = Literal["+", "-"]

PrefixOperators = Literal["+", "-", "NOT"]

VerboseOperators = Literal["BETWEEN", "CAST", "IN", "IS", "TRIM"]

DataTypes = Literal[
    "BIGINT",
    "BIGSERIAL",
    "BIT",
    "BOOLEAN",
    "BOX",
    "BYTEA",
    "CHARACTER",
    "CIRCLE",
    "DATE",
    "DECIMAL",
    "DOUBLE PRECISION",
    "INTEGER",
    "INTERVAL",
    "JSON",
    "JSONB",
    "LINE",
    "LSEG",
    "MONEY",
    "NUMERIC",
    "PATH",
    "POINT",
    "POLYGON",
    "REAL",
    "REGCLASS",
    "SERIAL",
    "SMALLINT",
    "SMALLSERIAL",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "UUID",
    "XML",
]

TimeIntervals = Literal[
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    "YEAR TO MONTH",
    "DAY TO HOUR",
    "DAY TO MINUTE",
    "DAY TO SECOND",
    "HOUR TO MINUTE",
    "HOUR TO SECOND",
    "MINUTE TO SECOND",
]


class BaseOperator(BaseModel, frozen=True):
    """
    A base model for operators.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    operator: Union[InfixOperators, MixedOperators, PrefixOperators, VerboseOperators]

    @field_validator("operator", mode="before")
    @classmethod
    def normalize_operator(cls, value: str) -> str:
        """
        Normalize the operator.
        """

        return value.upper()

    @computed_field
    @property
    def sql_operator(self) -> sql.Composable:
        """
        Get the SQL operator.
        """
        return sql.SQL(f" {self.operator} ")


class InfixOperator(BaseOperator, frozen=True):
    """
    A model representing an infix operator.
    """

    expressions: Any | list[Any] | None = None
    operator: InfixOperators
    left: Any | None = None
    right: Any | None = None
    wrap: bool = False

    @model_validator(mode="after")
    def check_values(self) -> "InfixOperator":
        """
        Validate the model fields.
        """

        if self.expressions is None and not (self.left and self.right):
            raise ValueError(
                f"Operator '{self.operator}' requires 'left' and 'right' arguments"
            )
        return self


class MixedOperator(BaseOperator, frozen=True):
    """
    A model representing a mixed operator.
    """

    expressions: Any | list[Any] | None = None
    left: Any | None = None
    operand: Any | None = None
    operator: MixedOperators
    right: Any | None = None


class PrefixOperator(BaseOperator, frozen=True):
    """
    A model representing a prefix operator.
    """

    operator: PrefixOperators
    operand: Any


class BetweenOperator(BaseOperator, frozen=True):
    """
    A model representing a BETWEEN operator.
    """

    expression: Any
    left: Any
    right: Any
    operator: Literal["BETWEEN"]
    symmetric: bool = False


class CastOperator(BaseOperator, frozen=True):
    """
    A model representing a CAST operator.
    """

    expression: Any
    type: DataTypes
    interval: TimeIntervals | None = None
    operator: Literal["CAST"]
    varying: bool = False
    length: int | None = None
    precision: int | None = None
    scale: int | None = None
    with_time_zone: bool | None = None

    @field_validator("interval", "type", mode="before")
    @classmethod
    def normalize_fields(cls, value: str) -> str:
        """
        Normalize the type.
        """

        return value.upper()

    @computed_field
    @property
    def type_name(self) -> sql.Composable:
        """
        Get the data type name.
        """
        return sql.SQL(f" AS {self.type} ")

    @model_validator(mode="after")
    def check_values(self) -> "CastOperator":
        """
        Validate the model fields.
        """

        if self.length and self.precision:
            raise ValueError(
                f"Operator '{self.operator}' cannot have both 'length' and 'precision'"
            )
        return self


class InOperator(BaseOperator, frozen=True):
    """
    A model representing an IN operator.
    """

    left: Any
    operator: Literal["IN"]
    right: list[Any] | Any

    @computed_field
    @property
    def args(self) -> list[Any]:
        """
        Get the function arguments.
        """
        return self.right if isinstance(self.right, list) else [self.right]  # type: ignore
