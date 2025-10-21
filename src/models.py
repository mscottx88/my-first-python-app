"""
Data models for the application.
"""

from typing import Annotated, Any, Callable, Literal, Sequence, Union
from psycopg import sql
from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    computed_field,
    field_validator,
    model_validator,
)


def get_expression_type(data: Any) -> str:
    """
    Get the expression type from the data.
    """

    if isinstance(data, list):
        return "Sequence[ExpressionItem]"
    if data is None:
        return "none"
    if isinstance(data, bool):
        return "bool"
    if isinstance(data, int):
        return "int"
    if isinstance(data, float):
        return "float"
    if isinstance(data, str):
        return "str"
    if "column" in data:
        return "column"
    if "default" in data:
        return "default"
    if "function name" in data:
        return "function"
    if "operator" in data:
        return "operator"
    if "sub query" in data:
        return "sub query"
    if "value" in data:
        return "value"

    raise ValueError("Invalid expression type")


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

Clauses = Literal[
    "combine",
    "with",
    "delete",
    "insert",
    "select",
    "update",
    "values",
    "from",
    "where",
    "group by",
    "having",
    "order by",
    "limit",
    "offset",
    "returning",
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

FunctionNames = Literal["IN", "MAX", "TRIM", "UPPER"]


class BaseExpression(BaseModel, frozen=True):
    """
    A base model for SQL expressions.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)
    wrap: bool = False


class ColumnExpression(BaseExpression, frozen=True):
    """
    A model representing a column expression.
    """

    column: str | Literal["*"]
    correlation: str | None = None


class DefaultExpression(BaseExpression, frozen=True):
    """
    A model representing a default expression.
    """

    default: bool = True


class FunctionExpression(BaseExpression, frozen=True):
    """
    A model representing a function expression.
    """

    args: Sequence["ExpressionItem"] | None = []
    function_name: FunctionNames = Field(alias="function name")
    pad: bool = False
    schema_name: str | None = None

    @field_validator("function_name", mode="before")
    @classmethod
    def normalize_values(cls, value: str) -> str:
        """
        Normalize the value.
        """

        return value.upper()


class OperatorExpression(BaseExpression, frozen=True):
    """
    A model representing an operator expression.
    """

    operator: str
    expressions: Union["ExpressionItem", Sequence["ExpressionItem"], None] = None
    left: Union["ExpressionItem", None] = None
    right: Union["ExpressionItem", None] = None
    operand: Union["ExpressionItem", None] = None


class SubQueryExpression(BaseExpression, frozen=True):
    """
    A model representing a sub query expression.
    """

    sub_query: "Criteria" = Field(alias="sub query")


class ValueExpression(BaseExpression, frozen=True):
    """
    A model representing a value expression.
    """

    value: bool | int | float | str | None


class BaseOperator(BaseModel, frozen=True):
    """
    A base model for operators.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    operator: InfixOperators | MixedOperators | PrefixOperators | VerboseOperators

    @field_validator("operator", mode="before")
    @classmethod
    def normalize_values(cls, value: str) -> str:
        """
        Normalize the value.
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

    expressions: Any | Sequence[Any] | None = None
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

    expressions: Any | Sequence[Any] | None = None
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
    def normalize_values(cls, value: str) -> str:
        """
        Normalize the value.
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
    right: Sequence[Any] | Any

    @computed_field
    @property
    def args(self) -> Sequence[Any]:
        """
        Get the function arguments.
        """
        return self.right if isinstance(self.right, list) else [self.right]  # type: ignore


class BaseClause(BaseModel):
    """
    A base model for SQL clauses.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)


class CombineItem(BaseClause):
    """
    A model representing a combine item.
    """

    all: bool = False
    sub_query: "Criteria" = Field(alias="sub query")
    type: Literal["UNION", "INTERSECT", "EXCEPT"] | None = None

    @field_validator("type", mode="before")
    @classmethod
    def normalize_values(cls, value: str | None) -> str | None:
        """
        Normalize the value.
        """

        return value.upper() if value else None

    @computed_field
    @property
    def combine_type(self) -> sql.Composable | None:
        """
        Get the combine type.
        """
        return sql.SQL(f" {self.type} ") if self.type else None


class WithItem(BaseClause):
    """
    A model representing a WITH item.
    """

    columns: Sequence[str] | None = None
    name: str
    materialized: bool | None = None
    recursive: bool = False
    sub_query: "Criteria" = Field(alias="sub query")


class InsertItem(BaseClause):
    """
    A model representing an INSERT item.
    """

    alias: str | None = None
    columns: Sequence[str] | None = None
    table: str


type ExpressionItem = Annotated[
    Annotated[Sequence["ExpressionItem"], Tag("Sequence[ExpressionItem]")]
    | Annotated[None, Tag("none")]
    | Annotated[bool, Tag("bool")]
    | Annotated[int, Tag("int")]
    | Annotated[float, Tag("float")]
    | Annotated[str, Tag("str")]
    | Annotated[ColumnExpression, Tag("column")]
    | Annotated[DefaultExpression, Tag("default")]
    | Annotated[FunctionExpression, Tag("function")]
    | Annotated[OperatorExpression, Tag("operator")]
    | Annotated[SubQueryExpression, Tag("sub query")]
    | Annotated[ValueExpression, Tag("value")],
    Discriminator(get_expression_type),
]


class SelectItem(BaseClause):
    """
    A model representing a SELECT item.
    """

    alias: str | None = None


class UpdateItem(BaseClause):
    """
    A model representing an UPDATE item.
    """

    alias: str | None = None
    set: dict[str, ExpressionItem]
    table: str


class FromItem(BaseClause):
    """
    A model representing a FROM item.
    """

    alias: str | None = None
    on: Sequence[ExpressionItem] | None = None
    sub_query: Union["Criteria", None] = Field(default=None, alias="sub query")
    table: str | None = None
    type: Literal["INNER", "LEFT", "RIGHT", "FULL", "CROSS"] | None = None

    @field_validator("type", mode="before")
    @classmethod
    def normalize_values(cls, value: str | None) -> str | None:
        """
        Normalize the value.
        """

        return value.upper() if value else None

    @computed_field
    @property
    def join_type(self) -> sql.Composable | None:
        """
        Get the join type.
        """
        return sql.SQL(f" {self.type} JOIN ") if self.type else None

    @model_validator(mode="after")
    def check_values(self) -> "FromItem":
        """
        Validate the model fields.
        """

        if self.sub_query is None and self.table is None:
            raise ValueError("FROM item must have either 'table' or 'sub query'")
        return self


class Criteria(BaseModel):
    """
    A model representing SQL criteria.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, populate_by_name=True)

    combine: Sequence[CombineItem] | None = None
    with_: Sequence[WithItem] | None = Field(default=None, alias="with")
    delete: Any | None = None
    insert: InsertItem | None = None
    select: Sequence[Annotated[ExpressionItem, SelectItem]] | None = None
    update: UpdateItem | None = None
    values: Sequence[Sequence[ExpressionItem]] | None = None
    from_: Sequence[FromItem] | None = Field(default=None, alias="from")
    where: Sequence[ExpressionItem] | None = None
    group_by: Sequence[ExpressionItem] | None = Field(default=None, alias="group by")
    having: Sequence[ExpressionItem] | None = None
    order_by: Sequence[ExpressionItem] | None = Field(default=None, alias="order by")
    limit: ExpressionItem | None = None
    offset: ExpressionItem | None = None
    returning: Sequence[ExpressionItem] | None = None

    @model_validator(mode="after")
    def check_values(self) -> "Criteria":
        """
        Validate the model fields.
        """

        for index, combine_item in enumerate(self.combine or []):
            if index < len(self.combine or []) - 1:
                if combine_item.type is None:
                    raise ValueError("Intermediate combine items must have 'type'")
            elif combine_item.type is not None or combine_item.all:
                raise ValueError("Final combine item cannot have 'type' or 'all'")

        for index, from_item in enumerate(self.from_ or []):
            model = FromItem(**from_item.model_dump())
            if index == 0:
                if model.type is not None or model.on is not None:
                    raise ValueError("First FROM item cannot have 'type' or 'on'")
            else:
                if model.type is None:
                    raise ValueError("JOIN items must have 'type'")
                if model.type == "CROSS" and model.on is not None:
                    raise ValueError("CROSS JOIN cannot have 'on' clause")
                if model.type != "CROSS" and model.on is None:
                    raise ValueError("JOIN items must have 'on' clause")

        return self
