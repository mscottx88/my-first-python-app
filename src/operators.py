"""
This module contains functions to parse various SQL operators.
It is used by the expression parser module to handle operators in expressions.
Each function constructs the appropriate SQL syntax for the operator and
its operands, and appends it to the current SQL statement.
"""

from typing import Callable, LiteralString, cast, Any
from psycopg import sql
from src import models, parsers


def parse_infix_operator(
    statement: sql.Composable,
    values: list[Any],
    **kwargs: Any,
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an infix operator.
    Any generic infix operator can be parsed with this function e.g. =, <, >, etc
    """

    model = models.InfixOperator(**kwargs)

    expressions = model.expressions or [model.left, model.right]
    return parsers.parse_expression(
        expressions, statement, values, joiner=model.sql_operator, wrap=model.wrap
    )


def parse_prefix_operator(
    statement: sql.Composable,
    values: list[Any],
    **kwargs: Any,
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a prefix operator.
    """

    model = models.PrefixOperator(**kwargs)

    statement += model.sql_operator
    return parsers.parse_expression(model.operand, statement, values)


def parse_mixed_operator(
    statement: sql.Composable,
    values: list[Any],
    /,
    **kwargs: Any,
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a mixed operator.
    """

    if ("left" in kwargs and "right" in kwargs) or "expressions" in kwargs:
        return parse_infix_operator(statement, values, **kwargs)

    operand = kwargs["operand"] if "operand" in kwargs else kwargs["left"]
    if operand is not None:
        return parse_prefix_operator(statement, values, **kwargs | {"operand": operand})

    raise ValueError(
        "Operator requires 'operand', 'left', and 'right' or 'expressions' arguments"
    )


def parse_between_operator(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a BETWEEN operator.
    """

    model = models.BetweenOperator(**kwargs)

    statement, values = parsers.parse_expression(model.expression, statement, values)
    statement += model.sql_operator
    if model.symmetric:
        statement += sql.SQL("SYMMETRIC ")
    statement, values = parsers.parse_expression(model.left, statement, values)
    statement += sql.SQL(" AND ")
    return parsers.parse_expression(model.right, statement, values)


def parse_cast_operator(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a CAST operator.
    """

    model = models.CastOperator(**kwargs)

    statement += sql.SQL("CAST(")
    statement, values = parsers.parse_expression(model.expression, statement, values)
    statement += model.type_name

    if model.interval:
        statement += sql.SQL(model.interval)

    if model.varying:
        statement += sql.SQL(" VARYING")

    length = model.length or model.precision
    if length:
        statement += sql.SQL(cast(LiteralString, f"({length}"))
        if model.scale:
            statement += sql.SQL(cast(LiteralString, f", {model.scale}"))
        statement += sql.SQL(")")

    if model.with_time_zone is not None:
        if model.with_time_zone:
            statement += sql.SQL(" WITH TIME ZONE")
        else:
            statement += sql.SQL(" WITHOUT TIME ZONE")

    return statement + sql.SQL(")"), values


def parse_in_operator(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an IN operator.
    """

    model = models.InOperator(**kwargs)

    statement, values = parsers.parse_expression(model.left, statement, values)
    return parsers.parse_function(
        statement, values, function_name="IN", args=model.args, pad=True
    )


def parse_is_operator(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an IS operator.
    """

    if "left" not in kwargs:
        raise ValueError("IS operator requires 'left' argument")

    if "right" not in kwargs:
        right = "UNKNOWN"
    elif isinstance(kwargs["right"], bool):
        right = "TRUE" if kwargs["right"] else "FALSE"
    elif kwargs["right"] is None:
        right = "NULL"
    else:
        raise ValueError(f"Unsupported IS operator right-hand side: {kwargs['right']}")

    statement, values = parsers.parse_expression(kwargs["left"], statement, values)
    return statement + sql.SQL(f" IS {right}"), values


def parse_trim_operator(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a TRIM operator.
    """

    if "expression" not in kwargs:
        raise ValueError("TRIM operator requires 'expression' argument")

    statement += sql.SQL("TRIM(")
    if "both" in kwargs and kwargs["both"]:
        statement += sql.SQL("BOTH ")
    elif "trailing" in kwargs and kwargs["trailing"]:
        statement += sql.SQL("TRAILING ")
    elif "leading" in kwargs and kwargs["leading"]:
        statement += sql.SQL("LEADING ")

    if "characters" in kwargs:
        statement, values = parsers.parse_expression(
            kwargs["characters"], statement, values
        )
        statement += sql.SQL(" ")

    statement += sql.SQL("FROM ")
    statement, values = parsers.parse_expression(
        kwargs["expression"], statement, values
    )
    return statement + sql.SQL(")"), values


operators: dict[models.VerboseOperators, Callable[..., Any]] = {
    "BETWEEN": parse_between_operator,
    "CAST": parse_cast_operator,
    "IN": parse_in_operator,
    "IS": parse_is_operator,
    "TRIM": parse_trim_operator,
}
