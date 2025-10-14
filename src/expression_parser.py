"""
Expression parser for SQL statements.
"""

from typing import Callable, LiteralString, cast, Any
from psycopg import sql
import src.query_builder as qb


def parse_expression_list(
    expressions: list[Any],
    statement: sql.Composable,
    values: list[Any],
    /,
    joiner: sql.Composable | None = None,
    parser: (
        Callable[[Any, sql.Composable, list[Any]], tuple[sql.Composable, list[Any]]]
        | None
    ) = None,
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a list of expressions.
    """

    for index, expression in enumerate(expressions):
        if index > 0:
            statement += joiner if joiner else sql.SQL(", ")
        statement, values = (
            parser(expression, statement, values)
            if parser
            else parse_expression(expression, statement, values)
        )
    return statement, values


def parse_column_list(
    columns: list[str], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a list of column references.
    """

    statement += sql.SQL(" (")
    for index, column in enumerate(columns):
        if index > 0:
            statement += sql.SQL(", ")
        statement += sql.Identifier(column)
    statement += sql.SQL(")")
    return statement, values


def parse_column(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a column reference.
    """

    if "column" not in kwargs:
        raise ValueError("Column reference requires 'column' argument")

    if "correlation" in kwargs:
        statement += sql.Identifier(kwargs["correlation"]) + sql.SQL(".")

    if kwargs["column"] == "*":
        statement += sql.SQL("*")
    else:
        statement += sql.Identifier(kwargs["column"])

    return statement, values


def parse_function(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a function call.
    """

    if "function_name" not in kwargs:
        raise ValueError("Function call requires 'function_name' argument")

    function_name = kwargs["function_name"].upper()
    if function_name not in ("MAX", "TRIM", "UPPER"):
        raise ValueError(f"Unsupported function: {function_name}")

    if "schema_name" in kwargs:
        statement += sql.SQL(".") + sql.Identifier(kwargs["schema_name"])
    statement += sql.SQL(function_name)

    statement += sql.SQL("(")
    statement, values = parse_expression(kwargs.get("args", []), statement, values)
    return statement + sql.SQL(")"), values


def parse_infix_operator(
    operator: sql.Composable,
    statement: sql.Composable,
    values: list[Any],
    /,
    **kwargs: Any,
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a binary operator.
    """

    if not ("left" in kwargs and "right" in kwargs):
        raise ValueError(f"Operator '{operator}' requires 'left' and 'right' arguments")

    statement, values = parse_expression(kwargs["left"], statement, values)
    statement += operator
    return parse_expression(kwargs["right"], statement, values)


def parse_prefix_operator(
    operator: sql.Composable,
    statement: sql.Composable,
    values: list[Any],
    /,
    **kwargs: Any,
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a unary operator.
    """

    if "operand" not in kwargs:
        raise ValueError(f"Operator '{operator}' requires 'operand' argument")

    statement += operator
    return parse_expression(kwargs["operand"], statement, values)


def parse_mixed_operator(
    operator: sql.Composable,
    statement: sql.Composable,
    values: list[Any],
    /,
    **kwargs: Any,
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a mixed operator.
    """

    if "left" in kwargs and "right" in kwargs:
        return parse_infix_operator(operator, statement, values, **kwargs)

    operand = kwargs["operand"] if "operand" in kwargs else kwargs["left"]
    if operand is not None:
        return parse_prefix_operator(
            operator, statement, values, **kwargs | {"operand": operand}
        )

    raise ValueError(
        f"Operator '{operator}' requires 'operand', 'left', and/or 'right' arguments"
    )


def parse_operator_and(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an AND operator.
    """

    if "expressions" not in kwargs:
        raise ValueError("AND operator requires 'expressions' argument")

    statement += sql.SQL("(")
    statement, values = parse_expression(
        kwargs["expressions"], statement, values, sql.SQL(" AND ")
    )
    statement += sql.SQL(")")
    return statement, values


def parse_operator_cast(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a CAST operator.
    """

    if not ("expression" in kwargs and "type" in kwargs):
        raise ValueError("CAST operator requires 'expression' and 'type' arguments")

    type_name = kwargs["type"].upper()
    if type_name not in (
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
    ):
        raise ValueError(f"Unsupported CAST type: {type_name}")

    statement += sql.SQL("CAST(")
    statement, values = parse_expression(kwargs["expression"], statement, values)
    statement += sql.SQL(f" AS {type_name}")

    if "interval" in kwargs:
        interval = kwargs["interval"].upper()
        if interval not in (
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
        ):
            raise ValueError(f"Unsupported INTERVAL type: {interval}")

        statement += sql.SQL(interval)

    if "varying" in kwargs and kwargs["varying"]:
        statement += sql.SQL(" VARYING")

    if "length" in kwargs and isinstance(kwargs["length"], int):
        statement += sql.SQL(cast(LiteralString, f"({kwargs['length']})"))
    elif "precision" in kwargs and isinstance(kwargs["precision"], int):
        statement += sql.SQL(cast(LiteralString, f"({kwargs['precision']}"))
        if "scale" in kwargs and isinstance(kwargs["scale"], int):
            statement += sql.SQL(cast(LiteralString, f", {kwargs['scale']}"))
        statement += sql.SQL(")")

    if "with_time_zone" in kwargs and kwargs["with_time_zone"]:
        statement += sql.SQL(" WITH TIME ZONE")

    return statement + sql.SQL(")"), values


def parse_operator_in(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an IN operator.
    """

    if not ("left" in kwargs and "right" in kwargs):
        raise ValueError("IN operator requires 'left' and 'right' arguments")

    if not isinstance(kwargs["right"], list):
        raise ValueError("IN operator 'right' argument must be a list")

    statement, values = parse_expression(kwargs["left"], statement, values)
    statement += sql.SQL(" IN (")
    statement, values = parse_expression(
        cast(list[Any], kwargs["right"]), statement, values
    )
    return statement + sql.SQL(")"), values


def parse_operator_or(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an OR operator.
    """

    if "expressions" not in kwargs:
        raise ValueError("AND operator requires 'expressions' argument")

    statement += sql.SQL("(")
    statement, values = parse_expression(
        kwargs["expressions"], statement, values, sql.SQL(" OR ")
    )
    statement += sql.SQL(")")
    return statement, values


def parse_operator_trim(
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
        statement, values = parse_expression(kwargs["characters"], statement, values)
        statement += sql.SQL(" ")

    statement += sql.SQL("FROM ")
    statement, values = parse_expression(kwargs["expression"], statement, values)
    return statement + sql.SQL(")"), values


def parse_operator(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an operator.
    """

    if "operator" not in kwargs:
        raise ValueError("Operator requires 'operator' argument")

    operator = kwargs["operator"].upper()
    match operator:
        case "=" | "!=" | "<>" | ">" | "<" | ">=" | "<=" | "*" | "/" | "%":
            return parse_infix_operator(
                sql.SQL(f" {operator} "), statement, values, **kwargs
            )
        case "+" | "-":
            return parse_mixed_operator(
                sql.SQL(f" {operator} "), statement, values, **kwargs
            )
        case "AND":
            return parse_operator_and(statement, values, **kwargs)
        case "CAST":
            return parse_operator_cast(statement, values, **kwargs)
        case "IN":
            return parse_operator_in(statement, values, **kwargs)
        case "OR":
            return parse_operator_or(statement, values, **kwargs)
        case "TRIM":
            return parse_operator_trim(statement, values, **kwargs)
        case _:
            raise ValueError(f"Unsupported operator: {operator}")


def parse_value(
    statement: sql.Composable, values: list[Any], /, **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a literal value.
    """

    if "value" not in kwargs:
        raise ValueError("Value requires 'value' argument")

    value = kwargs["value"]

    if value is None:
        statement += sql.Placeholder()
        values.append(None)
        return statement, values

    if isinstance(value, bool):
        statement += sql.Placeholder()
        values.append(bool(value))
        return statement, values

    if isinstance(value, (int, float)):
        statement += sql.Placeholder()
        values.append(value)
        return statement, values

    if isinstance(value, str):
        statement += sql.Placeholder()
        values.append(value)
        return statement, values

    raise ValueError(f"Unsupported value type: {type(value)}")


def parse_expression(
    expression: list[dict[str, Any]] | dict[str, Any],
    statement: sql.Composable,
    values: list[Any],
    /,
    joiner: sql.Composable | None = None,
    parser: (
        Callable[[Any, sql.Composable, list[Any]], tuple[sql.Composable, list[Any]]]
        | None
    ) = None,
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single expression.
    """

    if isinstance(expression, list):
        return parse_expression_list(expression, statement, values, joiner, parser)

    if "column" in expression:
        return parse_column(statement, values, **expression)

    if "function_name" in expression:
        return parse_function(statement, values, **expression)

    if "operator" in expression:
        return parse_operator(statement, values, **expression)

    if "value" in expression:
        return parse_value(statement, values, **expression)

    if "default" in expression and expression["default"]:
        return statement + sql.SQL("DEFAULT"), values

    if "sub_query" in expression:
        sub_query, values = qb.build_statement(expression["sub_query"], values)
        statement += sql.SQL("(") + sub_query + sql.SQL(")")
        return statement, values

    raise ValueError("Invalid expression")
