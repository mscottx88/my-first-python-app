"""
Expression parser for SQL statements.

This module shares a mutually recursive relationship with the query builder module.
A query can itself contain many expressions, which can also be sub-queries.
The query builder entry point constructs the query from the current top-level dictionary,
and if there are any "sub query" values, the parse_expression function invokes the query builder
to construct the sub-query recursively.
"""

from operator import itemgetter
from typing import Any, Sequence, cast, get_args
from psycopg import sql
from src import models, operators as op
import src.query_builder as qb


def parse_expression_list(
    expressions: list[Any],
    statement: sql.Composable,
    values: list[Any],
    *,
    joiner: sql.Composable | None = None,
    parser: models.Parser | None = None,
    wrap: bool = False,
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a list of expressions.

    Parameters:
        expressions (list[Any]): List of expressions to parse.
        statement (sql.Composable): The current SQL statement being built.
        values (list[Any]): The list of values for parameterized queries.
        joiner (sql.Composable | None): SQL to join expressions (e.g., ", " or " AND ").
        parser (models.Parser | None): Optional custom parser function for expressions.
        wrap (bool): Whether to wrap the entire expression list in parentheses.

    Returns:
        tuple[sql.Composable, list[Any]]: Updated SQL statement and values list.
    """

    if wrap:
        statement += sql.SQL("(")
    for index, expression in enumerate(expressions):
        if index > 0:
            statement += joiner if joiner else sql.SQL(", ")
        statement, values = (
            parser(expression, statement, values)
            if parser
            else parse_expression(expression, statement, values)
        )
    if wrap:
        statement += sql.SQL(")")
    return statement, values


def parse_column_list(
    columns: Sequence[str], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a list of column references.

    Parameters:
        columns (list[str]): List of column names.
        statement (sql.Composable): The current SQL statement being built.
        values (list[Any]): The list of values for parameterized queries.

    Returns:
        tuple[sql.Composable, list[Any]]: Updated SQL statement and values list.
    """

    statement += sql.SQL(" (")
    for index, column in enumerate(columns):
        if index > 0:
            statement += sql.SQL(", ")
        statement += sql.Identifier(column)
    statement += sql.SQL(")")
    return statement, values


def parse_column(
    statement: sql.Composable, values: list[Any], **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a column reference.

    Parameters:
        statement (sql.Composable): The current SQL statement being built.
        values (list[Any]): The list of values for parameterized queries.
        kwargs (dict[str, Any]): Additional keyword arguments.

    Returns:
        tuple[sql.Composable, list[Any]]: Updated SQL statement and values list.
    """

    model = models.ColumnExpression(**kwargs)

    wrap = kwargs.get("wrap", False)
    if wrap:
        statement += sql.SQL("(")
    if model.correlation:
        statement += sql.Identifier(model.correlation) + sql.SQL(".")
    statement += sql.SQL("*") if model.column == "*" else sql.Identifier(model.column)
    if wrap:
        statement += sql.SQL(")")

    return statement, values


def parse_function(
    statement: sql.Composable, values: list[Any], **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a function call.

    Parameters:
        statement (sql.Composable): The current SQL statement being built.
        values (list[Any]): The list of values for parameterized queries.
        kwargs (dict[str, Any]): Additional keyword arguments.

    Returns:
        tuple[sql.Composable, list[Any]]: Updated SQL statement and values list.
    """

    model = models.FunctionExpression(**kwargs)

    if model.schema_name:
        statement += sql.Identifier(model.schema_name) + sql.SQL(".")

    if model.pad:
        statement += sql.SQL(f" {model.function_name} ")
    else:
        statement += sql.SQL(model.function_name)

    return parse_expression(kwargs.get("args", []), statement, values, wrap=True)


def parse_operator(
    statement: sql.Composable, values: list[Any], **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an operator.

    Parameters:
        statement (sql.Composable): The current SQL statement being built.
        values (list[Any]): The list of values for parameterized queries.
        kwargs (dict[str, Any]): Additional keyword arguments.

    Returns:
        tuple[sql.Composable, list[Any]]: Updated SQL statement and values list.
    """

    (operator) = itemgetter("operator")(models.BaseOperator(**kwargs).model_dump())
    match operator:
        case _ if operator in get_args(models.MixedOperators):
            return op.parse_mixed_operator(statement, values, **kwargs)
        case _ if operator in get_args(models.PrefixOperators):
            return op.parse_prefix_operator(statement, values, **kwargs)
        case _ if operator in get_args(models.InfixOperators):
            return op.parse_infix_operator(
                statement, values, **kwargs, wrap=operator in ("AND", "OR")
            )
        case _ if operator in op.operators:
            return op.operators[operator](statement, values, **kwargs)
        case _:
            raise ValueError(f"Unsupported operator: {operator}")


def parse_value(
    statement: sql.Composable, values: list[Any], **kwargs: Any
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a literal value.

    Parameters:
        statement (sql.Composable): The current SQL statement being built.
        values (list[Any]): The list of values for parameterized queries.
        kwargs (dict[str, Any]): Additional keyword arguments.

    Returns:
        tuple[sql.Composable, list[Any]]: Updated SQL statement and values list.
    """

    model = models.ValueExpression(**kwargs)

    if model.value is None:
        statement += sql.Placeholder()
        values.append(None)
        return statement, values

    if isinstance(model.value, bool):
        statement += sql.Placeholder()
        values.append(bool(model.value))
        return statement, values

    if isinstance(model.value, (int, float)):
        statement += sql.Placeholder()
        values.append(model.value)
        return statement, values

    statement += sql.Placeholder()
    values.append(model.value)
    return statement, values


def parse_expression(
    expression: Any,
    statement: sql.Composable,
    values: list[Any],
    *,
    joiner: sql.Composable | None = None,
    parser: models.Parser | None = None,
    wrap: bool = False,
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single expression.

    Parameters:
        expression (list[dict[str, Any]] | dict[str, Any]): The expression to parse.
        statement (sql.Composable): The current SQL statement being built.
        values (list[Any]): The list of values for parameterized queries.
        joiner (sql.Composable | None): SQL to join expressions (e.g., ", " or " AND ").
        parser (models.Parser | None): Optional custom parser function for expressions.
        wrap (bool): Whether to wrap the entire expression in parentheses.

    Returns:
        tuple[sql.Composable, list[Any]]: Updated SQL statement and values list.
    """

    if isinstance(expression, list):
        return parse_expression_list(
            cast(list[dict[str, Any]], expression),
            statement,
            values,
            joiner=joiner,
            parser=parser,
            wrap=wrap,
        )

    match models.get_expression_type(expression):
        case "column":
            return parse_column(statement, values, **expression)
        case "default":
            return statement + sql.SQL("DEFAULT"), values
        case "function":
            return parse_function(statement, values, **expression)
        case "operator":
            return parse_operator(statement, values, **expression)
        case "sub query":
            return qb.build_statement(
                expression["sub query"], statement, values, wrap=True
            )
        case "value":
            return parse_value(statement, values, **expression)
        case _:
            raise ValueError(f"Unsupported expression type: {expression}")
