"""
Query builder for PostgreSQL using psycopg.
"""

from typing import Any, cast
from psycopg import sql
import src.expression_parser as ep


def parse_combine(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a combination of queries (e.g., UNION, INTERSECT).
    """

    for index, item in enumerate(items):
        statement, values = ep.parse_expression(item, statement, values)

        if index < len(items) - 1:
            if "type" not in item:
                raise ValueError("Intermediate combine items must have 'type'")

            combine_type = item["type"].upper()
            if combine_type not in ("UNION", "INTERSECT", "EXCEPT"):
                raise ValueError(f"Invalid combine type: {combine_type}")

            statement += sql.SQL(f" {combine_type} ")
            if "all" in item and item["all"]:
                statement += sql.SQL("ALL ")
            else:
                statement += sql.SQL("DISTINCT ")

        elif "type" in item or "all" in item:
            raise ValueError("Last combine item cannot have 'type' or 'all'")

    return statement, values


def parse_with_item(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single WITH item.
    """

    if "name" not in item or "sub_query" not in item:
        raise ValueError("WITH item must have 'name' and 'sub_query'")

    if "recursive" in item and item["recursive"]:
        statement += sql.SQL("RECURSIVE ")

    statement += sql.Identifier(item["name"])

    if "columns" in item:
        statement += sql.SQL(" (")
        for index, column in enumerate(item["columns"]):
            if index > 0:
                statement += sql.SQL(", ")
            statement += sql.Identifier(column)
        statement += sql.SQL(")")

    statement += sql.SQL(" AS")

    if "materialized" in item:
        if item["materialized"]:
            statement += sql.SQL(" MATERIALIZED")
        else:
            statement += sql.SQL(" NOT MATERIALIZED")

    return ep.parse_expression(item, statement, values)


def parse_with(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a WITH clause.
    """

    statement += sql.SQL(" WITH ")
    for index, item in enumerate(items):
        if index > 0:
            statement += sql.SQL(", ")
        statement, values = parse_with_item(item, statement, values)
    return statement, values


def parse_select_item(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single SELECT item.
    """

    if "column" in item and item["column"] == "*":
        statement += sql.SQL("*")
    else:
        statement, values = ep.parse_expression(item, statement, values)

    if "alias" in item:
        statement += sql.SQL(" AS ") + sql.Identifier(item["alias"])

    return statement, values


def parse_select(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a SELECT statement.
    """

    statement += sql.SQL(" SELECT ")
    for index, item in enumerate(items):
        if index > 0:
            statement += sql.SQL(", ")
        statement, values = parse_select_item(item, statement, values)
    return statement, values


def parse_from_item(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single FROM item.
    """

    if "subquery" in item:
        statement, values = ep.parse_expression(item, statement, values)
    elif "table" in item:
        statement += sql.Identifier(item["table"])
    else:
        raise ValueError("Invalid FROM item")

    if "alias" in item:
        statement += sql.SQL(" AS ") + sql.Identifier(item["alias"])

    return statement, values


def parse_from(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a FROM clause.
    """

    statement += sql.SQL(" FROM ")
    for index, item in enumerate(items):
        if index > 0:
            statement += sql.SQL(" ")
        statement, values = parse_from_item(item, statement, values)
    return statement, values


def parse_where(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a WHERE clause.
    """

    statement += sql.SQL(" WHERE ")
    for index, item in enumerate(items):
        if index > 0:
            statement += sql.SQL(" AND ")
        statement, values = ep.parse_expression(item, statement, values)
    return statement, values


def parse_group_by(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a GROUP BY clause.
    """

    statement += sql.SQL(" GROUP BY ")
    for index, item in enumerate(items):
        if index > 0:
            statement += sql.SQL(", ")
        statement, values = ep.parse_expression(item, statement, values)
    return statement, values


def parse_having(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a HAVING clause.
    """

    statement += sql.SQL(" HAVING ")
    for index, item in enumerate(items):
        if index > 0:
            statement += sql.SQL(" AND ")
        statement, values = ep.parse_expression(item, statement, values)
    return statement, values


def parse_order_by_item(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single ORDER BY item.
    """

    if "expression" not in item:
        raise ValueError("ORDER BY item must have an expression")

    direction = "ASC" if "direction" not in item else item["direction"].upper()
    if direction not in ("ASC", "DESC"):
        raise ValueError(f"Invalid ORDER BY direction: {direction}")

    statement, values = ep.parse_expression(item["expression"], statement, values)
    statement += sql.SQL(f" {direction}")
    return statement, values


def parse_order_by(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an ORDER BY clause.
    """

    statement += sql.SQL(" ORDER BY ")
    for index, item in enumerate(items):
        if index > 0:
            statement += sql.SQL(", ")
        statement, values = parse_order_by_item(item, statement, values)
    return statement, values


def parse_limit(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a LIMIT clause.
    """

    statement += sql.SQL(" LIMIT ")
    statement, values = ep.parse_expression(item, statement, values)
    return statement, values


def parse_offset(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an OFFSET clause.
    """

    statement += sql.SQL(" OFFSET ")
    statement, values = ep.parse_expression(item, statement, values)
    return statement, values


def build_query(
    criteria: dict[str, Any], values: list[Any] | None = None
) -> tuple[sql.Composed, list[Any]]:
    """
    Build an SQL query from criteria.
    """
    if values is None:
        values = []

    statement: sql.Composable = sql.SQL("")

    if "combine" in criteria:
        statement, values = parse_combine(criteria["combine"], statement, values)

    if "with" in criteria:
        statement, values = parse_with(criteria["with"], statement, values)

    if "select" in criteria:
        statement, values = parse_select(criteria["select"], statement, values)

    if "from" in criteria:
        statement, values = parse_from(criteria["from"], statement, values)

    if "where" in criteria:
        statement, values = parse_where(criteria["where"], statement, values)

    if "group_by" in criteria:
        statement, values = parse_group_by(criteria["group_by"], statement, values)

    if "having" in criteria:
        statement, values = parse_having(criteria["having"], statement, values)

    if "order_by" in criteria:
        statement, values = parse_order_by(criteria["order_by"], statement, values)

    if "limit" in criteria:
        statement, values = parse_limit(criteria["limit"], statement, values)

    if "offset" in criteria:
        statement, values = parse_offset(criteria["offset"], statement, values)

    return cast(sql.Composed, statement), values
