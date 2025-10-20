"""
Query builder for PostgreSQL using psycopg.
"""

from typing import Any
from psycopg import sql
from src import models, parsers


def parse_combine_item(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single combine item.
    """

    model = models.CombineItem(**item)

    statement, values = parsers.parse_expression(item, statement, values)

    if model.combine_type:
        statement += model.combine_type
        if model.all:
            statement += sql.SQL("ALL ")
        else:
            statement += sql.SQL("DISTINCT ")

    return statement, values


def parse_combine(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a combination of queries (e.g., UNION, INTERSECT).
    """

    return parsers.parse_expression(
        items, statement, values, joiner=sql.SQL(""), parser=parse_combine_item
    )


def parse_with_item(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single WITH item.
    """

    model = models.WithItem(**item)

    if model.recursive:
        statement += sql.SQL("RECURSIVE ")

    statement += sql.Identifier(model.name)

    if model.columns:
        statement, values = parsers.parse_column_list(model.columns, statement, values)

    statement += sql.SQL(" AS")

    if model.materialized is not None:
        if model.materialized:
            statement += sql.SQL(" MATERIALIZED")
        else:
            statement += sql.SQL(" NOT MATERIALIZED")

    return parsers.parse_expression(item, statement + sql.SQL(" "), values)


def parse_with(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a WITH clause.
    """

    statement += sql.SQL(" WITH ")
    return parsers.parse_expression(items, statement, values, parser=parse_with_item)


def parse_delete(
    _: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a DELETE statement.
    """

    return statement + sql.SQL(" DELETE "), values


def parse_insert(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an INSERT statement.
    """

    model = models.InsertItem(**item)

    statement += sql.SQL(" INSERT INTO ") + sql.Identifier(model.table)

    if model.alias:
        statement += sql.SQL(" AS ") + sql.Identifier(model.alias)

    if model.columns:
        statement, values = parsers.parse_column_list(model.columns, statement, values)

    return statement, values


def parse_select_item(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single SELECT item.
    """

    model = models.SelectItem(**item)

    statement, values = parsers.parse_expression(item, statement, values)

    if model.alias:
        statement += sql.SQL(" AS ") + sql.Identifier(model.alias)

    return statement, values


def parse_select(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a SELECT statement.
    """

    statement += sql.SQL(" SELECT ")
    return parsers.parse_expression(items, statement, values, parser=parse_select_item)


def parse_update(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an UPDATE statement.
    """

    if "table" not in item:
        raise ValueError("UPDATE statement must have 'table'")

    statement += sql.SQL(" UPDATE ") + sql.Identifier(item["table"])

    if "alias" in item:
        statement += sql.SQL(" AS ") + sql.Identifier(item["alias"])

    if "set" not in item or not isinstance(item["set"], dict):
        raise ValueError("UPDATE statement must have 'set' clause as a dictionary")

    statement += sql.SQL(" SET ")
    for index, (column, value) in enumerate(item["set"].items()):
        if index > 0:
            statement += sql.SQL(", ")
        statement += sql.Identifier(column) + sql.SQL(" = ")
        statement, values = parsers.parse_expression(value, statement, values)

    return statement, values


def parse_values(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a VALUES clause.
    """

    statement += sql.SQL(" VALUES ")
    for index, item in enumerate(items):
        if index > 0:
            statement += sql.SQL(", ")
        statement, values = parsers.parse_expression(item, statement, values, wrap=True)
    return statement, values


def parse_from_item(
    item: Any, statement: sql.Composable, values: list[Any], index: int
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single FROM item.
    """

    if index == 0:
        if "type" in item or "on" in item:
            raise ValueError("First FROM item cannot have 'type' or 'on'")
    else:
        if "type" not in item:
            raise ValueError("JOIN items must have 'type'")

        join_type = item["type"].upper()
        if join_type not in ("INNER", "LEFT", "RIGHT", "FULL", "CROSS"):
            raise ValueError(f"Invalid join type: {join_type}")
        if join_type in ("INNER", "LEFT", "RIGHT", "FULL") and "on" not in item:
            raise ValueError(f"{join_type} JOIN must have 'on' clause")
        if join_type == "CROSS" and "on" in item:
            raise ValueError("CROSS JOIN cannot have 'on' clause")

        statement += sql.SQL(f" {join_type} JOIN ")

    if "sub query" in item:
        statement, values = parsers.parse_expression(item, statement, values)
    elif "table" in item:
        statement += sql.Identifier(item["table"])
    else:
        raise ValueError("Invalid FROM item")

    if "alias" in item:
        statement += sql.SQL(" AS ") + sql.Identifier(item["alias"])

    if "on" in item:
        statement, values = parsers.parse_expression(
            item["on"], statement + sql.SQL(" ON "), values, joiner=sql.SQL(" AND ")
        )

    return statement, values


def parse_from(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a FROM clause.
    """

    statement += sql.SQL(" FROM ")
    for index, item in enumerate(items):
        statement, values = parse_from_item(item, statement, values, index)
    return statement, values


def parse_where(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a WHERE clause.
    """

    statement += sql.SQL(" WHERE ")
    return parsers.parse_expression(items, statement, values, joiner=sql.SQL(" AND "))


def parse_group_by(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a GROUP BY clause.
    """

    statement += sql.SQL(" GROUP BY ")
    return parsers.parse_expression(items, statement, values, joiner=sql.SQL(" AND "))


def parse_having(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a HAVING clause.
    """

    statement += sql.SQL(" HAVING ")
    return parsers.parse_expression(items, statement, values, joiner=sql.SQL(" AND "))


def parse_order_by_item(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a single ORDER BY item.
    """

    direction = "ASC" if "direction" not in item else item["direction"].upper()
    if direction not in ("ASC", "DESC"):
        raise ValueError(f"Invalid ORDER BY direction: {direction}")

    expression = {key: value for key, value in item.items() if key != "direction"}
    statement, values = parsers.parse_expression(expression, statement, values)
    return statement + sql.SQL(f" {direction}"), values


def parse_order_by(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an ORDER BY clause.
    """

    statement += sql.SQL(" ORDER BY ")
    return parsers.parse_expression(
        items, statement, values, parser=parse_order_by_item
    )


def parse_limit(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a LIMIT clause.
    """

    statement += sql.SQL(" LIMIT ")
    return parsers.parse_expression(item, statement, values)


def parse_offset(
    item: Any, statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse an OFFSET clause.
    """

    statement += sql.SQL(" OFFSET ")
    return parsers.parse_expression(item, statement, values)


def parse_returning(
    items: list[Any], statement: sql.Composable, values: list[Any]
) -> tuple[sql.Composable, list[Any]]:
    """
    Parse a RETURNING clause.
    """

    statement += sql.SQL(" RETURNING ")
    return parsers.parse_expression(items, statement, values)


clauses: dict[models.Clauses, models.Parser] = {
    "combine": parse_combine,
    "with": parse_with,
    "delete": parse_delete,
    "insert": parse_insert,
    "select": parse_select,
    "update": parse_update,
    "values": parse_values,
    "from": parse_from,
    "where": parse_where,
    "group by": parse_group_by,
    "having": parse_having,
    "order by": parse_order_by,
    "limit": parse_limit,
    "offset": parse_offset,
    "returning": parse_returning,
}
