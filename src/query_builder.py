"""
Query builder for PostgreSQL using psycopg.
"""

from typing import Any, cast
from psycopg import sql
from src.clauses import clauses


def build_statement(
    criteria: dict[str, Any],
    statement: sql.Composable | None = None,
    values: list[Any] | None = None,
    *,
    wrap: bool = False,
) -> tuple[sql.Composed, list[Any]]:
    """
    Build an SQL query from criteria.
    """

    if statement is None:
        statement = sql.SQL("")
    elif wrap:
        statement += sql.SQL("(")

    if values is None:
        values = []

    for key, parser in clauses.items():
        if key in criteria:
            statement, values = parser(criteria[key], statement, values)

    if wrap:
        statement += sql.SQL(")")

    return cast(sql.Composed, statement), values
