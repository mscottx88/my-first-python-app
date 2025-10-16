"""
Query builder for PostgreSQL using psycopg.
"""

from typing import Any, cast
from psycopg import sql
from src import models
from src.clauses import clauses


def build_statement(
    criteria: dict[str, Any],
    statement: sql.Composable | None = None,
    values: list[Any] | None = None,
    *,
    wrap: bool = False,
) -> tuple[sql.Composed, list[Any]]:
    """
    Build an SQL statement from criteria.

    Parameters:
        criteria (dict[str, Any]): The criteria for building the SQL statement.
        statement (sql.Composable | None): The initial SQL statement.
        values (list[Any] | None): The initial list of values for parameterized queries.
        wrap (bool): Whether to wrap the entire statement in parentheses.

    Returns:
        tuple[sql.Composed, list[Any]]: The final SQL statement and values list.
    """

    model = models.Criteria(**criteria).model_dump(by_alias=True)

    if statement is None:
        statement = sql.SQL("")
    elif wrap:
        statement += sql.SQL("(")

    if values is None:
        values = []

    for key, parser in clauses.items():
        if key in model and model[key] is not None:
            statement, values = parser(model[key], statement, values)

    if wrap:
        statement += sql.SQL(")")

    return cast(sql.Composed, statement), values
