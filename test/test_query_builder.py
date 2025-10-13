"""
Unit tests for the query_builder module
"""

import sys
import unittest
import asyncio
from psycopg.rows import dict_row
from src.py_pg import PyPg
import src.query_builder as qb


if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class TestQueryBuilder(unittest.TestCase):
    """
    Test class for query_builder module
    """

    def __init__(self, methodName: str = "") -> None:
        super().__init__(methodName)
        self.pg = PyPg()

    def test_build_query(self):
        """
        Test building a query
        """

        async def test():
            async with await self.pg.connect() as conn:
                await self.pg.replace_table("test_table", conn, name="TEXT")
                await self.pg.list_add_rows(
                    "test_table",
                    conn,
                    {"name": "Test1"},
                    {"name": "Test2"},
                    {"name": "Test3"},
                )

                statement, values = qb.build_statement(
                    {
                        "combine": [
                            {
                                "sub_query": {
                                    "with": [
                                        {
                                            "name": "cte_example",
                                            "columns": ["name"],
                                            "sub_query": {
                                                "select": [{"column": "name"}],
                                                "from": [{"table": "test_table"}],
                                                "where": [
                                                    {
                                                        "left": {"column": "name"},
                                                        "operator": "<>",
                                                        "right": {"value": "A"},
                                                    }
                                                ],
                                            },
                                            "materialized": True,
                                        }
                                    ],
                                    "select": [
                                        {
                                            "function_name": "max",
                                            "args": [
                                                {
                                                    "column": "name2",
                                                    "correlation": "alias_example",
                                                }
                                            ],
                                        },
                                        {
                                            "operator": "+",
                                            "left": {"value": 1},
                                            "right": {"value": 2},
                                            "alias": "add",
                                        },
                                        {
                                            "operator": "-",
                                            "operand": {"value": 1},
                                            "alias": "neg",
                                        },
                                        {
                                            "operator": "*",
                                            "left": {"value": 1},
                                            "right": {"value": 10},
                                            "alias": "mult",
                                        },
                                        {
                                            "operator": "/",
                                            "left": {"value": 10},
                                            "right": {"value": 2},
                                            "alias": "div",
                                        },
                                        {
                                            "function_name": "upper",
                                            "args": [{"column": "name"}],
                                            "alias": "upper",
                                        },
                                        {
                                            "function_name": "trim",
                                            "args": [
                                                {
                                                    "operator": "CAST",
                                                    "expression": {"column": "name"},
                                                    "type": "CHARACTER",
                                                    "varying": True,
                                                }
                                            ],
                                            "alias": "trim_function",
                                        },
                                        {
                                            "operator": "TRIM",
                                            "expression": {"value": "  more spaces  "},
                                            "both": True,
                                            "alias": "trim_operator",
                                        },
                                        {
                                            "operator": "CAST",
                                            "expression": {"value": 5.23},
                                            "type": "DECIMAL",
                                            "precision": 5,
                                            "scale": 2,
                                            "alias": "cast_decimal",
                                        },
                                        {
                                            "sub_query": {
                                                "select": [{"column": "name"}],
                                                "from": [{"table": "test_table"}],
                                                "where": [
                                                    {
                                                        "left": {"column": "name"},
                                                        "operator": "=",
                                                        "right": {"value": "Test1"},
                                                    }
                                                ],
                                            },
                                            "alias": "subquery_example",
                                        },
                                    ],
                                    "from": [
                                        {"table": "cte_example"},
                                        {
                                            "sub_query": {
                                                "select": [
                                                    {
                                                        "value": "Test10",
                                                        "alias": "name2",
                                                    }
                                                ],
                                            },
                                            "alias": "alias_example",
                                            "type": "INNER",
                                            "on": [{"value": True}],
                                        },
                                    ],
                                    "where": [
                                        {
                                            "left": {"column": "name"},
                                            "operator": "=",
                                            "right": {"value": "Test2"},
                                        },
                                        {
                                            "left": {"column": "name"},
                                            "operator": "<>",
                                            "right": {"value": "foo"},
                                        },
                                        {
                                            "operator": "OR",
                                            "expressions": [
                                                {
                                                    "operator": "AND",
                                                    "expressions": [
                                                        {
                                                            "left": {"column": "name"},
                                                            "operator": "=",
                                                            "right": {"value": "foo"},
                                                        },
                                                        {
                                                            "left": {"column": "name"},
                                                            "operator": "<>",
                                                            "right": {"value": "bar"},
                                                        },
                                                    ],
                                                },
                                                {
                                                    "left": {"column": "name"},
                                                    "operator": "!=",
                                                    "right": {"value": "foo"},
                                                },
                                                {
                                                    "left": {"column": "name"},
                                                    "operator": ">",
                                                    "right": {"value": "a"},
                                                },
                                                {
                                                    "left": {"column": "name"},
                                                    "operator": ">=",
                                                    "right": {"value": "Test"},
                                                },
                                            ],
                                        },
                                        {
                                            "left": {"column": "name"},
                                            "operator": "<",
                                            "right": {"value": "Z"},
                                        },
                                        {
                                            "left": {"column": "name"},
                                            "operator": "<=",
                                            "right": {"value": "Test9"},
                                        },
                                        {
                                            "left": {"column": "name"},
                                            "operator": "IN",
                                            "right": [
                                                {"value": "Test1"},
                                                {"value": "Test2"},
                                                {"value": "Test3"},
                                            ],
                                        },
                                        {
                                            "left": {"column": "name"},
                                            "operator": "IN",
                                            "right": [
                                                {
                                                    "sub_query": {
                                                        "select": [{"value": "name"}],
                                                    }
                                                },
                                                {"value": "Test2"},
                                            ],
                                        },
                                    ],
                                    "group_by": [{"column": "name"}],
                                    "having": [
                                        {
                                            "left": {"column": "name"},
                                            "operator": "=",
                                            "right": {"value": "Test2"},
                                        },
                                    ],
                                    "order_by": [
                                        {
                                            "column": "name",
                                            "direction": "ASC",
                                        },
                                        {
                                            "column": "name",
                                            "direction": "DESC",
                                        },
                                    ],
                                    "limit": {"value": 100},
                                    "offset": {"value": 0},
                                },
                                "type": "UNION",
                                "all": False,
                            },
                            {
                                "sub_query": {
                                    "select": [
                                        {"column": "name"},
                                        {"value": 2},
                                        {"value": -3},
                                        {"value": 10},
                                        {"value": 5},
                                        {"value": "TEST"},
                                        {"value": "  spaces  "},
                                        {"value": "  spaces  "},
                                        {"value": 7.89},
                                        {"value": "  spaces  "},
                                    ],
                                    "from": [{"table": "test_table"}],
                                },
                            },
                        ],
                        "limit": {"value": 50},
                        "offset": {"value": 0},
                    }
                )

                async with conn.cursor(row_factory=dict_row) as cur:
                    print(f"\nStatement: {statement.as_string()}\n")
                    print(f"Values: {values}\n")

                    await cur.execute(statement, values)
                    rows = await cur.fetchall()
                    self.assertIsInstance(rows, list)
                    for row in rows:
                        print(f"Row: {row}")

        asyncio.run(test())

    def test_build_insert(self):
        """
        Test building a statement
        """

        async def test():
            async with await self.pg.connect() as conn:
                await self.pg.replace_table("test_table", conn, name="TEXT")

                statement, values = qb.build_statement(
                    {
                        "insert": {
                            "table": "test_table",
                            "alias": "t",
                            "columns": ["name"],
                        },
                        "values": [
                            [{"value": "Test1"}],
                            [{"value": "Test2"}],
                            [{"value": "Test3"}],
                        ],
                        "returning": [{"column": "*"}],
                    }
                )

                async with conn.cursor(row_factory=dict_row) as cur:
                    print(f"\nStatement: {statement.as_string()}\n")
                    print(f"Values: {values}\n")

                    await cur.execute(statement, values)
                    rows = await cur.fetchall()
                    self.assertIsInstance(rows, list)
                    for row in rows:
                        print(f"Row: {row}")

        asyncio.run(test())

    def test_build_update(self):
        """
        Test building an update statement
        """

        async def test():
            async with await self.pg.connect() as conn:
                await self.pg.replace_table("test_table", conn, name="TEXT", age="INT")
                await self.pg.list_add_rows(
                    "test_table",
                    conn,
                    {"name": "Test1", "age": 10},
                    {"name": "Test2", "age": 20},
                    {"name": "Test3", "age": 30},
                )

                statement, values = qb.build_statement(
                    {
                        "update": {
                            "table": "test_table",
                            "alias": "t",
                            "set": {
                                "name": {"value": "UpdatedName"},
                                "age": {"default": True},
                            },
                        },
                        "where": [
                            {
                                "left": {"column": "name"},
                                "operator": "=",
                                "right": {"value": "Test2"},
                            }
                        ],
                        "returning": [{"column": "*"}],
                    }
                )

                async with conn.cursor(row_factory=dict_row) as cur:
                    print(f"\nStatement: {statement.as_string()}\n")
                    print(f"Values: {values}\n")

                    await cur.execute(statement, values)
                    rows = await cur.fetchall()
                    self.assertIsInstance(rows, list)
                    for row in rows:
                        print(f"Row: {row}")

        asyncio.run(test())
