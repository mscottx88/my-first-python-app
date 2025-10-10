"""
Unit tests for the MyPg module
"""

import sys
import unittest
import asyncio
import asyncstdlib as a
from psycopg.rows import dict_row
from src.py_pg import PyPg


if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class TestMyPg(unittest.TestCase):
    """
    Test class for MyPg module
    """

    def __init__(self, methodName: str = "") -> None:
        super().__init__(methodName)
        self.pg = PyPg()

    def test_create_table(self):
        """
        Test creating a table
        """

        async def test():
            async with (
                await self.pg.connect() as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await self.pg.replace_table("test_table", conn, name="TEXT")
                await cur.execute("SELECT to_regclass('public.test_table')")
                row = await cur.fetchone()
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["to_regclass"], "test_table")

        asyncio.run(test())

    def test_drop_table(self):
        """
        Test dropping a table
        """

        async def test():
            async with (
                await self.pg.connect() as conn,
                conn.cursor(row_factory=dict_row) as cur,
            ):
                await self.pg.replace_table("test_table", conn, name="TEXT")
                await self.pg.drop_table("test_table", conn)
                await cur.execute("SELECT to_regclass('public.test_table')")
                row = await cur.fetchone()
                self.assertIsNotNone(row)
                assert row is not None
                self.assertIsNone(row["to_regclass"])

        asyncio.run(test())

    def test_add_row(self):
        """
        Test adding a row to the table
        """

        async def test():
            async with await self.pg.connect() as conn:
                await self.pg.replace_table("test_table", conn, name="TEXT")
                row = await self.pg.add_row("test_table", conn, name="Test")
                self.assertEqual(row["id"], 1)

        asyncio.run(test())

    def test_get_rows(self):
        """
        Test getting rows from the table
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
                async for i, row in a.enumerate(
                    self.pg.get_rows("test_table", conn, order_by=[{"column": "name"}])
                ):
                    self.assertEqual(row["name"], f"Test{i+1}")

        asyncio.run(test())

    def test_delete_rows(self):
        """
        Test deleting rows from the table
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
                async for row in self.pg.delete_rows(
                    "test_table",
                    conn,
                    where=[
                        {
                            "left": {"column": "name"},
                            "operator": "=",
                            "right": {"value": "Test2"},
                        }
                    ],
                ):
                    self.assertEqual(row["name"], "Test2")
                rows = await self.pg.list_get_rows("test_table", conn)
                self.assertEqual(len(rows), 2)
                self.assertEqual(rows[0]["name"], "Test1")
                self.assertEqual(rows[1]["name"], "Test3")

        asyncio.run(test())

    def test_exists_row(self):
        """
        Test checking for the existence of a row in the table
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
                exists = await self.pg.exists_row("test_table", conn, name="Test2")
                self.assertTrue(exists)

        asyncio.run(test())

    def test_truncate_table(self):
        """
        Test truncating a table
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
                rows = await self.pg.list_get_rows("test_table", conn)
                self.assertEqual(len(rows), 3)
                await self.pg.truncate_table("test_table", conn)
                rows = await self.pg.list_get_rows("test_table", conn)
                self.assertEqual(len(rows), 0)

        asyncio.run(test())

    def test_update_row(self):
        """
        Test updating a row in the table
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
                row = await self.pg.update_row(
                    "test_table", 2, conn, columns=["name"], name="UpdatedTest2"
                )
                self.assertIsNotNone(row)
                assert row is not None
                self.assertEqual(row["name"], "UpdatedTest2")

        asyncio.run(test())


if __name__ == "__main__":
    unittest.main()
