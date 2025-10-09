"""
Sample code for postgreSQL database connection and operations.
"""

import asyncio
from os import environ
import sys
from typing import cast, Any, AsyncGenerator, AsyncContextManager
import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import DictRow, dict_row
from psycopg import AsyncConnection, sql
from psycopg_pool import AsyncConnectionPool

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class PyPg:
    """
    Tries out different PostgreSQL database operations using asyncpg.
    """

    def __init__(self):
        self.db_host: str = environ.get("DB_HOST", "localhost")
        self.db_name: str = environ.get("DB_NAME", "postgres")
        self.db_password: str = environ.get("DB_PASSWORD", "postgres")
        self.db_user: str = environ.get("DB_USER", "postgres")
        self.pool = AsyncConnectionPool(
            open=False,
            conninfo=make_conninfo(
                dbname=self.db_name,
                host=self.db_host,
                password=self.db_password,
                user=self.db_user,
            ),
        )

    def parse_expression(
        self,
        expression: Any,
        /,
        statement: sql.Composed | None = None,
        values: list[Any] | None = None,
    ) -> tuple[sql.Composed, list[Any]]:
        """
        Parses an expression into a SQL fragment.
        A statement, if provided is appended to.
        Values for placeholders are appended to the values list.
        """

        if statement is None:
            statement = sql.SQL("").format()
        assert statement is not None

        if values is None:
            values = []
        assert values is not None

        if isinstance(expression, str):
            statement += sql.SQL(" ") + sql.Literal(expression)
        elif isinstance(expression, list):
            for item in cast(list[Any], expression):
                statement, values = self.parse_expression(item, statement, values)
        elif isinstance(expression, dict):
            for key, value in cast(dict[str, Any], expression).items():
                statement += sql.SQL(" ").join(
                    [
                        sql.Identifier(key),
                        sql.SQL("="),
                        sql.Placeholder(),
                    ]
                )
                values.append(value)

        return statement, values

    def parse_criteria(
        self,
        /,
        statement: sql.Composed | None = None,
        values: list[Any] | None = None,
        **criteria: Any,
    ) -> tuple[sql.Composed, list[Any]]:
        """
        Parses criteria into a SQL WHERE clause.
        A statement, if provided is appended to.
        Values for placeholders are appended to the values list.
        """

        if statement is None:
            statement = sql.SQL("").format()
        assert statement is not None

        if values is None:
            values = []
        assert values is not None

        if "select" in criteria:
            statement += sql.SQL(" SELECT ")
            if criteria["select"] == "*" or criteria["select"] is None:
                statement += sql.SQL("*")
            elif isinstance(criteria["select"], list):
                statement += sql.SQL(", ").join(
                    map(sql.Identifier, cast(list[str], criteria["select"]))
                )
            elif isinstance(criteria["select"], dict):
                statement += sql.SQL(", ").join(
                    [
                        sql.SQL(" ").join(
                            [
                                sql.Identifier(col),
                                sql.SQL("AS"),
                                sql.Identifier(alias),
                            ]
                        )
                        for col, alias in cast(
                            dict[str, str], criteria["select"]
                        ).items()
                    ]
                )

        if "from" in criteria:
            statement += sql.SQL(" FROM ") + sql.Identifier(criteria["from"])

        if "where" in criteria:
            statement += sql.SQL(" WHERE ")
            for index, [col, value] in enumerate(criteria["where"].items()):
                values.append(value)
                if index > 0:
                    statement += sql.SQL(" AND ")
                statement += sql.SQL(" ").join(
                    [
                        sql.Identifier(col),
                        sql.SQL("="),
                        sql.Placeholder(),
                    ]
                )

        if "order_by" in criteria:
            statement += sql.SQL(" ").join(
                [sql.SQL(" ORDER BY "), sql.SQL(criteria["order_by"])]
            )

        if "limit" in criteria:
            statement += sql.SQL(" ").join(
                [sql.SQL(" LIMIT "), sql.Literal(criteria["limit"])]
            )

        return statement, values

    async def connect(self) -> AsyncContextManager[AsyncConnection]:
        """
        Opens the connection pool (if necessary) and returns a connection to the database.
        """

        await self.pool.open()
        return self.pool.connection()

    async def create_table(
        self, table: str, conn: AsyncConnection | None = None, /, **columns: str
    ) -> None:
        """
        Create a table in the PostgreSQL database.
        Standard columns 'id', 'created_at', and 'updated_at' are added automatically.
        """

        parts: list[sql.Composed] = [
            sql.SQL(" ").join([sql.Identifier(col), sql.SQL(data_type)])  # type: ignore
            for col, data_type in columns.items()
        ]
        statement = sql.SQL(
            """
            CREATE TABLE {table} (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP,
                {columns}
            )
            """
        ).format(
            table=sql.Identifier(table),
            columns=sql.SQL(", ").join(parts),
        )
        try:
            if conn is None:
                async with await self.connect() as conn:
                    await conn.execute(statement)
            else:
                await conn.execute(statement)
            print("Table created successfully.")
        except psycopg.Error as e:
            print(f"Error creating table: {e}")
            raise e

    async def drop_table(
        self, table: str, conn: AsyncConnection | None = None, /
    ) -> None:
        """
        Drops a table from the PostgreSQL database.
        """

        statement = sql.SQL("DROP TABLE IF EXISTS {table} CASCADE").format(
            table=sql.Identifier(table)
        )
        try:
            if conn is None:
                async with await self.connect() as conn:
                    await conn.execute(statement)
            else:
                await conn.execute(statement)
            print("Table dropped successfully.")
        except psycopg.Error as e:
            print(f"Error dropping table: {e}")
            raise e

    async def replace_table(
        self, table: str, conn: AsyncConnection | None = None, /, **columns: str
    ) -> None:
        """
        Replaces a table in the PostgreSQL database.
        If the table exists, it is dropped and recreated.
        """

        async def replace(conn: AsyncConnection) -> None:
            await self.drop_table(table, conn)
            await self.create_table(table, conn, **columns)

        if conn is None:
            async with await self.connect() as conn:
                await replace(conn)
        else:
            await replace(conn)

    async def add_row(
        self, table: str, conn: AsyncConnection | None = None, /, **values: Any
    ) -> DictRow:
        """
        Adds a row to a table in the PostgreSQL database.
        The row is yielded after insertion.
        """

        async def add(conn: AsyncConnection) -> DictRow:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(statement, tuple(values.values()))
                row = await cur.fetchone()
                assert row is not None
                return row

        statement = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({placeholders}) RETURNING *"
        ).format(
            table=sql.Identifier(table),
            fields=sql.SQL(", ").join(map(sql.Identifier, values.keys())),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(values)),
        )
        try:
            row: DictRow | None = None
            if conn is None:
                async with await self.connect() as conn:
                    row = await add(conn)
            else:
                row = await add(conn)
            print("Inserted row successfully.")
            return row
        except psycopg.Error as e:
            print(f"Error inserting row: {e}")
            raise e

    async def iter_add_rows(
        self, table: str, conn: AsyncConnection | None = None, /, *rows: dict[str, Any]
    ) -> AsyncGenerator[DictRow]:
        """
        Adds multiple rows to a table in the PostgreSQL database.
        Each row is yielded after insertion.
        """

        if conn is None:
            async with await self.connect() as conn:
                for row in rows:
                    yield await self.add_row(table, conn, **row)
        else:
            for row in rows:
                yield await self.add_row(table, conn, **row)

    async def list_add_rows(
        self, table: str, conn: AsyncConnection | None = None, /, *rows: dict[str, Any]
    ) -> list[DictRow]:
        """
        Adds many rows to a table in the PostgreSQL database.
        All rows are returned as a list after insertion.
        """

        return [row async for row in self.iter_add_rows(table, conn, *rows)]

    async def get_rows(
        self,
        table: str,
        conn: AsyncConnection | None = None,
        /,
        columns: list[str] | None = None,
        **criteria: Any,
    ) -> AsyncGenerator[DictRow]:
        """
        Iterates over rows in a table from the PostgreSQL database.
        """

        statement, values = self.parse_criteria(
            **criteria | {"select": columns or "*", "from": table}
        )
        try:
            count = 0
            if conn is None:
                async with await self.connect() as conn, conn.cursor(
                    row_factory=dict_row
                ) as cur:
                    async for row in cur.stream(statement, values):
                        yield row
                        count += 1
            else:
                async with conn.cursor(row_factory=dict_row) as cur:
                    async for row in cur.stream(statement, values):
                        yield row
                        count += 1
            print(f"Retrieved {count} row(s) successfully.")
        except psycopg.Error as e:
            print(f"Error retrieving rows: {e}")
            raise e

    async def list_get_rows(
        self,
        table: str,
        conn: AsyncConnection | None = None,
        /,
        columns: list[str] | None = None,
        **criteria: Any,
    ) -> list[DictRow]:
        """
        Retrieves all rows from a specified table.
        """
        return [row async for row in self.get_rows(table, conn, columns, **criteria)]

    async def delete_rows(
        self, table: str, conn: AsyncConnection | None = None, /, **criteria: Any
    ) -> AsyncGenerator[DictRow]:
        """
        Deletes rows based on specified conditions.
        """

        values: list[Any] = []
        statement: sql.Composed = sql.SQL("DELETE FROM {table}").format(
            table=sql.Identifier(table)
        )
        statement, values = self.parse_criteria(statement, values, **criteria)
        statement += sql.SQL(" RETURNING *")
        try:
            count = 0
            if conn is None:
                async with await self.connect() as conn, conn.cursor(
                    row_factory=dict_row
                ) as cur:
                    async for row in cur.stream(statement, values):
                        yield row
                        count += 1
            else:
                async with conn.cursor(row_factory=dict_row) as cur:
                    async for row in cur.stream(statement, values):
                        yield row
                        count += 1
            print(f"Deleted {count} row(s) successfully.")
        except psycopg.Error as e:
            print(f"Error deleting rows: {e}")
            raise e

    async def delete_many_rows(
        self, table: str, conn: AsyncConnection | None = None, /, **criteria: Any
    ) -> list[DictRow]:
        """
        Deletes all rows from a specified table.
        """

        return [row async for row in self.delete_rows(table, conn, **criteria)]

    async def exists_row(
        self, table: str, conn: AsyncConnection | None = None, /, **criteria: Any
    ) -> bool:
        """
        Checks if a row exists based on specified conditions.
        """

        async def exists(conn: AsyncConnection) -> bool:
            async with conn.cursor() as cur:
                await cur.execute(statement, values)
                result = await cur.fetchone()
                return result is not None

        values: list[Any] = []
        statement: sql.Composed = sql.SQL("SELECT 1 FROM {table}").format(
            table=sql.Identifier(table)
        )
        statement, values = self.parse_criteria(statement, values, **criteria, limit=1)
        try:
            result: bool = False
            if conn is None:
                async with await self.connect() as conn:
                    result = await exists(conn)
            else:
                result = await exists(conn)
            print(f"Row existence check: {result}.")
            return result
        except psycopg.Error as e:
            print(f"Error checking row existence: {e}")
            raise e

    async def truncate_table(
        self, table: str, conn: AsyncConnection | None = None, /
    ) -> None:
        """
        Truncates a specified table, removing all rows.
        """

        statement = sql.SQL("TRUNCATE TABLE {table} CASCADE").format(
            table=sql.Identifier(table)
        )
        try:
            if conn is None:
                async with await self.connect() as conn:
                    await conn.execute(statement)
            else:
                await conn.execute(statement)
            print("Table truncated successfully.")
        except psycopg.Error as e:
            print(f"Error truncating table: {e}")
            raise e

    async def update_row(
        self,
        table: str,
        identity: Any,
        conn: AsyncConnection | None = None,
        /,
        columns: list[str] | None = None,
        **values: Any,
    ) -> DictRow:
        """
        Updates a row in a specified table.
        The row is yielded after updating.
        """

        async def update(conn: AsyncConnection) -> DictRow:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(statement, values | {"id": identity})
                row = await cur.fetchone()
                assert row is not None
                return row

        assignments: list[sql.Composed] = [
            sql.SQL(" ").join([sql.Identifier(col), sql.SQL("="), sql.Placeholder(col)])
            for col in values
            if col != "id"
        ]
        statement = sql.SQL(
            "UPDATE {table} SET {assignments} WHERE id = %(id)s RETURNING {fields}"
        ).format(
            table=sql.Identifier(table),
            assignments=sql.SQL(", ").join(assignments),
            fields=(
                sql.SQL(", ").join(map(sql.Identifier, columns))
                if columns
                else sql.SQL("*")
            ),
        )
        try:
            row = DictRow
            if conn is None:
                async with await self.connect() as conn:
                    row = await update(conn)
            else:
                row = await update(conn)
            print("Updated row successfully.")
            return row
        except psycopg.Error as e:
            print(f"Error updating row: {e}")
            raise e

    async def update_many_rows(
        self,
        table: str,
        conn: AsyncConnection | None = None,
        /,
        *rows: dict[str, Any],
        columns: list[str] | None = None,
    ) -> list[DictRow]:
        """
        Updates many rows in a specified table.
        """

        return [
            await self.update_row(table, row["id"], conn, columns, **row)
            for row in rows
        ]
