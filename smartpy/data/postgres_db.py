import json

from contextlib import contextmanager, asynccontextmanager
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession


class PostgresDB:
    def __init__(self, username, password, host, port, db_name):
        db_uri = f'postgresql://{username}:{password}@{host}:{port}/{db_name}'
        self.engine = create_engine(db_uri)
        self.sync_session_maker = sessionmaker(bind=self.engine)

        async_db_uri = f'postgresql+asyncpg://{username}:{password}@{host}:{port}/{db_name}'
        self.async_engine = create_async_engine(async_db_uri)
        self.async_session_maker = sessionmaker(
            bind=self.async_engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.sync_session_maker()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

    @asynccontextmanager
    async def async_session_scope(self):
        """Provide a transactional scope around a series of operations for asynchronous sessions."""
        session = self.async_session_maker()
        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise
        finally:
            await session.close()

    def write(self, query, params={}):
        with self.session_scope() as session:
            result = session.execute(text(query), params)
            session.commit()
        return result

    def read(self, query, params={}):
        with self.session_scope() as session:
            result = session.execute(text(query), params).fetchall()
        return result

    async def async_read(self, query: str, params={}):
        async with self.async_session_scope() as session:
            result = await session.execute(text(query), params)
            return result.fetchall()

    async def async_write(self, query: str, params={}):
        async with self.async_session_scope() as session:
            await session.execute(text(query), params)
            await session.commit()

    def insert(self, table_name, rows, on_conflict="do nothing"):
        if not rows:
            return

        # Extract columns from the first row
        columns = rows[0].keys()

        # Handle JSON serialization for dictionary values
        for row in rows:
            for key, value in row.items():
                if isinstance(value, dict):
                    row[key] = json.dumps(value)

        # Construct unique placeholders for parameterized query
        unique_placeholders = [
            '(' + ', '.join([f':{col}{i}' for col in columns]) + ')'
            for i, _ in enumerate(rows)
        ]
        values_placeholders = ', '.join(unique_placeholders)

        # Base query
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)}) 
            VALUES {values_placeholders}
        """
        if on_conflict == "do nothing":
            query += " ON CONFLICT DO NOTHING"
        elif on_conflict == "update":
            update_columns = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'primary_key_column'])
            query += f" ON CONFLICT (primary_key_column) DO UPDATE SET {update_columns}"
        elif on_conflict != "raise":
            raise ValueError("Invalid on_conflict option")

        params = {f'{col}{i}': row[col] for i, row in enumerate(rows) for col in columns}
        return self.write(query, params)

    def upsert(self, table_name, rows, conflict_target):
        if not rows:
            return
        columns = rows[0].keys()
        for row in rows:
            for key, value in row.items():
                if isinstance(value, dict):
                    row[key] = json.dumps(value)

        unique_placeholders = [
            '(' + ', '.join([f':{col}{i}' for col in columns]) + ')'
            for i, _ in enumerate(rows)
        ]
        values_placeholders = ', '.join(unique_placeholders)

        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)}) 
            VALUES {values_placeholders}
            ON CONFLICT ({', '.join(conflict_target)})
            DO UPDATE SET
                """ + ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col not in conflict_target]) + """
            RETURNING *;
            """

        params = {f'{col}{i}': row[col] for i, row in enumerate(rows) for col in columns}

        return self.write(query, params)


    async def async_upsert(self, table_name, rows, conflict_target):
        if not rows:
            return
        columns = rows[0].keys()
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)}) VALUES 
            """ + ",".join(
            ["(" + ",".join([f"%({k}{i})s" for k in columns]) + ")" for i, _ in enumerate(rows)]) + f"""
            ON CONFLICT ({conflict_target})
            DO UPDATE SET
                """ + ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != conflict_target]) + """
            RETURNING *;
            """
        params = {k + str(i): v for i, row in enumerate(rows) for k, v in row.items()}
        result = await self.async_write(query, params)
        return result
