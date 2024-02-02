import json
import os

from contextlib import contextmanager, asynccontextmanager
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from sqlalchemy.exc import OperationalError, TimeoutError, DisconnectionError, DatabaseError


DB_RETRIES = 0 if 'prod' not in os.environ['TINYLLM_CONFIG_PATH'] else 3
WAIT_SEC = 2

class PostgresDB:

    @retry(
        reraise=True,
        stop=stop_after_attempt(DB_RETRIES),
        wait=wait_fixed(WAIT_SEC),
        retry=retry_if_exception_type((OperationalError, TimeoutError, DisconnectionError, DatabaseError))
    )
    def __init__(self, username, password, host, port, db_name, sslmode=None):
        self.db_uri = f'postgresql://{username}:{password}@{host}:{port}/{db_name}' + (
            f'?sslmode={sslmode}' if sslmode else '')
        self.engine = create_engine(self.db_uri)
        self.sync_session_maker = sessionmaker(bind=self.engine)
        async_db_uri = f'postgresql+asyncpg://{username}:{password}@{host}:{port}/{db_name}' + (
            f'?sslmode={sslmode}' if sslmode else '')
        self.async_engine = create_async_engine(async_db_uri)
        self.async_session_maker = sessionmaker(
            bind=self.async_engine,
            expire_on_commit=False,
            class_=AsyncSession,
        )

    @retry(
        reraise=True,
        stop=stop_after_attempt(DB_RETRIES),
        wait=wait_fixed(WAIT_SEC),
        retry=retry_if_exception_type((OperationalError, TimeoutError, DisconnectionError, DatabaseError))
    )
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.sync_session_maker()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @retry(
        reraise=True,
        stop=stop_after_attempt(DB_RETRIES),
        wait=wait_fixed(WAIT_SEC),
        retry=retry_if_exception_type((OperationalError, TimeoutError, DisconnectionError, DatabaseError))
    )
    @asynccontextmanager
    async def async_session_scope(self):
        """Provide a transactional scope around a series of operations for asynchronous sessions."""
        session = self.async_session_maker()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    @retry(
        reraise=True,
        stop=stop_after_attempt(DB_RETRIES),
        wait=wait_fixed(WAIT_SEC),
        retry=retry_if_exception_type((OperationalError, TimeoutError, DisconnectionError, DatabaseError))
    )
    def read(self, query, params={}, as_dict=False):
        with self.session_scope() as session:
            result = session.execute(text(query), params).fetchall()
            if as_dict:
                result = [r._asdict() for r in result]
                return result
            else:
                return result

    @retry(
        reraise=True,
        stop=stop_after_attempt(DB_RETRIES),
        wait=wait_fixed(WAIT_SEC),
        retry=retry_if_exception_type((OperationalError, TimeoutError, DisconnectionError, DatabaseError))
    )
    async def async_read(self, query: str, params={}):
        async with self.async_session_scope() as session:
            result = await session.execute(text(query), params)
            return result.fetchall()

    @retry(
        reraise=True,
        stop=stop_after_attempt(DB_RETRIES),
        wait=wait_fixed(WAIT_SEC),
        retry=retry_if_exception_type((OperationalError, TimeoutError, DisconnectionError, DatabaseError))
    )
    def write(self, query, params={}):
        # Make params into list and execute as list
        if isinstance(params, dict):
            params = [params]

        if isinstance(query, str):
            query = [query]

        with self.session_scope() as session:
            for query, params in zip(query, params):
                result = session.execute(text(query), params)
        return result

    @retry(
        reraise=True,
        stop=stop_after_attempt(DB_RETRIES),
        wait=wait_fixed(WAIT_SEC),
        retry=retry_if_exception_type((OperationalError, TimeoutError, DisconnectionError, DatabaseError))
    )
    async def async_write(self, query: str, params={}):
        async with self.async_session_scope() as session:
            result = await session.execute(text(query), params)
            return result


    def insert(self, table_name, rows, on_conflict="do nothing"):
        if len(rows) == 0:
            return None, None
        query, params = self._get_upsert_query(table_name, rows, on_conflict)
        return self.write(query, params)

    async def async_insert(self, table_name, rows, on_conflict="do nothing"):
        if len(rows) == 0:
            return None, None
        query, params = self._get_upsert_query(table_name, rows, on_conflict)
        result = await self.async_write(query, params)
        return result

    def _get_upsert_query(self, table_name, rows, on_conflict="do nothing"):
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

        params = {f'{col}{i}': row.get(col, None) for i, row in enumerate(rows) for col in columns}
        return query, params
