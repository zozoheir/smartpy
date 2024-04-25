import json
import os
import uuid

from contextlib import contextmanager, asynccontextmanager
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from sqlalchemy.exc import OperationalError, TimeoutError, DisconnectionError, DatabaseError

from smartpy.utility.log_util import getLogger
from smartpy.utility.py_util import get_unique

DB_RETRIES = 0 if 'prod' not in os.environ['TINYLLM_CONFIG_PATH'] else 3
WAIT_SEC = 2

logger = getLogger(__name__)


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
        except Exception as e:
            session.rollback()
            logger.error(f"Error in session scope: {e}")
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
    async def async_read(self, query: str, params={}, as_dict=False):
        async with self.async_session_scope() as session:
            result = await session.execute(text(query), params)
            result = result.fetchall()
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
    def write(self, query, params={}):
        # Make params into list and execute as list
        if isinstance(params, dict):
            params = [params]

        if isinstance(query, str):
            query = [query]

        with self.session_scope() as session:
            for query_, param in zip(query, params):
                result = session.execute(text(query_), param)

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

    def insert(self, table_name, rows, pk_key='id', on_conflict="do nothing", uuid_cols=[]):
        if len(rows) == 0:
            return None, None
        query, params = self._get_upsert_query(table_name, rows, pk_key, on_conflict, uuid_cols)
        cursor_result = self.write(query, params)
        return cursor_result

    async def async_insert(self, table_name, rows, on_conflict="do nothing"):
        if len(rows) == 0:
            return None, None
        query, params = self._get_upsert_query(table_name, rows, on_conflict)
        result = await self.async_write(query, params)
        return result

    def _get_upsert_query(self,
                          table_name,
                          rows,
                          pk_key,
                          on_conflict="do nothing",
                          uuid_cols=[]):
        # Check that rows is not empty
        if not rows:
            raise ValueError("The 'rows' list cannot be empty")

        # Extract columns from the first row
        columns = list(rows[0].keys())

        # Handle JSON serialization for dictionary values
        for row in rows:
            for key, value in row.items():
                if isinstance(value, dict):
                    row[key] = json.dumps(value)

        # Create parameterized placeholders and parameters dictionary
        values_placeholders = []
        params = {}
        for i, row in enumerate(rows):
            placeholder = []
            for col in columns:
                param_key = f"{col}{i}"
                placeholder.append(f":{param_key}")
                if col in uuid_cols and row.get(col, None) is not None:
                    params[param_key] = uuid.UUID(row[col][0])
                else:
                    params[param_key] = row.get(col, None)

            values_placeholders.append(f"({', '.join(placeholder)})")

        values_placeholders_str = ', '.join(values_placeholders)

        # Construct base SQL query using parameterized placeholders
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES {values_placeholders_str}
        """

        # Handling ON CONFLICT scenarios
        if on_conflict == "do nothing":
            query += " ON CONFLICT DO NOTHING"
        elif on_conflict == "update":
            update_columns = ', '.join([
                f"{col} = EXCLUDED.{col}" for col in columns if col != pk_key
            ])
            query += f" ON CONFLICT ({pk_key}) DO UPDATE SET {update_columns}"
        elif on_conflict != "raise":
            raise ValueError("Invalid on_conflict option")

        return query, params

    def format_uuid_list(self, uuid_list):
        return ','.join([f"'{id}'::uuid" for id in uuid_list])
