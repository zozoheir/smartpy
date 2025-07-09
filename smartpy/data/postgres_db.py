import json
import os
import socket
import uuid
from contextlib import contextmanager, asynccontextmanager
from datetime import datetime
from decimal import Decimal

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, select
from sqlalchemy.exc import (OperationalError, TimeoutError, DisconnectionError,
                            DatabaseError, DBAPIError)
from psycopg2 import OperationalError as Psycopg2OperationalError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from smartpy.utility.log_util import getLogger

DB_RETRIES = 3
WAIT_SEC = 2

logger = getLogger(__name__)

exceptions = (socket.gaierror, OperationalError, TimeoutError, DisconnectionError,
              DatabaseError, DBAPIError, Psycopg2OperationalError)


class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.int32):
            return int(obj)
        elif isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, uuid.UUID):
            return str(obj)
        return super().default(obj)


class PostgresDB:

    @retry(reraise=True, stop=stop_after_attempt(DB_RETRIES), wait=wait_fixed(WAIT_SEC),
           retry=retry_if_exception_type(exceptions))
    def __init__(self, username, password, host, port, db_name, sslmode=None):
        self.db_uri = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}'
        if sslmode:
            self.db_uri += f'?sslmode={sslmode}'

        self.engine = create_engine(self.db_uri)
        self.sync_session_maker = sessionmaker(bind=self.engine)

        async_db_uri = f'postgresql+asyncpg://{username}:{password}@{host}:{port}/{db_name}'
        if sslmode:
            async_db_uri += f'?sslmode={sslmode}'

        self.async_engine = create_async_engine(async_db_uri)
        self.async_session_maker = sessionmaker(bind=self.async_engine,
                                                expire_on_commit=False,
                                                class_=AsyncSession)

    @retry(
        reraise=True,
        stop=stop_after_attempt(DB_RETRIES),
        wait=wait_fixed(WAIT_SEC),
        retry=retry_if_exception_type(exceptions)
    )
    def connect(self):
        self.engine.connect()


    @contextmanager
    def session_scope(self):
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

    @asynccontextmanager
    async def async_session_scope(self):
        session = self.async_session_maker()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()

    def read(self, query, params=None, session=None):
        if params is None:
            params = {}

        if session is None:
            with self.session_scope() as session:
                return self._execute_read(session, query, params)
        else:
            return self._execute_read(session, query, params)

    async def async_read(self, query, params=None, session=None):
        if params is None:
            params = {}

        if session is None:
            async with self.async_session_scope() as session:
                return await self._execute_async_read(session, query, params)
        else:
            return await self._execute_async_read(session, query, params)

    def _execute_read(self, session, query, params):
        result = session.execute(text(query), params).fetchall()
        return [r._asdict() for r in result]

    async def _execute_async_read(self, session, query, params):
        result = await session.execute(text(query), params)
        return [r._asdict() for r in result.fetchall()]

    def write(self, query, params=None, session=None):
        if params is None:
            params = {}
        if isinstance(params, dict):
            params = [params]
        if isinstance(query, str):
            query = [query]

        if session is None:
            with self.session_scope() as session:
                return self._execute_write(session, query, params)
        else:
            return self._execute_write(session, query, params)

    async def async_write(self, query, params=None, session=None):
        if params is None:
            params = {}
        if session is None:
            async with self.async_session_scope() as session:
                return await session.execute(text(query), params)
        else:
            return await session.execute(text(query), params)

    def _execute_write(self, session, query, params):
        result = None
        for q, p in zip(query, params):
            result = session.execute(text(q), p)
        return result

    def insert(self, table_name, rows, pk_key='id', on_conflict="do nothing", uuid_cols=[], session=None,on_conflict_cols=[]):
        if not rows:
            return None
        query, params = self._get_upsert_query(table_name, rows, pk_key, on_conflict, uuid_cols, on_conflict_cols=on_conflict_cols)
        return self.write(query, params, session=session)

    async def async_insert(self, table_name, rows, pk_key='id', on_conflict="do nothing", uuid_cols=[], session=None):
        if not rows:
            return None
        query, params = self._get_upsert_query(table_name, rows, pk_key, on_conflict, uuid_cols)
        return await self.async_write(query, params, session=session)

    def _get_upsert_query(self, table_name, rows, pk_key, on_conflict="do nothing", uuid_cols=[], on_conflict_cols=[]):
        if not rows:
            raise ValueError("The 'rows' list cannot be empty")

        columns = list(rows[0].keys())
        values_placeholders = []
        params = {}

        for i, row in enumerate(rows):
            placeholder = []
            for col in columns:
                param_key = f"{col}{i}"
                placeholder.append(f":{param_key}")
                value = row.get(col, None)
                if isinstance(value, dict):
                    params[param_key] = json.dumps(value, cls=CustomEncoder)
                elif col in uuid_cols and value is not None:
                    params[param_key] = uuid.UUID(value)
                else:
                    params[param_key] = value
            values_placeholders.append(f"({', '.join(placeholder)})")

        values_placeholders_str = ', '.join(values_placeholders)

        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES {values_placeholders_str}
        """

        if on_conflict == "do nothing":
            query += " ON CONFLICT DO NOTHING"
        elif on_conflict == "update":
            conflict_target = ', '.join(on_conflict_cols) if on_conflict_cols else pk_key
            update_columns = ', '.join(
                [f"{col} = EXCLUDED.{col}" for col in columns if col not in on_conflict_cols and col != pk_key])
            query += f" ON CONFLICT ({conflict_target}) DO UPDATE SET {update_columns}"
        elif on_conflict != "raise":
            raise ValueError("Invalid on_conflict option")

        return query, params


    def format_uuid_list(self, uuid_list):
        return ','.join([f"'{id}'::uuid" for id in uuid_list])
