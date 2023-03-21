import boto3
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, inspect, text

from sqlalchemy.dialects.mysql import insert as insert_mysql
from sqlalchemy.dialects.postgresql import insert as insert_postgre
from sqlalchemy import table, column

from smartpy.utility.log_util import getLogger

logger = getLogger(__name__)


class Database:

    def __init__(self,
                 database_name='db1',
                 endpoint='db1.chtt7vdrgpve.us-east-1.rds.amazonaws.com',
                 username='othmane1',
                 password='Zozoheir3[',
                 port='5432',
                 type='mysql'
                 ):
        self.database_name = database_name
        self.database_type = type

        if type == 'mysql':
            self.connection_string = 'mysql+pymysql://' + username + ':' + password + '@' + endpoint + ':' + str(
                port) + '/' + database_name
        elif type == 'postgres':
            self.connection_string = 'postgresql+psycopg2://' + username + ':' + password + '@' + endpoint + ':' + str(
                port) + '/' + database_name

        self.engine = create_engine(self.connection_string)

    def open(self):
        # Test connection
        try:
            self.engine = create_engine(self.connection_string)
            result = pd.read_sql("SELECT * FROM INFORMATION_SCHEMA.TABLES", self.engine)
        except Exception as e:
            logger.info("Can't open DB")
            raise e

    def close(self):
        self.engine.dispose()

    def query(self, query):
        """
        Exceptions are handled on a case by case basis in the code calling this function.
        :param query:
        :return:
        """
        try:
            return pd.read_sql(query, self.engine).replace('NULL', np.nan)
        except Exception as e:
            self.close()
            raise e

    def getTableNames(self):
        return self.query("SELECT * FROM information_schema.tables WHERE table_schema = '" + self.database_name + "'")[
            'TABLE_NAME'].values

    def upsert(self, df, table_name, primary_key=None, **kwargs):

        data_iter = [tuple(val.values()) for i, val in enumerate(df.to_dict('records'))]
        columns = [column(c) for c in df.columns]
        mytable = table(table_name, *columns)
        if self.database_type == 'mysql':
            insert_stmt = insert_mysql(mytable).values(data_iter)
            on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
            return self.engine.execute(on_duplicate_key_stmt)

        elif self.database_type == 'postgres':
            insert_stmt = text(
                f"INSERT INTO {table_name} ({','.join(df.columns)}) VALUES ({','.join([':' + col for col in df.columns])}) ON CONFLICT ({','.join(primary_key)}) DO UPDATE SET {','.join([f'{col}=excluded.{col}' for col in df.columns if col not in primary_key])}")

            # execute the SQL statement for each row in the dataframe
            for row in df.itertuples(index=False):
                self.engine.execute(insert_stmt, **row._asdict())

        logger.info(f"Upserting {len(data_iter)} rows into {table_name}")


    def insert(self, df, table_name, **kwargs):
        """
        Exceptions are handled on a case by case basis in the code calling this function.
        :param query:
        :return:
        """
        try:
            df.to_sql(name=table_name,
                      con=self.engine,
                      index=False,
                      chunksize=5000,
                      method='multi',
                      **kwargs)
            self.close()
        except Exception as e:
            self.close()
            raise e


def getDBHostURL(region_name, database_name):
    """
    Returns the URL of the DB host. This is used as an input when querying/inserting into a table_name
    :param database_name:
    :return:
    """
    instances = boto3.client('rds', region_name=region_name).describe_db_instances(
        DBInstanceIdentifier=database_name)
    rds_host = instances.get('DBInstances')[0].get('Endpoint').get('Address')
    return rds_host

def getSQLFromList(py_list_to_sql, column_type):
    if column_type == int:
        return "(" + ','.join([str(i) for i in list(py_list_to_sql)]) + ")"
    elif column_type == str:
        return "(" + ','.join([f"'{i}'" for i in list(py_list_to_sql)]) + ")"

