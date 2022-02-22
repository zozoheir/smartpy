import boto3
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError
import logging

##

class Database:

    def __init__(self,
                 database_name='db1',
                 endpoint='db1.chtt7vdrgpve.us-east-1.rds.amazonaws.com',
                 username='othmane1',
                 password='Zozoheir3[',
                 port='5432',
                 ):
        self.connection_string = 'mysql+pymysql://' + username + ':' + password + '@' + endpoint + ':' + str(
            port) + '/' + database_name
        self.database_name = database_name


    def open(self):
        # Test connection
        try:
            self.engine = create_engine(self.connection_string)
            result = pd.read_sql("SELECT * FROM INFORMATION_SCHEMA.TABLES", self.engine)
        except Exception as e:
            print("Can't open DB")
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


    def insert(self, df, table, **kwargs):
        """
        Exceptions are handled on a case by case basis in the code calling this function.
        :param query:
        :return:
        """
        try:
            df.to_sql(name=table, con=self.engine, index=False,  **kwargs)
            self.close()
        except Exception as e:
            self.close()
            raise e

    def initializeTables(self, tables_model):
        """
        This function checks if the tables supposed to be in the database are there.
        If they are not, it creates them. This avoids bugs when querying an emtpy database,
        like after creating a new environment, or after deleting a table by accident
        :param self:
        :return:
        """

        getEmptyDataFrame = lambda cols: pd.DataFrame({col: [None] for col in cols})
        empty_database_tables_from_model = {key: getEmptyDataFrame(tables_model[key]) for key in
                                            tables_model.keys()}

        # Detect tables that do not already exist
        absent_tables = []
        for t in empty_database_tables_from_model.keys():
            table_name = (self.database_name + '.' + t)
            try:
                pd.read_sql(f'SELECT * FROM {table_name}', self.engine)
            except Exception as e:
                if type(e) == ProgrammingError and e.orig.args[1] == f"Table '{table_name}' doesn't exist":
                    absent_tables.append(t)

        # Create absent tables
        for table in tables_model.keys():
            print(table)
            if table in absent_tables:
                print(f"Initializing {table}")
                empty_database_tables_from_model[table].to_sql(table, self.engine, index=False)


def getDBHostURL(region_name, database_name):
    """
    Returns the URL of the DB host. This is used as an input when querying/inserting into a table
    :param database_name:
    :return:
    """
    instances = boto3.client('rds', region_name=region_name).describe_db_instances(
        DBInstanceIdentifier=database_name)
    rds_host = instances.get('DBInstances')[0].get('Endpoint').get('Address')
    return rds_host
