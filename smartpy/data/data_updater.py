import logging
import pandas as pd
import numpy as np

from awswrangler.exceptions import NoFilesFound
from awswrangler import exceptions
from sqlalchemy.exc import ProgrammingError

from smartpy.aws.s3 import S3
from smartpy.data.database import Database
from smartpy.utility.data_util import ParquetUtil

logging.basicConfig(format='%(levelname)s | %(name)s | %(asctime)s : %(message)s ', level=logging.INFO)
logger = logging.getLogger(__name__)


class DataUpdater:

    def __init__(self,
                 id_column_name,
                 data_source='s3',
                 db_object: Database = None,
                 table_name=None,
                 **kwargs):

        self.data_source = data_source

        if data_source in ['s3', 'local_parquet']:
            self.current_source_data = None
        elif data_source == 'sql':
            self.db_object = db_object
            self.table_name = table_name

        self.data_to_input = None
        self.id_column_name = id_column_name
        self.ids_to_upload = []
        self.ids_already_present = []

    def inputNewData(self, data_to_input):
        self.data_to_input = data_to_input

    def getCurrentSourceData(self, **kwargs):

        if self.data_source == 'sql':
            self.current_source_data = self.db_object.query(f'SELECT * FROM {self.table_name}')

    def getIDsAlreadyPresent(self, **kwargs):
        try:
            self.getCurrentSourceData(**kwargs)
            if len(self.current_source_data) > 0:
                self.ids_already_present = self.current_source_data[self.id_column_name]
            else:
                self.ids_already_present = []
        # The read fails when the parquet dir is present but not the desired partition
        # or if file not present
        except (exceptions.NoFilesFound, ProgrammingError, FileNotFoundError):
            self.ids_already_present = []

        self.ids_already_present = [str(i) for i in self.ids_already_present]

    def getIDsToUpload(self):
        current_downloaded_ids = list(self.data_to_input[self.id_column_name].astype(str))
        self.ids_to_upload = np.setdiff1d(current_downloaded_ids, self.ids_already_present)
        self.ids_to_upload = [str(i) for i in self.ids_to_upload]

    def saveNewData(self, **kwargs):
        if len(self.ids_to_upload):
            self.df_to_upload = self.data_to_input[
                self.data_to_input[self.id_column_name].astype(str).isin(self.ids_to_upload)]
            logger.info(f'Uploading {len(self.ids_to_upload)} rows into {self.data_source}')
            if self.data_source == 'sql':
                self.db_object.insert(df=self.df_to_upload,
                                      table=self.table_name,
                                      if_exists='append')
        else:
            logger.info(f'No new data to upload to {self.data_source}')

    def run(self, df):
        self.inputNewData(df)
        self.getIDsAlreadyPresent()
        self.getIDsToUpload()
        self.saveNewData()


class S3DataUpdater(DataUpdater):

    def __init__(self,
                 id_column_name,
                 source_file_path,
                 aws_access_key_id,
                 aws_secret_access_key):

        self.source_file_path = source_file_path
        self.parquet_util = ParquetUtil(data_source='s3',
                                        aws_access_key_id=aws_access_key_id,
                                        aws_secret_access_key=aws_secret_access_key)
        self.s3 = S3(aws_access_key_id=aws_access_key_id,
                     aws_secret_access_key=aws_secret_access_key)

        super().__init__(id_column_name)

    def getCurrentSourceData(self, **kwargs):
        bucket, key = self.s3.getBucketKeyFromUri(self.source_file_path)
        if self.s3.isFile(bucket, key) is True:
            try:
                self.current_source_data = self.parquet_util.readParquet(self.source_file_path,
                                                                         columns=[self.id_column_name],
                                                                         partition_filter=kwargs['partition_filter'])
            except NoFilesFound:
                self.current_source_data = pd.DataFrame()
        else:
            self.current_source_data = pd.DataFrame()

    def saveNewData(self, **kwargs):
        if len(self.ids_to_upload) > 0:
            self.df_to_upload = self.data_to_input[
                self.data_to_input[self.id_column_name].astype(str).isin(self.ids_to_upload)]
            logger.info(f'Uploading {len(self.ids_to_upload)} rows into {self.source_file_path}')
            self.parquet_util.toParquet(df=self.df_to_upload,
                                        file_path=self.source_file_path,
                                        **kwargs)
        else:
            logger.info(f'No data to upload into {self.data_source}')
