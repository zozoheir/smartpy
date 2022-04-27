from smartpy.aws.s3 import S3
import smartpy.utility.data_util as data_util
import numpy as np
from awswrangler import exceptions
from sqlalchemy.exc import ProgrammingError
import logging

from smartpy.data.database import Database

logging.basicConfig(format='%(levelname)s | %(name)s | %(asctime)s : %(message)s ', level=logging.INFO)
logger = logging.getLogger(__name__)

s3 = S3()


class DataUpdater:

    def __init__(self, id_column_name,
                 source_file_path=None,
                 data_source='s3',
                 session_name=None,
                 db_object: Database = None,
                 table_name=None):

        self.source_file_path = source_file_path
        self.data_source = data_source
        self.datautil = data_util.DataUtil(session_name)
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
        if self.data_source == 's3':
            current_source_data = self.datautil.readS3ParquetDataset(s3_path=self.source_file_path,
                                                                     columns=[self.id_column_name],
                                                                     **kwargs)
        elif self.data_source == 'sql':
            current_source_data = self.db_object.query(f'SELECT * FROM {self.table_name}')
        elif self.data_source == 'local_parquet':
            current_source_data = self.datautil.readParquet(file_path=self.source_file_path)

        self.current_source_data = current_source_data

    def getIDsAlreadyPresent(self, **kwargs):
        try:
            self.getCurrentSourceData(**kwargs)
            if len(self.current_source_data)>0:
                self.ids_already_present = self.current_source_data[self.id_column_name]
            else:
                self.ids_already_present = []
        # The read fails when the parquet dir is present but not the desired partition
        except (exceptions.NoFilesFound, ProgrammingError, FileNotFoundError):
            self.ids_already_present = []

        self.ids_already_present = [str(i) for i in self.ids_already_present]

    def getIDsToUpload(self):
        current_downloaded_ids = self.data_to_input[self.id_column_name].astype(str)
        self.ids_to_upload = np.setdiff1d(current_downloaded_ids, self.ids_already_present)
        self.ids_to_upload = [str(i) for i in self.ids_to_upload]

    def saveNewData(self, **kwargs):
        if len(self.ids_to_upload):
            self.df_to_upload = self.data_to_input[
                self.data_to_input[self.id_column_name].astype(str).isin(self.ids_to_upload)]
            logger.info(f'Uploading {len(self.ids_to_upload)} rows into {self.data_source}')
            if self.data_source == 's3':
                self.datautil.toS3ParquetDataset(self.df_to_upload,
                                                 s3_path=self.source_file_path,
                                                 **kwargs)
            elif self.data_source == 'sql':
                self.db_object.insert(df=self.df_to_upload,
                                      table=self.table_name,
                                      if_exists='append')
            elif self.data_source == 'local_parquet':
                self.datautil.toParquetDataset(df=self.df_to_upload,
                                               local_path=self.source_file_path)

        else:
            logger.info(f'No new data to upload to {self.data_source}')

    def run(self, df):
        self.inputNewData(df)
        self.getIDsAlreadyPresent()
        self.getIDsToUpload()
        self.saveNewData()
