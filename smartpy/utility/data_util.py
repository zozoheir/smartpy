import boto3
import awswrangler as wr
import pandas as pd
import pyarrow.parquet as pq
from botocore.client import Config
from smartpy.aws.s3 import S3
import pyarrow.parquet as pq
import pyarrow as pa

wr.config.botocore_config = Config(connect_timeout=50, read_timeout=500)



class DataUtil:

    def __init__(self, profile_name):
        self.boto3_session = boto3.Session(profile_name=profile_name)
        self.s3 = S3(profile_name=profile_name)

    def readParquet(self, file_path, method='pandas'):
        if 's3://' in file_path:
            method = 'aws'

        if method == 'pyarrow':
            df = pq.read_table(file_path).to_pandas()
            return df
        elif method == 'pandas':
            df = pd.read_parquet(file_path, engine='pyarrow')
            return df
        elif method == 'aws':
            df = wr.s3.read_parquet(file_path, boto3_session=self.boto3_session)
            return df
        else:
            raise Exception("Method has to be aws, dask or pandas")


    def toParquet(self, df, save_to_path, method='pandas'):
        if 's3://' in save_to_path:
            method = 'aws'
        if method == 'pandas':
            df.to_parquet(save_to_path, engine='pyarrow')
        elif method == 'aws':
            wr.s3.to_parquet(df=df, path=save_to_path, boto3_session=self.boto3_session)

    def toS3ParquetDataset(self, df, s3_path, **kwargs):
        """
        https://aws-data-wrangler.readthedocs.io/en/2.4.0-docs/stubs/awswrangler.s3.read_parquet.html
        """
        bucket, key = self.s3.getBucketKeyFromUri(s3_path)
        if self.s3.isFile(bucket, key):
            wr.s3.to_parquet(df=df, path=s3_path, boto3_session=self.boto3_session, dataset=True, mode='append', **kwargs)
        else:
            wr.s3.to_parquet(df=df, path=s3_path, boto3_session=self.boto3_session, dataset=True, **kwargs)

    def readS3ParquetDataset(self, s3_path,  **kwargs):
        return wr.s3.read_parquet(s3_path, boto3_session=self.boto3_session, dataset=True,  **kwargs)


    def toParquetDataset(self, df, local_path, **kwargs):
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(table, root_path=local_path)
