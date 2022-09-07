import awswrangler as aws_wrangler
import pyarrow.parquet as pq
import pyarrow as pa
from botocore.client import Config

from smartpy.aws.s3 import S3

aws_wrangler.config.botocore_config = Config(connect_timeout=50, read_timeout=500)


class ParquetUtil:

    def __init__(self,
                 data_source,
                 aws_access_key_id=None,
                 aws_secret_access_key=None):
        self.data_source = data_source
        if data_source == 's3':
            self.s3 = S3(aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)
        else:
            self.data_source = data_source

    def readParquet(self, file_path, **kwargs):
        if self.data_source == 's3':
            return aws_wrangler.s3.read_parquet(file_path,
                                                boto3_session=self.s3.session,
                                                dataset=True,
                                                use_threads=True,
                                                **kwargs)
        else:
            return pq.read_table(file_path, **kwargs).to_pandas()

    def toParquet(self, df, file_path, **kwargs):
        if self.data_source == 's3':
            aws_wrangler.s3.to_parquet(df=df,
                                       path=file_path,
                                       boto3_session=self.s3.session,
                                       dataset=True,
                                       mode='append',
                                       **kwargs)
        else:
            table = pa.Table.from_pandas(df)
            return pq.write_to_dataset(table,
                                       root_path=file_path,
                                       **kwargs)