from smartpy.utility.data_util import ParquetUtil
from smartpy.aws.s3 import S3
from smartpy.constants import *
import smartpy.utility.os_util as os_util


s3 = S3(aws_access_key_id=os.environ['AWS_ACCESS_KEY'],
        aws_secret_access_key=os.environ['AWS_SECRET_KEY'])


class S3DataHandler:
    """
    S3DataHandler abstracts away all the nitty gritty of dealing with S3 data. This class allows data I/O both
    on S3 and locally.
    The class uses the local_directory as a mirror of our S3 data. When using getData, the class will return
    the local copy of that sizer_path, if present. Otherwise, it downloads it and keeps it in local_directory for the next call.
    """

    def __init__(self, local_directory, bucket=None, profile_name=None):
        self.local_dir = local_directory
        self.bucket = bucket
        self.data_util = ParquetUtil(profile_name)

    def getData(self, key, method='aws', **kwargs):

        # aws method has highest priority, no matter if local_dir provided or not
        if method == 'aws' or self.local_dir is None:
            s3_uri = f"s3://{self.bucket}/{key}"
            data_to_return = self.data_util.readS3ParquetDataset(s3_uri, **kwargs)


        if self.local_dir and method != 'aws':
            expected_local_file_path = os_util.joinPaths([self.local_dir, self.bucket, key])
            if os_util.fileExists(expected_local_file_path):
                print(f'Returning local sizer_path : {expected_local_file_path}')
                data_to_return = self.data_util.readS3ParquetDataset(expected_local_file_path, **kwargs)
            else:
                print(f'File not present locally')
                self.downloadData(bucket=self.bucket, key=key, save_to_file_path=expected_local_file_path)
                data_to_return = self.data_util.readS3ParquetDataset(expected_local_file_path, **kwargs)
        elif self.local_dir is None and method != 'aws':
            raise Exception('No local directory provided. You need a local sizer_path to use the pandas/pyarrow/dask method')

        return data_to_return

    def downloadData(self, bucket, key, save_to_file_path):
        os_util.ensureDir(save_to_file_path)
        s3.downloadFile(bucket, key, save_to_file_path)

    def pushData(self, s3_uri, df):
        self.data_util.toS3ParquetDataset(df, s3_uri)

    def purgeDirectory(self, patterns: list = ['parquet']):
        files = os_util.walkDir(self.local_dir)
        for file in files:
            for pattern in patterns:
                if pattern in file:
                    os_util.remove(file)
