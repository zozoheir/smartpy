import os

from urllib.parse import urlparse
import json
import pickle
import tempfile

import boto3
from s3fs.core import S3FileSystem
from botocore.exceptions import ClientError


class S3:

    def __init__(self,
                 aws_access_key_id,
                 aws_secret_access_key):
        # We need this so setup the proper session locally
        # boto3.setup_default_session(profile_name=profile_name)

        self.session = boto3.session.Session(aws_access_key_id=aws_access_key_id,
                                             aws_secret_access_key=aws_secret_access_key)
        self.boto3_client = self.session.client('s3')
        self.boto3_resource = self.session.resource('s3')
        self.s3fs = S3FileSystem(session=self.session)

    # Pulling
    def downloadFile(self, bucket, key, save_to_path):
        if self.isFile(bucket, key):
            print(f'Downloading to {save_to_path}')
            self.boto3_resource.Bucket(bucket).download_file(key, save_to_path)
        else:
            raise Exception(f"The key {key} doesn't exist")

    def getTempfile(self, bucket, key):
        f = tempfile.NamedTemporaryFile()
        self.boto3_resource.Bucket(bucket).download_file(key, f.name)
        return f

    def getJson(self, bucket, key):
        obj = self.boto3_client.get_object(Bucket=bucket, Key=key)
        content = obj['Body']
        jsonObject = json.loads(content.read())
        return jsonObject

    def getPickleFile(self, bucket, key):
        return pickle.load(self.s3fs.open('{}/{}'.format(bucket, key)))

    # Pushing
    def uploadFile(self, bucket, key, file_path):
        return self.boto3_resource.Bucket(bucket).upload_file(file_path, key)

    def uploadDictAsJson(self, bucket, key, dictionary):
        return self.boto3_client.put_object(Body=str(json.dumps(dictionary)), Bucket=bucket, Key=key)

    def deleteFile(self, bucket, key):
        return self.boto3_client.delete_object(Bucket=bucket, Key=key)

    def deleteFolder(self, bucket, folder):
        self.boto3_resource.Bucket(bucket).objects.filter(Prefix=folder).delete()

    # Utility
    def isFile(self, bucket, key):

        bucket = self.boto3_resource.Bucket(bucket)
        for object_summary in bucket.objects.filter(Prefix=key):
            return True
        return False

    def isFolder(self, bucket_name, path_to_folder):
        try:
            res = self.boto3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=path_to_folder
            )
            return res['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as e:
            # Logic to coin errors.
            raise e

    def getObjectURI(self, bucket, key):
        return f"s3://{bucket}/{key}"

    def listFiles(self, bucket, key):
        """List package in specific S3 URL"""
        bucket = self.boto3_resource.Bucket(bucket)
        files_in_bucket = list(bucket.objects.all())
        return files_in_bucket

    def getBucketKeyFromUri(self, uri):
        parsed_uri = urlparse(uri, allow_fragments=False)
        return parsed_uri.netloc, parsed_uri.path[1:]
