import mimetypes
import boto3

from smartpy.utility import os_util


class DigitalOcean:
    def __init__(self, region_name, endpoint_url, key_id, secret_access_key):
        self.region_name = region_name
        self.client = self.get_spaces_client(region_name, endpoint_url, key_id, secret_access_key)

    def get_spaces_client(self, region_name, endpoint_url, key_id, secret_access_key):
        session = boto3.session.Session()
        return session.client(
            's3',
            region_name=region_name,
            endpoint_url=endpoint_url,
            aws_access_key_id=key_id,
            aws_secret_access_key=secret_access_key
        )

    def upload_file(self, project_name, space_name, file_src, save_as=None, is_public=False, content_type=None, meta=None):
        if not content_type:
            file_type_guess = mimetypes.guess_type(file_src)
            if not file_type_guess[0]:
                raise Exception("We can't identify content type. Please specify directly via content_type arg.")
            content_type = file_type_guess[0]

        save_as = save_as or os_util.getBaseName(file_src).replace(" ", "_")

        extra_args = {
            'ACL': "public-read" if is_public else "private",
            'ContentType': content_type
        }

        if isinstance(meta, dict):
            extra_args["Metadata"] = meta

        file = self.client.upload_file(file_src, space_name, save_as, ExtraArgs=extra_args)
        return f"https://{project_name}.{self.region_name}.digitaloceanspaces.com/{space_name}/{save_as}"

