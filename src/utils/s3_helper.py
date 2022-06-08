# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""s3 helper class."""
import io
import mimetypes
import os
import json
from typing import Dict, List, Tuple, Union
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError


class S3Client(object):
    """Helper Class for S3 operations."""

    def __init__(self):
        """
        Initialize the S3 client.
        :return:

        """
        session = boto3.Session(region_name=os.environ.get('AWS_REGION'))
        self.s3 = session.resource('s3')
        self.s3_client = session.client('s3')

    def validate_bucket_exists(self, s3_bucket_name: str):
        """Run a boto3 API to check if an S3 bucket exists."""
        self.s3.meta.client.head_bucket(Bucket=s3_bucket_name)

    def get_object_response_from_s3(self, s3_url: str):
        """Return object response retrieved from S3 url."""
        bucket, path = S3Client.bucket_key_from_s3_uri(s3_url)
        return self.s3_client.get_object(Bucket=bucket, Key=path)

    def get_object_content_from_s3(self, s3_url: str):
        """Return object content retrieved from S3 url."""
        print(f"Retrieving data from {s3_url}.")
        return self.get_object_response_from_s3(s3_url).get('Body').read().decode('utf-8')

    def write_content(self, s3_path: str, content: Union[str, bytes]):
        """
        Write the provided content to the given s3 path.

        :param s3_path: S3 path to write to.
        :param content: content to write
        """
        content_bytes = content if type(content) == bytes else content.encode('utf-8')
        bucket, key = S3Client.bucket_key_from_s3_uri(s3_path)

        self.s3_client.upload_fileobj(io.BytesIO(content_bytes), bucket, key)
        print(f'Uploaded data to {s3_path}')

    def copy_file(self, old_s3_path: str, new_s3_path: str):
        """Copy object from one location to another within the same bucket."""
        bucket_name, old_key = S3Client.bucket_key_from_s3_uri(old_s3_path)
        _, new_key = S3Client.bucket_key_from_s3_uri(new_s3_path)
        try:
            copy_source = {
                'Bucket': bucket_name,
                'Key': old_key
            }
            self.s3.meta.client.copy(copy_source, bucket_name, new_key)
        except ClientError as e:
            print(f'{e} {old_key}')
            if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == 'NoSuchKey':
                return False
            else:
                raise ValueError("Failed to copy data from {}.".format(old_key), e)
        return True

    def upload_file(self, local_path: str, bucket: str, s3_key: str, extra_args):
        """Upload a local file to S3."""
        try:
            self.s3_client.upload_file(local_path, bucket, s3_key, ExtraArgs=extra_args)
        except Exception as e:
            print(e)

    def upload_directory(self, local_dir_path: str, bucket_name: str, s3_path_prefix: str):
        """Upload a directory to S3."""
        for local_path, s3_key, extra_args in self._collect_s3_upload_file_arguments(local_dir_path, s3_path_prefix):
            self.upload_file(local_path=local_path, bucket=bucket_name, s3_key=s3_key, extra_args=extra_args)

    def download_file(self, local_path: str, bucket: str, s3_key: str):
        """Download a file from S3."""
        s3_bucket = self.s3.Bucket(bucket)
        s3_bucket.download_file(s3_key, local_path)

    def download_directory(self, local_dir_path: str, bucket_name: str, s3_path_prefix: str):
        """Download a directory from S3."""
        s3_download_file_args = self._get_s3_download_file_args(
            s3_objects=self.get_objects_from_s3(bucket=bucket_name, filter_prefix=s3_path_prefix),
            local_dir=local_dir_path
        )
        for download_source_key, download_target in s3_download_file_args:
            self.download_file(local_path=download_target, bucket=bucket_name, s3_key=download_source_key)

    def get_objects_from_s3(self, bucket: str, filter_prefix: str = "") -> List:
        """Get objects from S3 filtered by a given prefix."""
        s3_bucket = self.s3.Bucket(bucket)
        return s3_bucket.objects.filter(Prefix=filter_prefix)

    def write_jsonl(self, s3_path: str, rows: List[Dict]):
        """
        Write the passed in list of dictionaries to the given local or s3 path as JSONL.

        :param s3_path: Path to write to. Accepts local filesystem and S3 paths
        :param rows: List of dictionaries to encode as JSONL
        :return:
        """
        content = ''.join([f'{json.dumps(row)}\n' for row in rows])
        self.write_content(content=content, s3_path=s3_path)

    def _collect_s3_upload_file_arguments(self, local_path: str, s3_path_prefix: str):
        """Collect the local path, S3 key to copy to, and extra S3 args for all files in a local directory."""
        upload_file_arguments = []
        for root, dirs, files in os.walk(local_path):
            for file in files:
                extra_args = {}
                content_type = mimetypes.guess_type(file)[0]
                if content_type:
                    extra_args['ContentType'] = content_type
                upload_file_arguments.append((os.path.join(root, file), f'{s3_path_prefix}/{root}/{file}', extra_args))
        return upload_file_arguments

    def _get_s3_download_file_args(self, s3_objects: list, local_dir: str):
        """Get list of tuples of source keys, local target paths from S3 objects for all pdfs. Will also create local_dir if necessary."""
        args = []
        for obj in s3_objects:
            if obj.key.endswith('.pdf'):
                target = obj.key if local_dir is None \
                    else os.path.join(local_dir, os.path.basename(obj.key))
                if not os.path.exists(os.path.dirname(target)):
                    os.makedirs(os.path.dirname(target))
                args.append((obj.key, target))
        return args

    @staticmethod
    def bucket_key_from_s3_uri(s3_path: str) -> Tuple[str, str]:
        """Return bucket and key from s3 URL."""
        o = urlparse(s3_path, allow_fragments=False)
        bucket = o.netloc
        key = o.path.lstrip('/')
        return bucket, key

    @staticmethod
    def remove_extension(file: str):
        """Return a filename without its extension."""
        return os.path.splitext(file)[0]
