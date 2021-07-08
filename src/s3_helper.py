# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""s3 helper class."""
import boto3
import io
import os

from typing import Union
from botocore.exceptions import ClientError
# from urllib.parse import urlparse


class S3Client(object):
    """Helper Class for S3 operations."""

    s3_client = boto3.client("s3")
    s3 = boto3.resource("s3")

    def __init__(self, role_arn=None):
        """
        Initialize the S3 client.
        :param role_arn: Role which have access to consolidation request S3 payload file.
        :return:

        """
        default_session = "SSIE_Annotation_Consolidation_Lambda_Session"
        sts_connection = boto3.client('sts')
        assume_role_object = sts_connection.assume_role(RoleArn=role_arn, RoleSessionName=default_session)
        session = boto3.Session(
            aws_access_key_id=assume_role_object['Credentials']['AccessKeyId'],
            aws_secret_access_key=assume_role_object['Credentials']['SecretAccessKey'],
            aws_session_token=assume_role_object['Credentials']['SessionToken'])
        self.s3 = session.resource('s3')
        self.s3_client = session.client('s3')

    def get_object_from_s3(self, s3_url):
        """Return object retrieved from S3 url."""
        bucket, path = S3Client.bucket_key_from_s3_uri(s3_url)
        try:
            payload = self.s3_client.get_object(Bucket=bucket, Key=path).get('Body').read().decode('utf-8')
        except ClientError as e:
            print(e)
            if e.response['Error']['Code'] == "404" or e.response['Error']['Code'] == 'NoSuchKey':
                return None
            else:
                raise ValueError("Failed to retrieve data from {}.".format(s3_url), e)

        return payload

    def write_content(self, path: str, content: Union[str, bytes]):
        """
        Write the provided content to the given s3 path.

        :param path: S3 path to write to.
        :param content: content to write
        """
        content_bytes = content if type(content) == bytes else content.encode('utf-8')
        bucket, key = S3Client.bucket_key_from_s3_uri(path)

        self.s3_client.upload_fileobj(io.BytesIO(content_bytes), bucket, key)
        print(f'Uploaded data to {path}')

    @staticmethod
    def bucket_key_from_s3_uri(s3_path):
        """Return bucket and key from s3 URL."""
        # o = urlparse(s3_path, allow_fragments=False)
        # bucket = o.netloc
        # key = o.path
        path_parts = s3_path.replace("s3://", "").split("/")
        bucket = path_parts.pop(0)
        key = "/".join(path_parts)

        return bucket, key

    @staticmethod
    def remove_extension(file: str):
        """Return a filename without its extension."""
        return os.path.splitext(file)[0]
