from unittest import TestCase
from collections import namedtuple
import tempfile
from botocore.exceptions import ClientError

from utils.s3_helper import S3Client


def generate_s3_object(value=''):
    """Generate an S3 object with a key attribute."""
    return namedtuple("S3Object", ['key'])(value)

class Object(object):
    pass

class MockMetaClient(object):
    def __init__(self, raise_exception, error):
        self.raise_exception = raise_exception
        self.error = error

    def copy(self, copy_source: dict, bucket_name: str, new_key: str):
        if self.raise_exception:
            raise self.error

class MockS3Resource():
    def __init__(self, raise_exception, error):
        self.meta = Object()
        self.meta.client = MockMetaClient(raise_exception=raise_exception, error=error)

class MockS3Client(S3Client):
    def __init__(self, raise_exception=False, error=None):
        self.s3 = MockS3Resource(raise_exception=raise_exception, error=error)

class S3HelperTest(TestCase):

    def test_s3_client_collect_s3_upload_file_arguments(self):
        s3_client = MockS3Client()
        file_data = s3_client._collect_s3_upload_file_arguments(
            local_path='test/unit/resources/dir_collect_s3_upload_file_arguments',
            s3_path_prefix='upload/to/here'
        )
        self.assertEqual(len(file_data), 3)
    
    def test_s3_client_get_s3_download_file_args(self):
        with tempfile.TemporaryDirectory() as local_dir:
            s3_client = MockS3Client()
            args = [
                generate_s3_object('folder1/file1.pdf'),
                generate_s3_object('folder1/file1.json'),
                generate_s3_object('folder1/folder2/file2.pdf'),
                generate_s3_object('folder1/folder2/file2.txt'),
                generate_s3_object('folder1/folder2/folder3/file3.pdf'),
                generate_s3_object('folder1/folder2/folder3/file3.docx')
            ]
            s3_download_file_args = s3_client._get_s3_download_file_args(args, local_dir)
            self.assertListEqual(s3_download_file_args, [
                ('folder1/file1.pdf', f'{local_dir}/file1.pdf'),
                ('folder1/folder2/file2.pdf', f'{local_dir}/file2.pdf'),
                ('folder1/folder2/folder3/file3.pdf', f'{local_dir}/file3.pdf')
            ])
    
    def test_s3_client_bucket_key_from_s3_uri(self):
        bucket, key = S3Client.bucket_key_from_s3_uri('s3://bucket/path/to/file.pdf')
        self.assertEqual(bucket, 'bucket')
        self.assertEqual(key, 'path/to/file.pdf')

        bucket, key = S3Client.bucket_key_from_s3_uri('//bucket/path/to/file.pdf')
        self.assertEqual(bucket, 'bucket')
        self.assertEqual(key, 'path/to/file.pdf')

        bucket, key = S3Client.bucket_key_from_s3_uri('/path/to/file.pdf')
        self.assertEqual(bucket, '')
        self.assertEqual(key, 'path/to/file.pdf')

        bucket, key = S3Client.bucket_key_from_s3_uri('s3:/path/to/file.pdf')
        self.assertEqual(bucket, '')
        self.assertEqual(key, 'path/to/file.pdf')

        bucket, key = S3Client.bucket_key_from_s3_uri('')
        self.assertEqual(bucket, '')
        self.assertEqual(key, '')

    def test_s3_client_copy_file(self):
        s3_client = MockS3Client()
        self.assertTrue(s3_client.copy_file(old_s3_path='s3://bucket/old/path', new_s3_path='s3://bucket/new/path'))

        s3_client = MockS3Client(raise_exception=True, error=ClientError({ 'Error': { 'Code': '404' }}, 'copy'))
        self.assertFalse(s3_client.copy_file(old_s3_path='s3://bucket/old/path', new_s3_path='s3://bucket/new/path'))

        s3_client = MockS3Client(raise_exception=True, error=ClientError({ 'Error': { 'Code': 'NoSuchKey' }}, 'copy'))
        self.assertFalse(s3_client.copy_file(old_s3_path='s3://bucket/old/path', new_s3_path='s3://bucket/new/path'))

        with self.assertRaises(ValueError):
            s3_client = MockS3Client(raise_exception=True, error=ClientError({ 'Error': { 'Code': 'SomeCode' }}, 'copy'))
            s3_client.copy_file(old_s3_path='s3://bucket/old/path', new_s3_path='s3://bucket/new/path')
