from typing import Union
from unittest import TestCase
import tempfile
import os
import json
from datetime import datetime
from unittest.mock import patch

from lambdas.pre_human_task_lambda import get_geometry_from_plumber_word, get_pdf_blocks, get_temp_folder_bucket_key_from_s3_uri, \
    is_scanned_pdf, lambda_handler, output_pdf_temp_file_to_s3, plumber_line_to_blocks, textract_block_to_block, \
    textract_block_to_block_relationship
from utils.block_helper import Block, BoundingBox, Geometry, Relationship
from utils.s3_helper import S3Client
from utils.textract_helper import TextractClient


class MockTextractClient(TextractClient):
    def __init__(self, blocks_filename = ''):
        self.blocks_filename = blocks_filename

    def detect_document_text(self, byte_array):
        ret = {}
        with open(f'test/unit/resources/sample_blocks/{self.blocks_filename}', 'r') as f:
            ret = json.loads(f.read())
        return ret


class MockStreamingBody(object):
    def __init__(self, s3_url: str):
        self.s3_url = s3_url

    def read(self):
        filename = os.path.basename(self.s3_url)
        if '.pdf' in filename:
            with open(f'test/unit/resources/sample_documents/{filename}', 'rb') as f:
                return f.read()
        elif '.json' in filename:
            with open(f'test/unit/resources/sample_annotations/{filename}', 'rb') as f:
                return f.read()


class MockS3Client(S3Client):
    def __init__(self, temp_dir: str = ''):
        self.temp_dir = temp_dir

    def write_content(self, s3_path: str, content: Union[str, list]):
        file_path = os.path.join(self.temp_dir, s3_path.replace('s3://', ''))
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w') as f:
            if type(content) == list:
                content = '\n'.join(content)
            else:
                content = str(content)
    
    def get_object_response_from_s3(self, s3_url: str):
        return {'Body': MockStreamingBody(s3_url=s3_url), 'LastModified': datetime.now()}


@patch('lambdas.pre_human_task_lambda.S3Client.remove_extension', S3Client.remove_extension)
@patch('lambdas.pre_human_task_lambda.S3Client.bucket_key_from_s3_uri', S3Client.bucket_key_from_s3_uri)
class PreHumanTaskLambdaTest(TestCase):

    def test_is_scanned_pdf(self):
        images = []
        self.assertFalse(is_scanned_pdf(images, 10, 10))

        images = [
            {'width': 1, 'height': 1}, # 1
            {'width': 4.5, 'height': 5.3311} # < 24
        ]
        self.assertFalse(is_scanned_pdf(images, 10, 10))

        # single image meets threshold
        images = [
            {'width': 20, 'height': 1.25} # 25
        ]
        self.assertTrue(is_scanned_pdf(images, 10, 10))

        # multiple images meet threshold
        images = [
            {'width': 10, 'height': 2}, # 20
            {'width': 5, 'height': 1} # 5
        ]
        self.assertTrue(is_scanned_pdf(images, 10, 10))

    def test_get_pdf_blocks(self):
        scanned_pdf_file = open('test/unit/resources/sample_documents/scanned.pdf', 'rb')
        scanned_pdf_bytes = scanned_pdf_file.read()
        scanned_pdf_blocks, scanned_is_native = get_pdf_blocks(
            pdf_bytes=scanned_pdf_bytes,
            page_num=1,
            use_textract_only=False,
            source_ref='test/unit/resources/sample_documents/scanned.pdf',
            poppler_path=None,
            textract_client=MockTextractClient('scanned_1_blocks.json')
        )
        self.assertFalse(scanned_is_native)
        self.assertEqual(len(scanned_pdf_blocks), 549)
        scanned_pdf_file.close()

        native_pdf_file = open('test/unit/resources/sample_documents/file2.pdf', 'rb')
        native_pdf_bytes = native_pdf_file.read()
        native_pdf_blocks, native_is_native = get_pdf_blocks(
            pdf_bytes=native_pdf_bytes,
            page_num=1,
            use_textract_only=False,
            source_ref='test/unit/resources/sample_documents/file2.pdf',
            poppler_path=None,
            textract_client=None
        )
        self.assertTrue(native_is_native)
        self.assertEqual(len(native_pdf_blocks), 281)

        native_pdf_blocks, native_is_native = get_pdf_blocks(
            pdf_bytes=native_pdf_bytes,
            page_num=1,
            use_textract_only=True,
            source_ref='test/unit/resources/sample_documents/file2.pdf',
            poppler_path=None,
            textract_client=MockTextractClient('file2_1_blocks.json')
        )
        self.assertFalse(native_is_native)
        self.assertEqual(len(native_pdf_blocks), 303)
        native_pdf_file.close()

        # assert that file that fails pdfplumber.open will be tried with scanned PDF Textract logic
        pdfplumber_error_pdf_file = open('test/unit/resources/pdfplumber_error/not_pdf.pdf', 'rb')
        pdfplumber_error_pdf_bytes = pdfplumber_error_pdf_file.read()
        # an Exception is raised as the test file is also expected to fail with the Textract call
        with self.assertRaises(Exception):
            get_pdf_blocks(
                pdf_bytes=pdfplumber_error_pdf_bytes,
                page_num=1,
                use_textract_only=False,
                source_ref='test/unit/resources/sample_documents/file2.pdf',
                poppler_path=None,
                textract_client=None
            )

    def test_output_pdf_temp_file_to_s3(self):
        with tempfile.TemporaryDirectory() as local_dir:
            job_id = 'job_id'
            page_num = 1
            source_ref = 's3://bucket/path/to/file.pdf'
            list_content = ['list', 'content']
            bytes_content = b'bytes_content'
            s3_client = MockS3Client(temp_dir=local_dir)

            s3_write_path_str = output_pdf_temp_file_to_s3(s3_client, source_ref, content=bytes_content, page_num=page_num, job_id=job_id)
            self.assertEqual(s3_write_path_str, 's3://bucket/comprehend-semi-structured-docs-intermediate-output/job_id/file_1_base64')
            self.assertTrue(os.path.exists(os.path.join(local_dir, 'bucket/comprehend-semi-structured-docs-intermediate-output/job_id/file_1_base64')))

            s3_write_path_str = output_pdf_temp_file_to_s3(s3_client, source_ref, content=list_content, page_num=page_num, job_id=job_id)
            self.assertEqual(s3_write_path_str, 's3://bucket/comprehend-semi-structured-docs-intermediate-output/job_id/file_1_blocks.json')
            self.assertTrue(os.path.exists(os.path.join(local_dir, 'bucket/comprehend-semi-structured-docs-intermediate-output/job_id/file_1_blocks.json')))

    def test_get_temp_folder_bucket_key_from_s3_uri(self):
        s3_ref = 's3://bucket/path/to/file.pdf'
        job_id = 'sagemaker-labeling-job'
        self.assertEqual(get_temp_folder_bucket_key_from_s3_uri(s3_ref, job_id), ('bucket', 'comprehend-semi-structured-docs-intermediate-output/sagemaker-labeling-job'))

    def test_get_geometry_from_plumber_word(self):
        page_width = 10
        page_height = 10
        word = {
            'x0': 1,
            'x1': 7,
            'top': 4,
            'bottom': 9
        }
        geometry = get_geometry_from_plumber_word(word, page_width, page_height)
        self.assertDictEqual(geometry.BoundingBox.__dict__, Geometry(0.6, 0.5, 0.1, 0.4).BoundingBox.__dict__)

    def test_plumber_line_to_blocks(self):
        plumber_line = [
            {
                'x0': 1,
                'x1': 4,
                'top': 4,
                'bottom': 9,
                'text': 'Hello'
            },
            {
                'x0': 6,
                'x1': 10,
                'top': 4,
                'bottom': 9,
                'text': 'Amazonian'
            }
        ]
        blocks, block_index = plumber_line_to_blocks(page=1, plumber_line=plumber_line, block_index=-1, page_width=10, page_height=10)
        expected_blocks = [
            Block(page=1, block_type='WORD', text='Hello', index=1, geometry=Geometry(0.3, 0.5, 0.1, 0.4), parent_block_index=0),
            Block(page=1, block_type='WORD', text='Amazonian', index=2, geometry=Geometry(0.4, 0.5, 0.6, 0.4), parent_block_index=0)
        ]
        expected_line_block = Block(page=1, block_type='LINE', text='Hello Amazonian', index=0, geometry=Geometry(0.9, 0.5, 0.1, 0.4), parent_block_index=-1)
        expected_line_block.Relationships.append(Relationship(ids=[b.Id for b in expected_blocks], type='LINE'))
        expected_blocks.insert(0, expected_line_block)
        self.assertEqual(block_index, 2)
        for index, b in enumerate(blocks):
            self.assertDictEqual(b.Geometry.BoundingBox.__dict__, expected_blocks[index].Geometry.BoundingBox.__dict__)
            if index == 0:
                self.assertEqual(len(b.Relationships[0].Ids), len(expected_blocks[index].Relationships[0].Ids))
            else:
                self.assertEqual(len(b.Relationships), 0)
            self.assertEqual(b.BlockType, expected_blocks[index].BlockType)
            self.assertEqual(b.Text, expected_blocks[index].Text)
            self.assertEqual(b.parentBlockIndex, expected_blocks[index].parentBlockIndex)
            self.assertEqual(b.blockIndex, expected_blocks[index].blockIndex)

    def test_blocks_from_native_pdf(self):
        # already tested in test_get_pdf_blocks
        pass

    def test_textract_block_to_block_relationship(self):
        textract_relationship_list = [
            {
                'Type': 'NOT_CHILD',
                'Ids': ['id1', 'id2']
            },
            {
                'Type': 'CHILD',
                'Ids': ['id3', 'id4']
            }
        ]
        relationship = textract_block_to_block_relationship(textract_relationship_list=textract_relationship_list)
        self.assertEqual(relationship.__dict__, Relationship(['id3', 'id4'], 'CHILD').__dict__)

    def test_textract_block_to_block(self):
        textract_block = {
            'Geometry': {
                'BoundingBox': {
                    'Top': 5,
                    'Height': 10,
                    'Left': 3,
                    'Width': 6
                }
            },
            'BlockType': 'LINE',
            'Text': 'textract block text',
            'Id': 'textract_block_id',
            'Relationships': [{
                'Type': 'CHILD',
                'Ids': ['id1', 'id2']
            }]
        }
        block = textract_block_to_block(page=1, textract_block=textract_block, index=5, parent_index=3)
        self.assertDictEqual(block.Geometry.BoundingBox.__dict__, BoundingBox(6, 10, 3, 5).__dict__)
        self.assertEqual(block.Relationships[0].__dict__, Relationship(['id1', 'id2'], 'CHILD').__dict__)
        self.assertEqual(block.BlockType, 'LINE')
        self.assertEqual(block.Text, 'textract block text')
        self.assertEqual(block.parentBlockIndex, 3)
        self.assertEqual(block.blockIndex, 5)

    def test_blocks_from_scanned_pdf(self):
        # already tested in test_get_pdf_blocks
        pass

    @patch('lambdas.pre_human_task_lambda.TextractClient')
    @patch('lambdas.pre_human_task_lambda.S3Client')
    def test_lambda_handler(self, s3_client, textract_client):
        with tempfile.TemporaryDirectory() as local_dir:
            s3_client.return_value = MockS3Client(temp_dir=local_dir)
            textract_client.return_value = MockTextractClient(blocks_filename='sample_file1_1_blocks.json')
            # regular job
            event = {
                "version": "2018-10-06",
                "labelingJobArn": "arn:aws:sagemaker:us-west-2:123456789010:labeling-job/test-labeling-job-20211118t200608",
                "dataObject": {
                    "source-ref": "s3://comprehend-semi-structured-docs-us-west-2-123456789010/single_file/sample_file1.pdf",
                    "page": "1",
                    "metadata": {
                        "pages": "1",
                        "use-textract-only": False,
                        "labels": [
                            "EntityTypeA",
                            "EnityTypeB",
                            "EntityTypeC"
                        ]
                    }
                }
            }
            lambda_handler_output = lambda_handler(event=event, context=None)
            expected_lambda_handler_output = {
                'taskInput': {
                    'taskObject': {
                        'pdfBase64S3Ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/comprehend-semi-structured-docs-intermediate-output/test-labeling-job-20211118t200608/sample_file1_1_base64',
                        'pdfBlocksS3Ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/comprehend-semi-structured-docs-intermediate-output/test-labeling-job-20211118t200608/sample_file1_1_blocks.json',
                        'pdfType': 'NativePDF',
                        'version': '2021-04-30',
                        'metadata': {
                            'pages': '1',
                            'use-textract-only': 'false',
                            'labels': ['EntityTypeA', 'EnityTypeB', 'EntityTypeC'],
                            'page': '1',
                            'source_ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/single_file/sample_file1.pdf',
                            'document_id': 'sample_file1_1'
                        },
                        'annotatorMetadata': None,
                        'primaryAnnotationS3Ref': None,
                        'secondaryAnnotationS3Ref': None
                    },
                    'labels': ['EntityTypeA', 'EnityTypeB', 'EntityTypeC']
                },
                'humanAnnotationRequired': 'true'
            }
            self.assertDictEqual(lambda_handler_output, expected_lambda_handler_output)

        with tempfile.TemporaryDirectory() as local_dir:
            s3_client.return_value = MockS3Client(temp_dir=local_dir)
            # 1-job verification
            verification_event = {
                "version": "2018-10-06",
                "labelingJobArn": "arn:aws:sagemaker:us-west-2:123456789010:labeling-job/arbitration-labeling-job-20211214t085606",
                "dataObject": {
                    "source-ref": "s3://comprehend-semi-structured-docs-us-west-2-123456789010/single_file/sample_file1.pdf",
                    "page": "1",
                    "metadata": {
                        "pages": "1",
                        "use-textract-only": False,
                        "labels": [
                            "EntityTypeA",
                            "EnityTypeB",
                            "EntityTypeC"
                        ]
                    },
                    "primary-annotation-ref": "s3://comprehend-semi-structured-docs-us-west-2-123456789010/output/test-labeling-job-20211214T084540/annotations/consolidated-annotation/consolidation-response/iteration-1/annotations/sample_file1-1-570408b2-ann.json",
                }
            }
            verification_lambda_handler_output = lambda_handler(event=verification_event, context=None)
            verification_expected_lambda_handler_output = {
                'taskInput': {
                    'taskObject': {
                        'pdfBase64S3Ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/comprehend-semi-structured-docs-intermediate-output/arbitration-labeling-job-20211214t085606/sample_file1_1_base64',
                        'pdfBlocksS3Ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/comprehend-semi-structured-docs-intermediate-output/arbitration-labeling-job-20211214t085606/sample_file1_1_blocks.json',
                        'pdfType': 'NativePDF',
                        'version': '2021-04-30',
                        'metadata': {
                            'pages': '1',
                            'use-textract-only': 'false',
                            'labels': [
                                'EntityTypeA',
                                'EnityTypeB',
                                'EntityTypeC'
                            ],
                            'page': '1',
                            'source_ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/single_file/sample_file1.pdf',
                            'document_id': 'sample_file1_1'
                        },
                        'annotatorMetadata': None,
                        'primaryAnnotationS3Ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/output/test-labeling-job-20211214T084540/annotations/consolidated-annotation/consolidation-response/iteration-1/annotations/sample_file1-1-570408b2-ann.json',
                        'secondaryAnnotationS3Ref': None
                    },
                    'labels': [
                        'EntityTypeA',
                        'EnityTypeB',
                        'EntityTypeC'
                    ]
                },
                'humanAnnotationRequired': 'true'
            }
            self.assertDictEqual(verification_lambda_handler_output, verification_expected_lambda_handler_output)

        with tempfile.TemporaryDirectory() as local_dir:
            s3_client.return_value = MockS3Client(temp_dir=local_dir)
            # 2-job verification
            two_verification_event = {
                "version": "2018-10-06",
                "labelingJobArn": "arn:aws:sagemaker:us-west-2:123456789010:labeling-job/arbitration-labeling-job-20211214t085606",
                "dataObject": {
                    "source-ref": "s3://comprehend-semi-structured-docs-us-west-2-123456789010/single_file/sample_file1.pdf",
                    "page": "1",
                    "metadata": {
                        "pages": "1",
                        "use-textract-only": False,
                        "labels": [
                            "EntityTypeA",
                            "EnityTypeB",
                            "EntityTypeC"
                        ]
                    },
                    "primary-annotation-ref": "s3://comprehend-semi-structured-docs-us-west-2-123456789010/output/test-labeling-job-20211214T084540/annotations/consolidated-annotation/consolidation-response/iteration-1/annotations/sample_file1-1-570408b2-ann.json",
                    "secondary-annotation-ref": "s3://comprehend-semi-structured-docs-us-west-2-123456789010/output/test-2-labeling-job-20211214T084625/annotations/consolidated-annotation/consolidation-response/iteration-1/annotations/sample_file1-1-bad5a1bc-ann.json"
                }
            }
            two_verification_lambda_handler_output = lambda_handler(event=two_verification_event, context=None)
            two_verification_expected_lambda_handler_output = {
                'taskInput': {
                    'taskObject': {
                        'pdfBase64S3Ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/comprehend-semi-structured-docs-intermediate-output/arbitration-labeling-job-20211214t085606/sample_file1_1_base64',
                        'pdfBlocksS3Ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/comprehend-semi-structured-docs-intermediate-output/arbitration-labeling-job-20211214t085606/sample_file1_1_blocks.json',
                        'pdfType': 'NativePDF',
                        'version': '2021-04-30',
                        'metadata': {
                            'pages': '1',
                            'use-textract-only': 'false',
                            'labels': [
                                'EntityTypeA',
                                'EnityTypeB',
                                'EntityTypeC'
                            ],
                            'page': '1',
                            'source_ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/single_file/sample_file1.pdf',
                            'document_id': 'sample_file1_1'
                        },
                        'annotatorMetadata': None,
                        'primaryAnnotationS3Ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/output/test-2-labeling-job-20211214T084625/annotations/consolidated-annotation/consolidation-response/iteration-1/annotations/sample_file1-1-bad5a1bc-ann.json',
                        'secondaryAnnotationS3Ref': 's3://comprehend-semi-structured-docs-us-west-2-123456789010/output/test-labeling-job-20211214T084540/annotations/consolidated-annotation/consolidation-response/iteration-1/annotations/sample_file1-1-570408b2-ann.json'
                    },
                    'labels': [
                        'EntityTypeA',
                        'EnityTypeB',
                        'EntityTypeC'
                    ]
                },
                'humanAnnotationRequired': 'true'
            }
            self.assertDictEqual(two_verification_lambda_handler_output, two_verification_expected_lambda_handler_output)
