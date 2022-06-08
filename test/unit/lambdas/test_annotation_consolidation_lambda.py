from unittest import TestCase
import json
from unittest.mock import patch

from lambdas.annotation_consolidation_lambda import get_annotation_file_name, get_annotation_file_path, get_annotation_obj, \
    get_blocks_from_s3_ref, lambda_handler, remove_block_indices_from_blocks
from utils.block_helper import Block
from type.semi_structured_annotation import SemiStructuredDocumentType
from utils.s3_helper import S3Client


class MockS3Client(S3Client):
    def __init__(self):
        pass

    def get_object_content_from_s3(self, s3_url: str):
        if 'consolidation-request' in s3_url:
            with open('test/unit/resources/sample_consolidation_request/2021-11-18_02:19:51.json', 'r') as f:
                return f.read()
        else:
            with open('test/unit/resources/sample_blocks/sample_file1_1_blocks.json', 'r') as f:
                return f.read()

    def write_content(self, s3_path: str, content):
        pass


@patch('lambdas.annotation_consolidation_lambda.S3Client.remove_extension', S3Client.remove_extension)
@patch('lambdas.annotation_consolidation_lambda.S3Client.bucket_key_from_s3_uri', S3Client.bucket_key_from_s3_uri)
class AnnotationConsolidationLambdaTest(TestCase):

    def test_remove_block_indices_from_blocks(self):
        blocks = [
            Block(1, "LINE", "some_word another_word yet_another_word", 0).__dict__,
            Block(1, "WORD", "some_word", 1, parent_block_index=0).__dict__,
            Block(1, "WORD", "another_word", 2, parent_block_index=0).__dict__,
            Block(1, "WORD", "yet_another_word", 3, parent_block_index=0).__dict__
        ]
        self.assertTrue(all(["parentBlockIndex" in b and "blockIndex" in b for b in blocks]))
        blocks = remove_block_indices_from_blocks(blocks)
        self.assertTrue(all(["parentBlockIndex" not in b and "blockIndex" not in b for b in blocks]))

    def test_get_blocks_from_s3_ref(self):
        blocks_from_empty_s3_ref = get_blocks_from_s3_ref('', None)
        self.assertEqual(blocks_from_empty_s3_ref, [])

        # TODO: non-empty S3 reference

    def test_get_annotations(self):
        annotation_map = json.dumps({
            "DocumentMetadata": {
                "Pages": "10",
                "PageNumber": "1"
            },
            "Version": "1234-56-78",
            "DocumentType": SemiStructuredDocumentType.NativePDF.value
        })
        annotations_obj = get_annotation_obj(annotation_map, S3Client(), "some_file_name")
        self.assertEqual(annotations_obj.Version, "1234-56-78")
        self.assertEqual(annotations_obj.DocumentType, SemiStructuredDocumentType.NativePDF.value)
        self.assertEqual(annotations_obj.DocumentMetadata.Pages, "10")
        self.assertEqual(annotations_obj.DocumentMetadata.PageNumber, "1")
        self.assertEqual(annotations_obj.Blocks, [])
        self.assertEqual(annotations_obj.Entities, [])
        self.assertTrue(annotations_obj.File.startswith("some_file_name-1"))

    def test_get_annotation_file_name(self):
        file_name = get_annotation_file_name("some_file_name", 4)
        self.assertTrue(file_name.startswith("some_file_name-4") and file_name.endswith("-ann.json"))
        self.assertEqual(len(file_name), len("some_file_name-4-12345678-ann.json"))

    def test_get_annotation_file_path(self):
        consolidation_request_annotation_json = "s3://comprehend-semi-structured-docs-us-west-2-123456789010/output/cus-semi-structured-b1-" \
            "part1-blind1a/annotations/consolidated-annotation/consolidation-request/iteration-1/2021-10-25_22:52:14.json"
        annotation_file_name = "ANNOTATION_FILENAME-1-12345678-ann.json"
        annotation_file_path = get_annotation_file_path(consolidation_request_annotation_json, annotation_file_name)
        expected_file_path = "s3://comprehend-semi-structured-docs-us-west-2-123456789010/output/cus-semi-structured-b1-part1-blind1a/" \
            f"annotations/consolidated-annotation/consolidation-response/iteration-1/annotations/{annotation_file_name}"
        self.assertEqual(annotation_file_path, expected_file_path)

    def test_write_annotations(self):
        pass

    def test_do_consolidation(self):
        # tested in test_lambda_handler
        pass

    @patch('lambdas.annotation_consolidation_lambda.S3Client')
    def test_lambda_handler(self, s3_client):
        s3_client.return_value = MockS3Client()
        lambda_handler_event = {
            "version": "2018-10-06",
            "labelingJobArn": "arn:aws:sagemaker:us-west-2:123456789010:labeling-job/test-labeling-job-20211118t021255",
            "payload": {
                "s3Uri": "s3://comprehend-semi-structured-docs-us-west-2-123456789010/output/test-labeling-job-"
                "20211118T021255/annotations/consolidated-annotation/consolidation-request/iteration-1/2021-11-18_02:19:51.json"
            },
            "labelAttributeName": "test-labeling-job-20211118T021255",
            "roleArn": "arn:aws:iam::123456789010:role/sam-app-SageMakerExecutionRole-1RO6FGQ6YVEF5",
            "outputConfig": "s3://comprehend-semi-structured-docs-us-west-2-123456789010/output/test-labeling-job-20211118T021255/annotations",
            "maxHumanWorkersPerDataObject": 1,
            "vpcEnabled": False
        }
        lambda_handler_output = lambda_handler(event=lambda_handler_event, context=None)
        expected_consolidation_output = [
            {
                'datasetObjectId': '0',
                'consolidatedAnnotation': {
                    'content': {
                        'test-labeling-job-20211118T021255': {
                            'annotation-ref': "s3://comprehend-semi-structured-docs-us-west-2-123456789010/output/" \
                                "test-labeling-job-20211118T021255/annotations/consolidated-annotation/consolidation-response/" \
                                "iteration-1/annotations/sample_file1-1-" \
                                f"{lambda_handler_output[0]['consolidatedAnnotation']['content']['test-labeling-job-20211118T021255']['annotation-ref'][-17:-9]}-ann.json"
                        },
                        'test-labeling-job-20211118T021255-metadata': {
                            'job-name': 'labeling-job/test-labeling-job-20211118t021255',
                            'type': 'groundtruth/pdf-ner',
                            'creation-date': lambda_handler_output[0]['consolidatedAnnotation']['content']['test-labeling-job-20211118T021255-metadata']['creation-date'],
                            'human-annotated': 'yes'
                        }
                    }
                }
            }
        ]
        self.assertListEqual(expected_consolidation_output, lambda_handler_output)
