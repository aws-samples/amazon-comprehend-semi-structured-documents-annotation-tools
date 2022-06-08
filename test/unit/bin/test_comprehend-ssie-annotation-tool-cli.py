from unittest import TestCase
import importlib
import shutil
import os
from collections import namedtuple
import json

from utils.s3_helper import S3Client
from constants import general
from type.groundtruth_annotation_ui_config import GroundTruthAnnotationUIConfig, AnnotationUITaskSchemas, NamedEntitiesAnnotationTaskConfig


comprehend_ssie_annotation_tool_cli = importlib.import_module("comprehend-ssie-annotation-tool-cli")


def generate_schema_content(labels=general.DEFAULT_LABELS):
    """Generate the schema content for a UI schema file."""
    return json.dumps(GroundTruthAnnotationUIConfig(
        version="SSIE_NER_SCHEMA_2021-04-15",
        schemas=AnnotationUITaskSchemas(
            named_entity=NamedEntitiesAnnotationTaskConfig(
                annotation_task="NER",
                tags=labels,
                properties=[]
            )
        ),
        exported_time="2021-04-15T17:34:34.493Z",
        uuid="f44b0438-72ac-43ac-bdc7-5727914522b9"
    ).dict())


def generate_s3_object(value=''):
    """Generate an S3 object with a key attribute."""
    return namedtuple("S3Object", ['key'])(value)


def get_all_files_in_directory(local_path: str):
    return [f for f in os.listdir(local_path) if os.path.isfile(os.path.join(local_path, f))]


def generate_manifest_json_list(primary_annotation_ref_none=False, secondary_annotation_ref_none=False):
    """Generate a list of manifest json objects based off of test/unit/resources/sample_output_manifest.jsonl."""
    metadata = {"pages": "1", "use-textract-only": False, "labels": ["PER", "LOC", "ORG", "FAC", "BRAND", "COMM", "TITLE:MOVIE", \
                "TITLE:MUSIC", "TITLE:BOOK", "TITLE:SOFT", "TITLE:GAME", "TITLE:OTHER", "PERSON:TITLE", "QUANT", "IDENTITY", "OTHER"]}
    return [
        {
            "source-ref": "s3://bucket/documents/file1.pdf",
            "page": "1",
            "metadata": metadata,
            "annotator-metadata": None,
            "primary-annotation-ref": "s3://bucket/annotations/file1-1-616696e2-ann.json" if not primary_annotation_ref_none else None,
            "secondary-annotation-ref": "s3://bucket/annotations/file1-1-616696e2-ann.json" if not secondary_annotation_ref_none else None
        },
        {
            "source-ref": "s3://bucket/documents/file2.pdf",
            "page": "1",
            "metadata": metadata,
            "annotator-metadata": None,
            "primary-annotation-ref": "s3://bucket/annotations/file2-1-d56ced2d-ann.json" if not primary_annotation_ref_none else None,
            "secondary-annotation-ref": "s3://bucket/annotations/file2-1-d56ced2d-ann.json" if not secondary_annotation_ref_none else None
        },
        {
            "source-ref": "s3://bucket/documents/file3.pdf",
            "page": "1",
            "metadata": metadata,
            "annotator-metadata": None,
            "primary-annotation-ref": "s3://bucket/annotations/file3-1-5ce4d425-ann.json" if not primary_annotation_ref_none else None,
            "secondary-annotation-ref": "s3://bucket/annotations/file3-1-5ce4d425-ann.json" if not secondary_annotation_ref_none else None
        }
    ]


class MockS3Client(S3Client):
    """Mock S3 Client class."""

    def upload_file(self, local_path: str, bucket: str, s3_key: str, extra_args):
        """Mock upload_file method."""
        print(f"local_path: {local_path}, bucket: {bucket}, s3_key: {s3_key}, extra_args: {extra_args}")
        return True

    def get_objects_from_s3(self, bucket: str, filter_prefix: str = ""):
        """Mock get_objects_from_s3 method."""
        return [
            generate_s3_object('file1.pdf'),
            generate_s3_object('folder1/file2.pdf'),
            generate_s3_object('folder1/folder2/file3.pdf'),
        ]
    
    def _get_s3_download_file_args(self, s3_objects: list, local_dir: str):
        """Mock _get_s3_download_file_args method."""
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        return [
            ('folder1/file1.pdf', f'{local_dir}/file1.pdf'),
            ('folder1/folder2/file2.pdf', f'{local_dir}/file2.pdf'),
            ('folder1/folder2/folder3/file3.pdf', f'{local_dir}/file3.pdf')
        ]

    def download_file(self, local_path: str, bucket: str, s3_key: str):
        """Mock download_file method."""
        if True in [file in local_path for file in ['file1.pdf', 'file2.pdf', 'file3.pdf']]:
            shutil.copyfile(f'test/unit/resources/sample_documents/{os.path.basename(local_path)}', local_path)

    def get_object_content_from_s3(self, s3_url: str):
        """Mock get_object_content_from_s3 method."""
        if 'schema.json' in s3_url:
            return generate_schema_content()
        elif 'output.manifest' in s3_url:
            with open('test/unit/resources/sample_output_manifest.jsonl', 'r') as f:
                return f.read()


class MockS3ClientGetNoObjectsFromS3(S3Client):
    """Mock S3 Client class."""

    def get_objects_from_s3(self, bucket: str, filter_prefix: str = ""):
        """Mock get_objects_from_s3 method."""
        return []


class MockSagemakerClient():
    """Mock Sagemaker client."""

    @staticmethod
    def generate_sample_describe_labeling_job_response(LabelingJobName: str, bucket='bucket'):
        sample_response = {
            "LabelingJobStatus": "Completed",
            "LabelCounters": {
                "TotalLabeled": 1,
                "HumanLabeled": 1,
                "MachineLabeled": 0,
                "FailedNonRetryableError": 0,
                "Unlabeled": 0
            },
            "CreationTime": 1635978367.361,
            "LastModifiedTime": 1635979356.97,
            "JobReferenceCode": "123456789010D32dCqUODlFDkYu4",
            "LabelingJobName": LabelingJobName,
            "LabelingJobArn": f"arn:aws:sagemaker:us-west-2:123456789010:labeling-job/{LabelingJobName}",
            "LabelAttributeName": LabelingJobName,
            "InputConfig": {
                "DataSource": {
                    "S3DataSource": {
                        "ManifestS3Uri": f"s3://{bucket}/input-manifest/{LabelingJobName}.manifest"
                    }
                }
            },
            "OutputConfig": {
                "S3OutputPath": "s3://{bucket}/output/",
                "KmsKeyId": ""
            },
            "RoleArn": "arn:aws:iam::123456789010:role/iam-role",
            "StoppingConditions": {
                "MaxPercentageOfInputDatasetLabeled": 100
            },
            "HumanTaskConfig": {
                "WorkteamArn": "arn:aws:sagemaker:us-west-2:123456789010:workteam/private-crowd/work-team",
                "UiConfig": {
                    "UiTemplateS3Uri": "s3://{bucket}/ui-template/{LabelingJobName}/ui-template/template-2021-04-15.liquid"
                },
                "PreHumanTaskLambdaArn": "arn:aws:lambda:us-west-2:123456789010:function:pre-lambda",
                "TaskTitle": LabelingJobName,
                "TaskDescription": LabelingJobName,
                "NumberOfHumanWorkersPerDataObject": 1,
                "TaskTimeLimitInSeconds": 3600,
                "TaskAvailabilityLifetimeInSeconds": 864000,
                "MaxConcurrentTaskCount": 1000,
                "AnnotationConsolidationConfig": {
                    "AnnotationConsolidationLambdaArn": "arn:aws:lambda:us-west-2:123456789010:function:consolidation-lambda"
                }
            },
            "LabelingJobOutput": {
                "OutputDatasetS3Uri": f"s3://{bucket}/output/{LabelingJobName}/manifests/output/output.manifest",
                "FinalActiveLearningModelArn": ""
            }
        }
        return sample_response

    def describe_labeling_job(self, LabelingJobName: str):
        """Mock describe_labeling_job method."""
        return self.generate_sample_describe_labeling_job_response(LabelingJobName=LabelingJobName)


class MockParserArgs():
    """Mock parser.parse_args class."""

    def __init__(self, input_s3_path='', schema_path='', entity_types=general.DEFAULT_LABELS, create_input_manifest_only=True,
                 blind1_labeling_job_name=None, blind2_labeling_job_name=None, only_include_expired_tasks=False):
        """Init MockParseArgs class."""
        self.input_s3_path = input_s3_path
        self.create_input_manifest_only = create_input_manifest_only
        self.schema_path = schema_path
        self.entity_types = entity_types
        self.blind1_labeling_job_name = blind1_labeling_job_name
        self.blind2_labeling_job_name = blind2_labeling_job_name
        self.only_include_expired_tasks = only_include_expired_tasks


class ComprehendSSIEAnnotationToolCliTest(TestCase):

    def test_generate_schema(self):
        pass

    def test_cleanup(self):
        temp_dir_path = 'test/unit/resources/temp'
        if not os.path.exists(temp_dir_path):
            os.makedirs(temp_dir_path)
        for i in range(3):
            with open(os.path.join(temp_dir_path, f"file_{i}.txt"), 'w'):
                pass
        self.assertEqual(len(get_all_files_in_directory(temp_dir_path)), 3)

        comprehend_ssie_annotation_tool_cli.cleanup(temp_dir_path)
        self.assertFalse(os.path.exists(temp_dir_path))

    def test_validate_and_format_annotator_metadata(self):
        metadata_str = "key=key1 ,value= value1,key= key2,value=value2 "
        metadata_obj = comprehend_ssie_annotation_tool_cli.validate_and_format_annotator_metadata(metadata_str)
        self.assertEqual(metadata_obj, {"key1 ":" value1", " key2":"value2 "})

        metadata_str_errors = [
            "keyvalue=keyvalue1",
            "key=file1,key=file2,value=value2,value=value1",
            "key=file1,value=value1,key=file1"
        ]
        for metadata_str in metadata_str_errors:
            with self.assertRaises(comprehend_ssie_annotation_tool_cli.InvalidAnnotatorMetadataException):
                comprehend_ssie_annotation_tool_cli.validate_and_format_annotator_metadata(metadata_str)

    def test_get_ui_template_root_from_sagemaker_job(self):
        ui_template_root = comprehend_ssie_annotation_tool_cli.get_ui_template_root_from_sagemaker_job(MockSagemakerClient(), 'job-name')
        self.assertEqual(ui_template_root, 'job-name')

    def test_get_output_manifest_ref_from_sagemaker_job(self):
        output_manifest_ref = comprehend_ssie_annotation_tool_cli.get_output_manifest_ref_from_sagemaker_job(MockSagemakerClient(), 'job-name')
        self.assertEqual(output_manifest_ref, "s3://bucket/output/job-name/manifests/output/output.manifest")

    def test_generate_manifest_jsons_from_previous_jobs(self):
        with open('test/unit/resources/sample_output_manifest.jsonl', 'r') as f:
            primary_output_manifest_contents = f.read()

            expected_manifest_json_list = generate_manifest_json_list(secondary_annotation_ref_none=True)
            received_manifest_json_list = comprehend_ssie_annotation_tool_cli.generate_manifest_jsons_from_previous_jobs(
                s3_client=MockS3Client(),
                sagemaker_client=MockSagemakerClient(),
                primary_output_manifest_contents=primary_output_manifest_contents,
                labels=general.DEFAULT_LABELS,
                annotator_metadata=None,
                args=MockParserArgs(blind1_labeling_job_name='mock_job_name')
            )
            self.assertListEqual(expected_manifest_json_list, received_manifest_json_list)

            expected_manifest_json_list = generate_manifest_json_list()
            received_manifest_json_list = comprehend_ssie_annotation_tool_cli.generate_manifest_jsons_from_previous_jobs(
                s3_client=MockS3Client(),
                sagemaker_client=MockSagemakerClient(),
                primary_output_manifest_contents=primary_output_manifest_contents,
                labels=general.DEFAULT_LABELS,
                annotator_metadata=None,
                args=MockParserArgs(blind1_labeling_job_name='mock_job_name', blind2_labeling_job_name='mock_job_name')
            )
            self.assertListEqual(expected_manifest_json_list, received_manifest_json_list)

        with open('test/unit/resources/output_with_expired_tasks_manifest.jsonl', 'r') as f:
            primary_output_manifest_contents = f.read()

            expected_manifest_json_list = [
                {
                    "source-ref": "s3://bucket/documents/file2.pdf",
                    "page": "1",
                    "metadata": {"pages": "1", "use-textract-only": False, "labels": ["PER", "LOC", "ORG", "FAC", "BRAND", "COMM", "TITLE:MOVIE", "TITLE:MUSIC", "TITLE:BOOK", "TITLE:SOFT", "TITLE:GAME", "TITLE:OTHER", "PERSON:TITLE", "QUANT", "IDENTITY", "OTHER"]},
                    "annotator-metadata": None,
                    "primary-annotation-ref": "s3://bucket/annotations/file2-1-12345678-ann.json",
                    "secondary-annotation-ref": "s3://bucket/annotations/file2-1-23456789-ann.json"
                },
                {
                    "source-ref": "s3://bucket/documents/file3.pdf",
                    "page": "1",
                    "metadata": {"pages": "1", "use-textract-only": False, "labels": ["PER", "LOC", "ORG", "FAC", "BRAND", "COMM", "TITLE:MOVIE", "TITLE:MUSIC", "TITLE:BOOK", "TITLE:SOFT", "TITLE:GAME", "TITLE:OTHER", "PERSON:TITLE", "QUANT", "IDENTITY", "OTHER"]},
                    "annotator-metadata": None,
                    "primary-annotation-ref": "s3://bucket/annotations/file3-1-12345678-ann.json",
                    "secondary-annotation-ref": "s3://bucket/annotations/file3-1-23456789-ann.json"
                }
            ]
            received_manifest_json_list = comprehend_ssie_annotation_tool_cli.generate_manifest_jsons_from_previous_jobs(
                s3_client=MockS3Client(),
                sagemaker_client=MockSagemakerClient(),
                primary_output_manifest_contents=primary_output_manifest_contents,
                labels=general.DEFAULT_LABELS,
                annotator_metadata=None,
                args=MockParserArgs(blind1_labeling_job_name='mock_job_name', only_include_expired_tasks=True)
            )
            self.assertListEqual(expected_manifest_json_list, received_manifest_json_list)
        

    def test_validate_and_download_from_input_s3_path(self):
        for input_s3_path in ['', '/', '//']:
            with self.assertRaises(ValueError) as cm:
                comprehend_ssie_annotation_tool_cli.validate_and_download_from_input_s3_path(
                    s3_client=MockS3Client(),
                    ssie_documents_s3_bucket='s3://bucket',
                    args=MockParserArgs(input_s3_path=input_s3_path)
                )
            self.assertEqual(
                'An input S3 path is required for NON-verification jobs, but none was given.',
                str(cm.exception)
            )

        with self.assertRaises(ValueError) as cm:
            comprehend_ssie_annotation_tool_cli.validate_and_download_from_input_s3_path(
                s3_client=MockS3Client(),
                ssie_documents_s3_bucket='s3://bucket',
                args=MockParserArgs(input_s3_path='s3://different_bucket/folder/documents/', create_input_manifest_only=False)
            )
        self.assertEqual(
            'Please specify a s3 input path under s3://bucket to start a labeling job. Current input bucket is different_bucket.',
            str(cm.exception)
        )

        with self.assertRaises(comprehend_ssie_annotation_tool_cli.NoFilesInDirectoryException):
            comprehend_ssie_annotation_tool_cli.validate_and_download_from_input_s3_path(
                s3_client=MockS3ClientGetNoObjectsFromS3(),
                ssie_documents_s3_bucket='s3://bucket',
                args=MockParserArgs(input_s3_path='s3://bucket/folder/documents/')
            )
        
        input_s3_path, local_dir = comprehend_ssie_annotation_tool_cli.validate_and_download_from_input_s3_path(
            s3_client=MockS3Client(),
            ssie_documents_s3_bucket='s3://bucket',
            args=MockParserArgs(input_s3_path='s3://bucket/folder/documents/')
        )
        self.assertEqual(len(get_all_files_in_directory(local_path=local_dir)), 3)
        self.assertEqual(input_s3_path, 's3://bucket/folder/documents')

        comprehend_ssie_annotation_tool_cli.cleanup(local_dir)

    def test_get_labels_from_schema_content(self):
        labels = ['label1', 'label2']
        content = json.dumps(GroundTruthAnnotationUIConfig(
            schemas=AnnotationUITaskSchemas(
                named_entity=NamedEntitiesAnnotationTaskConfig(
                    tags=labels
                )
            )
        ).dict())
        self.assertListEqual(labels, comprehend_ssie_annotation_tool_cli.get_labels_from_schema_content(content))

        none_contents = [
            json.dumps(GroundTruthAnnotationUIConfig(schemas=AnnotationUITaskSchemas(named_entity=NamedEntitiesAnnotationTaskConfig())).dict()),
            json.dumps(GroundTruthAnnotationUIConfig(schemas=AnnotationUITaskSchemas()).dict()),
            json.dumps(GroundTruthAnnotationUIConfig().dict()),
        ]
        for content in none_contents:
            self.assertListEqual(comprehend_ssie_annotation_tool_cli.get_labels_from_schema_content(content), [])

    def test_get_labels_and_manifest(self):
        labels = ['label1', 'label2']
        content = json.dumps(GroundTruthAnnotationUIConfig(
            schemas=AnnotationUITaskSchemas(
                named_entity=NamedEntitiesAnnotationTaskConfig(
                    tags=labels
                )
            )
        ).dict())
        received_schema_content, received_labels = comprehend_ssie_annotation_tool_cli.get_labels_and_manifest(content, args=MockParserArgs(schema_path='schema_path'))
        self.assertListEqual(labels, received_labels)

        received_schema_content, received_labels = comprehend_ssie_annotation_tool_cli.get_labels_and_manifest(content, args=MockParserArgs(entity_types=['type1', 'type2']))
        self.assertListEqual(['type1', 'type2'], received_labels)
        self.assertEqual(generate_schema_content(labels=['type1', 'type2']), received_schema_content)

        received_schema_content, received_labels = comprehend_ssie_annotation_tool_cli.get_labels_and_manifest(content, args=MockParserArgs())
        self.assertListEqual(general.DEFAULT_LABELS, received_labels)

    def test_generate_manifest_jsons_and_schema_for_verification_job(self):
        received_manifest_json_list, received_schema_content = comprehend_ssie_annotation_tool_cli.generate_manifest_jsons_and_schema_for_verification_job(
            sagemaker_client=MockSagemakerClient(),
            s3_client=MockS3Client(),
            ssie_documents_s3_bucket='s3://bucket',
            annotator_metadata=None,
            args=MockParserArgs(blind1_labeling_job_name='mock_job_name')
        )
        self.assertEqual(received_schema_content, generate_schema_content())
        self.assertEqual(received_manifest_json_list, generate_manifest_json_list(secondary_annotation_ref_none=True))

    def test_generate_manifest_jsons_and_schema_for_standard_job(self):
        received_manifest_json_list, received_schema_content = comprehend_ssie_annotation_tool_cli.generate_manifest_jsons_and_schema_for_standard_job(
            s3_client=MockS3Client(),
            ssie_documents_s3_bucket='s3://bucket',
            use_textract_only=False,
            annotator_metadata=None,
            args=MockParserArgs(input_s3_path='s3://bucket/documents')
        )
        self.assertEqual(received_schema_content, generate_schema_content())
        for obj in received_manifest_json_list:
            obj["primary-annotation-ref"] = None
            obj["secondary-annotation-ref"] = None
        self.assertEqual(received_manifest_json_list, generate_manifest_json_list(primary_annotation_ref_none=True, secondary_annotation_ref_none=True))

    def test_generate_manifest_jsons_and_schema(self):
        received_manifest_json_list, received_schema_content = comprehend_ssie_annotation_tool_cli.generate_manifest_jsons_and_schema(
            sagemaker_client=MockSagemakerClient(),
            s3_client=MockS3Client(),
            ssie_documents_s3_bucket='s3://bucket',
            annotator_metadata=None,
            args=MockParserArgs(blind1_labeling_job_name='mock_job_name'),
            use_textract_only=False
        )
        self.assertEqual(received_schema_content, generate_schema_content())
        self.assertEqual(received_manifest_json_list, generate_manifest_json_list(secondary_annotation_ref_none=True))

        received_manifest_json_list, received_schema_content = comprehend_ssie_annotation_tool_cli.generate_manifest_jsons_and_schema(
            sagemaker_client=MockSagemakerClient(),
            s3_client=MockS3Client(),
            ssie_documents_s3_bucket='s3://bucket',
            use_textract_only=False,
            annotator_metadata=None,
            args=MockParserArgs(input_s3_path='s3://bucket/documents')
        )
        self.assertEqual(received_schema_content, generate_schema_content())
        for obj in received_manifest_json_list:
            obj["primary-annotation-ref"] = None
            obj["secondary-annotation-ref"] = None
        self.assertEqual(received_manifest_json_list, generate_manifest_json_list(primary_annotation_ref_none=True, secondary_annotation_ref_none=True))
    
    def test_validate_groundtruth_labeling_job_name(self):
        received_labeling_job_name = comprehend_ssie_annotation_tool_cli.validate_groundtruth_labeling_job_name(
            sagemaker_client=MockSagemakerClient(),
            job_name_prefix='job_name',
            no_suffix=True
        )
        self.assertIsNone(received_labeling_job_name)

        class MockSagemakerClientDescribeLabelingJobException():
            """Mock Sagemaker client."""
            def describe_labeling_job(self, LabelingJobName: str):
                """Mock describe_labeling_job method."""
                raise Exception()
        received_labeling_job_name = comprehend_ssie_annotation_tool_cli.validate_groundtruth_labeling_job_name(
            sagemaker_client=MockSagemakerClientDescribeLabelingJobException(),
            job_name_prefix='job_name',
            no_suffix=False
        )
        self.assertTrue(received_labeling_job_name.startswith('job_name-labeling-job-'))
        received_labeling_job_name = comprehend_ssie_annotation_tool_cli.validate_groundtruth_labeling_job_name(
            sagemaker_client=MockSagemakerClientDescribeLabelingJobException(),
            job_name_prefix='job_name',
            no_suffix=True
        )
        self.assertEqual(received_labeling_job_name, 'job_name')
