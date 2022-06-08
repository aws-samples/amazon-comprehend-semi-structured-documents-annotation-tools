#!/usr/bin/env python
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
"""Script used to create a Sagemaker GroundTruth labeling job for semi-structured documents."""

import argparse
import json
import os
import shutil
import logging
import datetime
from typing import List
import uuid

import pdfplumber
import boto3

from utils.s3_helper import S3Client
from utils.output_manifest_utils import is_an_expired_task
from constants import general
from type.groundtruth_annotation_ui_config import GroundTruthAnnotationUIConfig, AnnotationUITaskSchemas, NamedEntitiesAnnotationTaskConfig
from type.semi_structured_input_manifest import SemiStructuredInputManifestObject
from type.semi_structured_output_manifest import SemiStructuredOutputManifestObject, SemiStructuredOutputManifestJobObject


logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)


class InvalidAnnotatorMetadataException(Exception):
    """Raise when invalid annotator metadata is given."""

    def __init__(self, message="Incorrect annotator metadata argument. Cannot parse a valid key/value pair."):
        """Initialize the custom exception."""
        super().__init__(message)


class NoFilesInDirectoryException(Exception):
    """Raise when local directory with files is empty."""

    def __init__(self, message="No semi-structured files have been found in input directory. Please re-enter a S3 path containing semi-structured files."):
        """Initialize the custom exception."""
        super().__init__(message)


def cleanup(temp_path: str):
    """Recursively delete all files and folders in path."""
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)


def validate_and_format_annotator_metadata(annotator_metadata_str: str):
    """Validate metadata passed in through arguments to be shown to the annotator."""
    annotator_metadata = dict()
    annotator_metadata_parts = annotator_metadata_str.split(',')
    if len(annotator_metadata_parts) >= 2:
        key_value_array = [annotator_metadata_parts[i:i + 2] for i in range(0, len(annotator_metadata_parts), 2)]
        try:
            for key_str, val_str in key_value_array:
                if not (key_str.lower().startswith('key=') and val_str.lower().startswith('value=')):
                    raise InvalidAnnotatorMetadataException()
                annotator_metadata[key_str.split('=')[1]] = val_str.split('=')[1]
        except ValueError:
            raise InvalidAnnotatorMetadataException()
        return annotator_metadata
    raise InvalidAnnotatorMetadataException()


def describe_sagemaker_labeling_job(sagemaker_client, job_name: str):
    """Call Sagemaker's describe-labeling-job API on a job."""
    labeling_job_response = sagemaker_client.describe_labeling_job(LabelingJobName=job_name)
    return labeling_job_response


def get_ui_template_root_from_sagemaker_job(sagemaker_client, job_name: str):
    """Return the UI template root in S3 of a sagemaker job."""
    labeling_job_response = describe_sagemaker_labeling_job(sagemaker_client, job_name)
    input_manifest_ref = labeling_job_response['InputConfig']['DataSource']['S3DataSource']['ManifestS3Uri']
    return os.path.splitext(os.path.basename(input_manifest_ref))[0]


def get_output_manifest_ref_from_sagemaker_job(sagemaker_client, job_name: str):
    """Return the S3 reference of a sagemaker job's output manifest."""
    labeling_job_response = describe_sagemaker_labeling_job(sagemaker_client, job_name)
    output_manifest_ref = labeling_job_response['LabelingJobOutput']['OutputDatasetS3Uri']
    return output_manifest_ref


def generate_manifest_jsons_from_previous_jobs(s3_client: S3Client, sagemaker_client, primary_output_manifest_contents: str,
                                               labels: List, annotator_metadata: dict, args):
    """Generate manifest json lines given previous jobs."""
    manifest_json_list = []
    if args.blind2_labeling_job_name:
        secondary_output_manifest_s3_ref = get_output_manifest_ref_from_sagemaker_job(sagemaker_client, args.blind2_labeling_job_name)
        secondary_output_manifest_contents = s3_client.get_object_content_from_s3(secondary_output_manifest_s3_ref)
        secondary_output_manifest_lines = secondary_output_manifest_contents.splitlines()

    for idx, manifest_line in enumerate(primary_output_manifest_contents.splitlines()):
        primary_output_manifest_json_obj = json.loads(manifest_line)
        primary_output_manifest_obj = SemiStructuredOutputManifestObject(json_obj=primary_output_manifest_json_obj, job_name=args.blind1_labeling_job_name)

        if args.only_include_expired_tasks:
            if not is_an_expired_task(primary_output_manifest_obj, args.blind1_labeling_job_name):
                continue

            primary_annotation_ref = getattr(primary_output_manifest_obj, "primary-annotation-ref")
            secondary_annotation_ref = getattr(primary_output_manifest_obj, "secondary-annotation-ref")
        else:
            primary_output_manifest_job_object = getattr(primary_output_manifest_obj, str(args.blind1_labeling_job_name), SemiStructuredOutputManifestJobObject({}))
            primary_annotation_ref = getattr(primary_output_manifest_job_object, "annotation-ref")

            secondary_output_manifest_json_obj = json.loads(secondary_output_manifest_lines[idx]) if args.blind2_labeling_job_name else {}
            secondary_output_manifest_obj = SemiStructuredOutputManifestObject(json_obj=secondary_output_manifest_json_obj, job_name=args.blind2_labeling_job_name)
            secondary_output_manifest_job_object = getattr(secondary_output_manifest_obj, str(args.blind2_labeling_job_name), SemiStructuredOutputManifestJobObject({}))
            secondary_annotation_ref = getattr(secondary_output_manifest_job_object, "annotation-ref")

        manifest_json = SemiStructuredInputManifestObject(
            source_ref=getattr(primary_output_manifest_obj, "source-ref"),
            page=primary_output_manifest_obj.page,
            metadata={
                **getattr(primary_output_manifest_obj, "metadata").to_dict(),
                "labels": labels
            },
            annotator_metadata=annotator_metadata,
            primary_annotation_ref=primary_annotation_ref,
            secondary_annotation_ref=secondary_annotation_ref
        )
        manifest_json_list.append(manifest_json.__dict__)
    return manifest_json_list


def validate_and_download_from_input_s3_path(s3_client: S3Client, ssie_documents_s3_bucket: str, args):
    """Validate the inputted S3 path and download the files locally. Returns the S3 and local paths."""
    input_s3_path = args.input_s3_path.rstrip('/')
    if not input_s3_path:
        raise ValueError('An input S3 path is required for NON-verification jobs, but none was given.')
    input_bucket, input_key = S3Client.bucket_key_from_s3_uri(input_s3_path)

    if ssie_documents_s3_bucket != input_bucket and not args.create_input_manifest_only:
        raise ValueError(f'Please specify a s3 input path under {ssie_documents_s3_bucket} to start a labeling job. '
                         f'Current input bucket is {input_bucket}.')

    local_dir = f"/tmp/{uuid.uuid4()}"

    s3_client.download_directory(local_dir_path=local_dir, bucket_name=input_bucket, s3_path_prefix=f'{input_key}/')

    if not os.path.isdir(local_dir):
        raise NoFilesInDirectoryException()

    print(f'Downloaded files to temp local directory {local_dir}')

    return (input_s3_path, local_dir)


def get_labels_from_schema_content(content: str) -> List[str]:
    """Return schemas.named_entity.tags value from GroundTruthAnnotationUIConfig object."""
    schema_obj = GroundTruthAnnotationUIConfig(**json.loads(content))
    return schema_obj.schemas.named_entity.tags


def get_labels_and_manifest(schema_content: str, args):
    """Get schema content and labels from cli argument or automated creation."""
    if args.schema_path:
        labels = get_labels_from_schema_content(schema_content)
    else:
        labels = general.DEFAULT_LABELS if not args.entity_types else args.entity_types
        schema_content = json.dumps(GroundTruthAnnotationUIConfig(
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
    return schema_content, labels


def generate_manifest_jsons_and_schema_for_verification_job(sagemaker_client, s3_client: S3Client, ssie_documents_s3_bucket: str, annotator_metadata: dict, args):
    """Generate manifest jsons and schema for a verification job."""
    primary_output_manifest_s3_ref = get_output_manifest_ref_from_sagemaker_job(sagemaker_client, args.blind1_labeling_job_name)
    primary_output_manifest_contents = s3_client.get_object_content_from_s3(primary_output_manifest_s3_ref)

    blind1_ui_template_root = get_ui_template_root_from_sagemaker_job(sagemaker_client, args.blind1_labeling_job_name)
    ui_schema_path_copy = f's3://{ssie_documents_s3_bucket}/comprehend-semi-structured-docs-ui-template/{blind1_ui_template_root}/ui-template/schema.json'
    print(f'Using UI schema path: {ui_schema_path_copy}')
    schema_content = s3_client.get_object_content_from_s3(ui_schema_path_copy)

    labels = get_labels_from_schema_content(schema_content)

    manifest_json_list = generate_manifest_jsons_from_previous_jobs(s3_client, sagemaker_client, primary_output_manifest_contents, labels, annotator_metadata, args)
    return manifest_json_list, schema_content


def generate_manifest_jsons_and_schema_for_standard_job(s3_client: S3Client, ssie_documents_s3_bucket: str, use_textract_only: bool, annotator_metadata: dict, args):
    """Generate manifest jsons and schema for a standard job."""
    input_s3_path, local_dir = validate_and_download_from_input_s3_path(s3_client, ssie_documents_s3_bucket, args)

    schema_content = s3_client.get_object_content_from_s3(args.schema_path) if args.schema_path else None
    schema_content, labels = get_labels_and_manifest(schema_content, args)

    # Generate input manifest file for each page of PDF.
    manifest_json_list = []
    for file_name in os.listdir(local_dir):
        file_path = os.path.join(local_dir, file_name)
        try:
            with pdfplumber.open(file_path) as pdf:
                for i in range(len(pdf.pages)):
                    manifest_json_obj = SemiStructuredInputManifestObject(
                        source_ref=f'{input_s3_path}/{file_name}',
                        page=f'{i+1}',
                        metadata={
                            "pages": f'{len(pdf.pages)}',
                            "use-textract-only": use_textract_only,
                            "labels": labels
                        },
                        annotator_metadata=annotator_metadata
                    )
                    manifest_json_list.append(manifest_json_obj.__dict__)
        except Exception:
            print(f'Unreadable file: {file_path}. Skipping.')
            continue

    cleanup(local_dir)
    print(f'Deleted downloaded temp files from {local_dir}')

    return manifest_json_list, schema_content


def generate_manifest_jsons_and_schema(s3_client: S3Client, sagemaker_client, ssie_documents_s3_bucket: str,
                                       annotator_metadata: dict, use_textract_only: bool, args):
    """Create manifests and schema for the annotation job."""
    if args.blind1_labeling_job_name:
        manifest_json_list, schema_content = generate_manifest_jsons_and_schema_for_verification_job(sagemaker_client, s3_client, ssie_documents_s3_bucket, annotator_metadata, args)
    else:
        manifest_json_list, schema_content = generate_manifest_jsons_and_schema_for_standard_job(s3_client, ssie_documents_s3_bucket, use_textract_only, annotator_metadata, args)
    return (manifest_json_list, schema_content) if manifest_json_list else None


def validate_groundtruth_labeling_job_name(sagemaker_client, job_name_prefix: str, no_suffix: bool):
    """Generate and validate GT labeling job name uniqueness."""
    now = datetime.datetime.utcnow()
    now_str = now.strftime('%Y%m%dT%H%M%S')
    if no_suffix:
        ground_truth_labeling_job_name = job_name_prefix
        try:
            describe_sagemaker_labeling_job(sagemaker_client, ground_truth_labeling_job_name)
            return None
        except Exception:
            pass
    else:
        ground_truth_labeling_job_name = f'{job_name_prefix}-labeling-job-{now_str}'
    return ground_truth_labeling_job_name


def main():
    """To be run when script called."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-s3-path', type=str, help='S3 path that contains PDFs to annotate.')
    parser.add_argument('--cfn-name', type=str, required=True, help='CloudFormation stack deployed by comprehend-semi-structured-documents-annotation-template.')
    parser.add_argument('--work-team-name', type=str, required=True, help='Work team name the Ground Truth labeling job will be assigned to.')
    parser.add_argument('--entity-types', type=lambda s: [item.strip() for item in s.split(',')],
                        help='A list of entity types for annotation. Usage: --entity-types "TYPE1, TYPE2" ')
    parser.add_argument('--region', type=str, default='us-west-2', help='AWS region of resources created. (Default: us-west-2)')
    parser.add_argument('--create-input-manifest-only', action='store_true', help='option to create and upload input manifest only.')
    parser.add_argument('--use-textract-only', action='store_true', help='Flag to indicate to only use Amazon Textract to analyze PDF document')
    parser.add_argument('--job-name-prefix', type=str, default='comprehend-semi-structured-docs', help='GroundTruth labeling job prefix. (Default: comprehend-semi-structured-docs)')
    parser.add_argument('--annotator-metadata', type=str, help='Metadata to expose in the annotator UI. \
        Usage: --annotator-metadata "key=Info,value=Sample information,key=Due Date,value=Sample date value 12/12/1212" ')
    parser.add_argument('--blind1-labeling-job-name', type=str, help='Blind1 labeling job name to use for verification. Usage: --blind1-labeling-job-name sagemaker-labeling-job-123')
    parser.add_argument('--blind2-labeling-job-name', type=str, help='Blind2 labeling job name to use for verification. Usage: --blind2-labeling-job-name sagemaker-labeling-job-123')
    parser.add_argument('--only-include-expired-tasks', action='store_true', help='Flag to indicate to only include tasks which had expired (only available for use in conjunction with a verification job). \
        Usage: --blind1-labeling-job-name labeling-job-123 --only-include-expired-tasks')
    parser.add_argument('--schema-path', type=str, help='Local path to schema file to use for the labeling job which will overwrite --entity-types. Usage: --annotation-schema /home/user1/schema.json')
    parser.add_argument('--no-suffix', action='store_true', help='Flag to indicate to only use job-name-prefix for job name. Otherwise, a unique ID will be appended to the job-name-prefix.')
    parser.add_argument('--task-time-limit', type=int, default=3600, help='Time limit in seconds given for each task (default: 3600). Usage (for 2 hours): --task-time-limit 7200')
    parser.add_argument('--task-availability-time-limit', type=int, default=864000, help='Time availability time limit in seconds given for each task (default: 864000). \
        Usage (for 30 days): --task-availability-time-limit 2592000')

    args = parser.parse_args()

    region = args.region
    use_textract_only = args.use_textract_only

    annotator_metadata_str = args.annotator_metadata
    annotator_metadata = validate_and_format_annotator_metadata(annotator_metadata_str) if annotator_metadata_str else None

    session = boto3.Session(region_name=region)
    sts = session.client('sts')
    cfn = session.client('cloudformation')

    account_id = sts.get_caller_identity().get('Account')
    workteam_arn = f"arn:aws:sagemaker:{region}:{account_id}:workteam/private-crowd/{args.work_team_name}"

    try:
        output = cfn.describe_stacks(StackName=args.cfn_name)
        for output in output['Stacks'][0]['Outputs']:
            if output['OutputKey'] == 'SemiStructuredDocumentsS3Bucket':
                ssie_documents_s3_bucket = output['OutputValue']
            if output['OutputKey'] == 'GTAnnotationConsolidationLambdaFunctionName':
                gt_annotation_consolidation_lambda_function = output['OutputValue']
            if output['OutputKey'] == 'GTPreHumanTaskLambdaFunctionName':
                gt_pre_human_task_lambda_function = output['OutputValue']
            if output['OutputKey'] == 'SageMakerRoleARN':
                sagemaker_role_arn = output['OutputValue']
    except Exception as e:
        print(e)
        if not args.create_input_manifest_only:
            print('Please enter correct cloudformation stack name deployed by comprehend-semi-structured-documents-annotation-template.')
            return
    s3_client = S3Client()
    sagemaker_client = session.client('sagemaker')

    ground_truth_labeling_job_name = validate_groundtruth_labeling_job_name(sagemaker_client=sagemaker_client, job_name_prefix=args.job_name_prefix, no_suffix=args.no_suffix)
    if not ground_truth_labeling_job_name:
        print(f'{ground_truth_labeling_job_name} already exists. Please use a different job name.')
        return

    generated_data = generate_manifest_jsons_and_schema(
        s3_client,
        sagemaker_client,
        ssie_documents_s3_bucket,
        annotator_metadata,
        use_textract_only,
        args
    )
    if not generated_data:
        print('Data could not be extracted. Exiting.')
        return
    manifest_json_list, schema_content = generated_data

    # Upload input manifest
    input_manifest_path = f's3://{ssie_documents_s3_bucket}/input-manifest/{ground_truth_labeling_job_name}.manifest'
    print('Uploading input manifest file.')
    s3_client.write_jsonl(s3_path=input_manifest_path, rows=manifest_json_list)

    if args.create_input_manifest_only:
        return

    # Upload ui artifacts
    ui_template_s3_path_prefix = f'comprehend-semi-structured-docs-ui-template/{ground_truth_labeling_job_name}'
    local_ui_template_directory_name = 'ui-template'
    s3_client.upload_directory(local_dir_path=local_ui_template_directory_name, bucket_name=ssie_documents_s3_bucket, s3_path_prefix=ui_template_s3_path_prefix)

    # Upload the annotation schema file
    ui_schema_path = f's3://{ssie_documents_s3_bucket}/{ui_template_s3_path_prefix}/{local_ui_template_directory_name}/schema.json'
    print('Uploading schema file.')
    s3_client.write_content(content=schema_content, s3_path=ui_schema_path)

    # Upload the UI template file
    ui_template_path = f's3://{ssie_documents_s3_bucket}/{ui_template_s3_path_prefix}/{local_ui_template_directory_name}/template-2021-04-15.liquid'
    with open(f'./{local_ui_template_directory_name}/index.html', encoding='utf-8') as template:
        template_file = template.read().replace('TO_BE_REPLACE', f'{ssie_documents_s3_bucket}/{ui_template_s3_path_prefix}')
        print('Uploading template UI.')
        s3_client.write_content(content=template_file, s3_path=ui_template_path)

    # Start a Sagemaker Groundtruth labeling job
    human_task_config = {
        'WorkteamArn': workteam_arn,
        'UiConfig': {
            'UiTemplateS3Uri': ui_template_path
        },
        'PreHumanTaskLambdaArn': gt_pre_human_task_lambda_function,
        'TaskTitle': ground_truth_labeling_job_name,
        'TaskDescription': ground_truth_labeling_job_name,
        'NumberOfHumanWorkersPerDataObject': 1,
        'TaskTimeLimitInSeconds': args.task_time_limit,
        'TaskAvailabilityLifetimeInSeconds': args.task_availability_time_limit,
        'AnnotationConsolidationConfig': {
            'AnnotationConsolidationLambdaArn': gt_annotation_consolidation_lambda_function
        }
    }
    start_labeling_job_request = {'LabelingJobName': f'{ground_truth_labeling_job_name}',
                                  'LabelAttributeName': f'{ground_truth_labeling_job_name}',
                                  'InputConfig': {
                                      'DataSource': {
                                          'S3DataSource': {
                                              'ManifestS3Uri': input_manifest_path
                                          }
                                      }
                                  },
                                  'OutputConfig': {
                                      'S3OutputPath': f's3://{ssie_documents_s3_bucket}/output/'
                                  },
                                  'RoleArn': sagemaker_role_arn,
                                  'StoppingConditions': {
                                      "MaxPercentageOfInputDatasetLabeled": 100
                                  },
                                  'HumanTaskConfig': human_task_config}

    result = sagemaker_client.create_labeling_job(**start_labeling_job_request)
    print("Sagemaker GroundTruth Labeling Job submitted: {}".format(result["LabelingJobArn"]))


if __name__ == "__main__":
    main()
