#!/usr/bin/env python
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pdfplumber
import argparse
import json
import os
import io
import logging
import boto3
import datetime
from typing import List, Dict
from urllib.parse import urlparse
import uuid
import mimetypes
import shutil


logging.basicConfig(level=logging.WARN)
logger = logging.getLogger(__name__)
"""
Script running on cloud dev desktop to pre-upload annotation data to dynamodb and s3 for testing purpose.
"""

DEFAULT_LABELS = ["PER", "LOC", "ORG", "FAC", "BRAND", "COMM", "TITLE:MOVIE", "TITLE:MUSIC", "TITLE:BOOK", "TITLE:SOFT",
              "TITLE:GAME", "TITLE:OTHER", "PERSON:TITLE", "QUANT", "IDENTITY", "OTHER"]

def parse_s3_url(s3_input_path: str):
    """ Parse s3 url to get bucket name and key """
    parsed_s3_url = urlparse(s3_input_path)
    bucket_name = parsed_s3_url.netloc
    key = parsed_s3_url.path.strip('/')
    return bucket_name, key


def write_jsonl(path: str, rows: List[Dict], s3_client=None):
    """
    Writes the passed in list of dictionaries to the given local or s3 path as JSONL

    :param path: Path to write to. Accepts local filesystem and S3 paths
    :param rows: List of dictionaries to encode as JSONL
    :param boto3_session: boto3 session.  required for writing to S3 paths
    :return:
    """
    content = ''.join([f'{json.dumps(row)}\n' for row in rows])
    upload_object(content, path, s3_client)


def upload_object(content, path, s3_client):
    content_bytes = content if type(content) == bytes else content.encode('utf-8')
    bucket, key = parse_s3_url(path)
    s3_client.upload_fileobj(io.BytesIO(content_bytes), bucket, key)


def upload_directory(path, bucket_name, s3_path_prefix, s3_client):
    for root, dirs, files in os.walk(path):
        for file in files:
            extra_args = {}
            content_type = mimetypes.guess_type(file)[0]
            if content_type:
                extra_args['ContentType'] = content_type
            s3_client.upload_file(os.path.join(root, file), bucket_name, f'{s3_path_prefix}/{root}/{file}', ExtraArgs=extra_args)


def cleanup(temp_path):
    if os.path.exists(temp_path):
        shutil.rmtree(temp_path)


def validate_and_format_annotator_metadata(annotator_metadata_str: str):
    """Validate metadata passed in through arguments to be shown to the annotator."""
    annotator_metadata = dict()
    annotator_metadata_parts = annotator_metadata_str.split(',')
    if len(annotator_metadata_parts) >= 2:
        key_value_array = [annotator_metadata_parts[i:i+2] for i in range(0, len(annotator_metadata_parts), 2)]
        for key_str, val_str in key_value_array:
            if not (key_str.lower().startswith('key=') and val_str.lower().startswith('value=')):
                raise ValueError('Incorrect annotator metadata argument')
            annotator_metadata[key_str.split('=')[1]] = val_str.split('=')[1]
        return annotator_metadata
    return ValueError('Incorrect annotator metadata argument')


def get_s3_file_contents(s3_client, s3_ref):
    """Return contents from an S3 file decoded with utf-8"""
    bucket, key = parse_s3_url(s3_ref)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    payload = response['Body'].read().decode('utf-8')
    return payload


def describe_sagemaker_labeling_job(sagemaker_client, job_name):
    """Call Sagemaker's describe-labeling-job API on a job."""
    labeling_job_response = sagemaker_client.describe_labeling_job(LabelingJobName=job_name)
    return labeling_job_response


def get_ui_template_root_from_sagemaker_job(sagemaker_client, job_name):
    """Return the UI template root in S3 of a sagemaker job."""
    labeling_job_response = describe_sagemaker_labeling_job(sagemaker_client, job_name)
    input_manifest_ref = labeling_job_response['InputConfig']['DataSource']['S3DataSource']['ManifestS3Uri']
    return os.path.splitext(os.path.basename(input_manifest_ref))[0]


def get_output_manifest_ref_from_sagemaker_job(sagemaker_client, job_name):
    """Return the S3 reference of a sagemaker job's output manifest."""
    labeling_job_response = describe_sagemaker_labeling_job(sagemaker_client, job_name)
    output_manifest_ref = labeling_job_response['LabelingJobOutput']['OutputDatasetS3Uri']
    return output_manifest_ref


def generate_manifests_from_previous_jobs(s3_client, sagemaker_client, primary_output_manifest_contents,
                                          labels, annotator_metadata, args):
    """"""
    manifest_json_list = []
    if args.blind2_labeling_job_name:
        secondary_output_manifest_s3_ref = get_output_manifest_ref_from_sagemaker_job(sagemaker_client, args.blind2_labeling_job_name)
        secondary_output_manifest_contents = get_s3_file_contents(s3_client, secondary_output_manifest_s3_ref)
        secondary_output_manifest_lines = secondary_output_manifest_contents.splitlines()

    for idx, manifest_line in enumerate(primary_output_manifest_contents.splitlines()):
        primary_output_manifest_obj = json.loads(manifest_line)
        primary_output_manifest_obj['metadata']['labels'] = labels
        primary_annotation_ref = primary_output_manifest_obj[args.blind1_labeling_job_name]['annotation-ref']

        secondary_output_manifest_obj = json.loads(secondary_output_manifest_lines[idx]) if args.blind2_labeling_job_name else {}
        secondary_annotation_ref = secondary_output_manifest_obj[args.blind2_labeling_job_name]['annotation-ref'] \
                                    if secondary_output_manifest_obj else None

        manifest_json = {
            'source-ref': primary_output_manifest_obj['source-ref'],
            'page': primary_output_manifest_obj['page'],
            'metadata': primary_output_manifest_obj['metadata'],
            'annotator-metadata': annotator_metadata,
            'primary-annotation-ref': primary_annotation_ref,
            'secondary-annotation-ref': secondary_annotation_ref
        }
        manifest_json_list.append(manifest_json)
    return manifest_json_list


def validate_and_download_from_input_s3_path(s3_resource, ssie_documents_s3_bucket, args):
    """Validate the inputted S3 path and download the files locally. Returns the S3 and local paths."""
    input_s3_path = args.input_s3_path.rstrip('/')
    if not input_s3_path:
        raise ValueError('An input S3 path is required for NON-arbitration/iteration jobs, but none was given.')
    input_bucket, input_key = parse_s3_url(input_s3_path)

    if ssie_documents_s3_bucket != input_bucket and not args.create_input_manifest_only:
        print(f'Please specify a s3 input path under {ssie_documents_s3_bucket} to start a labeling job. '
            f'Current input bucket is {input_bucket} ')
        return None

    local_dir = '/tmp/{}'.format(uuid.uuid4())
    bucket = s3_resource.Bucket(input_bucket)

    for obj in bucket.objects.filter(Prefix=input_key):
        if obj.key.endswith('.pdf'):
            target = obj.key if local_dir is None \
                else os.path.join(local_dir, os.path.basename(obj.key))
            if not os.path.exists(os.path.dirname(target)):
                os.makedirs(os.path.dirname(target))
            bucket.download_file(obj.key, target)

    if not os.path.isdir(local_dir):
        print('No pdf files have been found in input directory. Please re-enter a S3 path contains PDF files.')
        return None
    print(f'Downloaded files to temp local directory {local_dir}')

    return (input_s3_path, local_dir)


def generate_manifests_and_schema(s3_client, s3_resource, sagemaker_client, ssie_documents_s3_bucket,
                                annotator_metadata, use_textract_only, args):
    if args.blind1_labeling_job_name:
        primary_output_manifest_s3_ref = get_output_manifest_ref_from_sagemaker_job(sagemaker_client, args.blind1_labeling_job_name)
        primary_output_manifest_contents = get_s3_file_contents(s3_client, primary_output_manifest_s3_ref)

        blind1_ui_template_root = get_ui_template_root_from_sagemaker_job(sagemaker_client, args.blind1_labeling_job_name)
        ui_schema_path_copy = f's3://{ssie_documents_s3_bucket}/comprehend-semi-structured-docs-ui-template/' \
                                f'{blind1_ui_template_root}/ui-template/schema.json'
        print(f'Using UI schema path: {ui_schema_path_copy}')
        schema_content = get_s3_file_contents(s3_client, ui_schema_path_copy)

        labels = json.loads(schema_content)['schemas']['named_entity']['tags']

        manifest_json_list = generate_manifests_from_previous_jobs(s3_client, sagemaker_client, primary_output_manifest_contents,
                                                                   labels, annotator_metadata, args)
    else:
        paths = validate_and_download_from_input_s3_path(s3_resource, ssie_documents_s3_bucket, args)
        if not paths:
            return None
        input_s3_path, local_dir = paths

        labels = DEFAULT_LABELS if not args.entity_types else args.entity_types
        schema_content = json.dumps({"version": "SSIE_NER_SCHEMA_2021-04-15",
                        "schemas": {
                            "named_entity": {
                                "annotation_task": "NER",
                                "tags": labels,
                                "properties": []}},
                        "exported_time": "2021-04-15T17:34:34.493Z",
                        "uuid": "f44b0438-72ac-43ac-bdc7-5727914522b9"
                        })

        # Generate input manifest file for each page of PDF.
        manifest_json_list = []
        for file_name in os.listdir(local_dir):
            file_path = os.path.join(local_dir, file_name)
            with pdfplumber.open(file_path) as pdf:
                for i in range(len(pdf.pages)):
                    manifest_json = {
                        'source-ref': f'{input_s3_path}/{file_name}',
                        'page': f'{i+1}',
                        'metadata': {
                            'pages': f'{len(pdf.pages)}',
                            'use-textract-only': use_textract_only,
                            'labels': labels
                        },
                        'annotator-metadata': annotator_metadata,
                    }
                    manifest_json_list.append(manifest_json)

        cleanup(local_dir)
        print(f'Deleted downloaded temp files from {local_dir}')

    return manifest_json_list, schema_content


def main():
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
    parser.add_argument('--annotator-metadata', type=str, help='Metadata to expose in the annotator UI. Usage: --annotator-metadata "key=Info,value=Sample information,key=Due Date,value=Sample date value 12/12/1212" ')
    parser.add_argument('--blind1-labeling-job-name', type=str, help='Blind1 labeling job name to use for arbitration or iteration')
    parser.add_argument('--blind2-labeling-job-name', type=str, help='Blind2 labeling job name to use for arbitration')

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
    s3_client = session.client('s3')
    s3_resource = session.resource('s3')
    sagemaker_client = session.client('sagemaker')

    generated_data = generate_manifests_and_schema(
        s3_client,
        s3_resource,
        sagemaker_client,
        ssie_documents_s3_bucket,
        annotator_metadata,
        use_textract_only,
        args
    )
    if not generated_data:
        return
    manifest_json_list, schema_content = generated_data

    now = datetime.datetime.utcnow()
    now_str = now.strftime('%Y%m%dT%H%M%S')
    job_name_prefix = args.job_name_prefix
    ground_truth_labeling_job_name =  f'{job_name_prefix}-labeling-job-{now_str}'
    input_manifest_path = f's3://{ssie_documents_s3_bucket}/input-manifest/{ground_truth_labeling_job_name}.manifest'
    write_jsonl(
        input_manifest_path,
        manifest_json_list,
        s3_client
    )

    print(f'Uploaded input manifest file to {input_manifest_path}')
    if args.create_input_manifest_only:
        return

    # Uploade ui artifacts
    ui_template_s3_path_prefix = f'comprehend-semi-structured-docs-ui-template/{ground_truth_labeling_job_name}'
    local_ui_template_directory_name = 'ui-template'
    upload_directory(local_ui_template_directory_name, ssie_documents_s3_bucket, ui_template_s3_path_prefix, s3_client)

    # Upload the annotation schema file
    ui_schema_path = f's3://{ssie_documents_s3_bucket}/{ui_template_s3_path_prefix}/{local_ui_template_directory_name}/schema.json'
    upload_object(schema_content, ui_schema_path, s3_client)
    print(f'Uploaded schema file to {ui_schema_path}')

    # Upload the UI template file
    ui_template_path = f's3://{ssie_documents_s3_bucket}/{ui_template_s3_path_prefix}/{local_ui_template_directory_name}/template-2021-04-15.liquid'
    with open(f'./{local_ui_template_directory_name}/index.html', encoding='utf-8') as template:
        template_file = template.read().replace('TO_BE_REPLACE', f'{ssie_documents_s3_bucket}/{ui_template_s3_path_prefix}')
        upload_object(template_file, ui_template_path, s3_client)
        print(f'Uploaded template UI to {ui_template_path}')

    # Start a Sagemaker Groundtruth labeling job
    human_task_config = {
        'WorkteamArn': workteam_arn,
        'UiConfig': {
            'UiTemplateS3Uri': ui_template_path
        },
        'PreHumanTaskLambdaArn': gt_pre_human_task_lambda_function,
        'TaskTitle': f'{job_name_prefix}-labeling-job-task-{now_str}',
        'TaskDescription': f'{job_name_prefix}-labeling-job-task-{now_str}',
        'NumberOfHumanWorkersPerDataObject': 1,
        'TaskTimeLimitInSeconds': 3600,
        'TaskAvailabilityLifetimeInSeconds': 864000,
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
