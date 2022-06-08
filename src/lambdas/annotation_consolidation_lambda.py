# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Annotation Consolidation lambda."""
import json
import os
from datetime import datetime
from typing import List, Dict
import uuid

from utils.s3_helper import S3Client
from type.semi_structured_annotation import SemiStructuredAnnotation


def remove_block_indices_from_blocks(blocks: List[dict]):
    """Remove parentBlockIndex and blockIndex from blocks."""
    for b in blocks:
        if 'parentBlockIndex' in b:
            del b['parentBlockIndex']
        if 'blockIndex' in b:
            del b['blockIndex']
    return blocks


def get_blocks_from_s3_ref(blocks_s3_ref: str, s3_client: S3Client) -> List:
    """Return a list of blocks from an S3 reference file."""
    blocks = [] if not blocks_s3_ref else json.loads(s3_client.get_object_content_from_s3(blocks_s3_ref))
    return remove_block_indices_from_blocks(blocks)


def get_annotation_obj(annotation_obj_str: str, s3_client: S3Client, file_name: str) -> SemiStructuredAnnotation:
    """Return a transformed annotations object to be written."""
    annotation_obj = SemiStructuredAnnotation(**json.loads(annotation_obj_str))
    annotation_obj.Blocks = get_blocks_from_s3_ref(annotation_obj.BlocksS3Ref, s3_client)
    page_number = annotation_obj.DocumentMetadata.PageNumber
    annotation_file_name = get_annotation_file_name(file_name, page_number)
    annotation_obj.File = annotation_file_name
    return annotation_obj


def get_annotation_file_name(file_name: str, page_number: str):
    """Generate an annotation file name."""
    return f"{S3Client.remove_extension(file_name)}-{str(page_number)}-{str(uuid.uuid4())[:8]}-ann.json"


def get_annotation_file_path(s3_ref: str, annotation_file_name: str):
    """Generate an annotation file path."""
    s3_output_path_prefix, iteration = os.path.split(os.path.split(s3_ref)[0])
    return f"{os.path.split(s3_output_path_prefix)[0]}/consolidation-response/{iteration}/annotations/{annotation_file_name}"


def write_annotations(annotations_obj: SemiStructuredAnnotation, annotation_file_path: str, s3_client: S3Client):
    """Given a s3 path of consolidation request json, return a s3 folder we use to save annotations."""
    s3_client.write_content(s3_path=annotation_file_path, content=json.dumps(annotations_obj.dict()))
    print(f"Wrote annotations to {annotation_file_path}")


def do_consolidation(payload: List[Dict], s3_client: S3Client, labeling_job_arn: str, label_attribute_name: str, s3_ref: str):
    """Consolidation methods for converting gt output to uf format."""
    consolidated_output = []

    for data_object_response in payload:
        try:
            # data_object_response: annotations from workers for a document.
            annotation_list: List[dict] = data_object_response["annotations"]
            annotation_file_path = ""
            # contains one worker per labeling job
            if annotation_list:
                annotation_map = json.loads(
                    annotation_list[0]["annotationData"]["content"]
                )
                if "document" in annotation_map:  # Workers can return empty responses
                    file_name = os.path.split(data_object_response["dataObject"]["s3Uri"])[1]
                    annotations_obj = get_annotation_obj(annotation_map["document"], s3_client, file_name)
                    annotation_file_path = get_annotation_file_path(s3_ref, annotations_obj.File)
                    #  write annotation file
                    write_annotations(annotations_obj, annotation_file_path, s3_client)

            #  return reference to s3
            response = {
                "datasetObjectId": data_object_response["datasetObjectId"],
                "consolidatedAnnotation": {
                    "content": {
                        label_attribute_name: {
                            "annotation-ref": annotation_file_path
                        },
                        label_attribute_name + "-metadata": {
                            "job-name": labeling_job_arn.split(":")[5],
                            "type": "groundtruth/pdf-ner",
                            "creation-date": datetime.utcnow().isoformat(),
                            "human-annotated": "yes",
                        },
                    }
                },
            }

            # Append individual data object response to the list of responses.
            if response is not None:
                consolidated_output.append(response)

        except Exception as e:
            print(f'An Error occurred in do_consolidation function: {e}')

    print(consolidated_output)

    return consolidated_output


def lambda_handler(event, context):
    """
    Sample Annotation Consolidation Lambda for custom labeling jobs.
    It takes worker's response for the item to be labeled, and output a consolidated annotation.

    Parameters
    ----------
    event: dict, required
        Content of an example event
        {
            "version": "2018-10-16",
            "labelingJobArn": <labelingJobArn>,
            "labelCategories": [<string>],  # If you created labeling job using aws console, labelCategories will be null
            "labelAttributeName": <string>,
            "roleArn" : "string", # SageMakerExecutionRole
            "payload": {
                "s3Uri": <string>
            }
            "outputConfig":"s3://<consolidated_output configured for labeling job>"
         }
        Content of payload.s3Uri
        [
            {
                "datasetObjectId": <string>,
                "dataObject": {
                    "s3Uri": <string>,
                    "content": <string>
                },
                "annotations": [{
                    "workerId": <string>,
                    "annotationData": {
                        "content": <string>,
                        "s3Uri": <string>
                    }
               }]
            }
        ]
        As SageMaker product evolves, content of event object & payload.s3Uri will change. For a latest version refer following URL
        Event doc: https://docs.aws.amazon.com/sagemaker/latest/dg/sms-custom-templates-step3.html
    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    -------
    consolidated_output: dict
        AnnotationConsolidation
        [
           {
                "datasetObjectId": <string>,
                "consolidatedAnnotation": {
                    "content": {
                        "<labelattributename>": {
                            # ... label content
                        }
                    }
                }
            }
        ]
        Return doc: https://docs.aws.amazon.com/sagemaker/latest/dg/sms-custom-templates-step3.html

    """
    # Event received
    print("Received event: " + json.dumps(event, indent=2))
    labeling_job_arn = event["labelingJobArn"]
    label_attribute_name = event["labelAttributeName"]

    payload = event["payload"]

    s3_client = S3Client()

    s3_ref = None
    if "s3Uri" in payload:
        s3_ref = payload["s3Uri"]
        payload = json.loads(s3_client.get_object_content_from_s3(s3_ref))

    # Perform consolidation
    return do_consolidation(payload, s3_client, labeling_job_arn, label_attribute_name, s3_ref)
