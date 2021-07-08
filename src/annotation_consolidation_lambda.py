# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Annotation Consolidation lambda."""
import json
import os
from datetime import datetime
from s3_helper import S3Client
from typing import List, Dict
import uuid


def get_blocks_from_s3_ref(blocks_s3_ref: str, s3_client) -> list:
    """Return a list of blocks from an S3 reference file."""
    blocks = json.loads(s3_client.get_object_from_s3(blocks_s3_ref))
    for b in blocks:
        if 'parentBlockIndex' in b:
            del b['parentBlockIndex']
        if 'blockIndex' in b:
            del b['blockIndex']
    return blocks


def get_doc_metadata(doc_metadata: dict) -> dict:
    """Return the document metadata object with the Page property parsed out."""
    doc_metadata.pop("Page", None)
    return doc_metadata


def get_annotations(annotation_map, s3_client):
    """Return a transformed annotations to be written."""
    blocks_entities_map = json.loads(annotation_map)
    page = blocks_entities_map.get("DocumentMetadata", {}).get("Page", "unknown")
    return {
        "Version": blocks_entities_map.get("Version", "unknown"),
        "DocumentType": blocks_entities_map.get("DocumentType", "unknown"),
        "DocumentMetadata": get_doc_metadata(blocks_entities_map.get("DocumentMetadata", {})),
        "Blocks": get_blocks_from_s3_ref(blocks_entities_map.get('BlocksS3Ref', ''), s3_client),
        "Entities": blocks_entities_map.get('Entities', [])
    }, page


def get_annotation_file_path(s3_ref: str, file_name: str, page: int):
    """Generate an annotation file path."""
    s3_output_path_prefix, iteration = os.path.split(os.path.split(s3_ref)[0])
    return f"{os.path.split(s3_output_path_prefix)[0]}/consolidation-response/{iteration}/annotations/" \
        f"{S3Client.remove_extension(file_name)}-{page}-{str(uuid.uuid4())[:8]}-ann.json"


def write_annotations(annotations: Dict, annotation_file_path: str, s3_client):
    """Given a s3 path of consolidation request json, return a s3 folder we use to save annotations."""
    s3_client.write_content(annotation_file_path, json.dumps(annotations))
    print(f"Wrote annotations to {annotation_file_path}")


def do_consolidation(payload, s3_client, labeling_job_arn, label_attribute_name, s3_ref):
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
                    annotations, page = get_annotations(annotation_map["document"], s3_client)
                    file_name = os.path.split(data_object_response["dataObject"]["s3Uri"])[1]
                    annotation_file_path = get_annotation_file_path(s3_ref, file_name, page)
                    #  write annotation file
                    write_annotations(annotations, annotation_file_path, s3_client)

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
            "roleArn" : "string",
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

    role_arn = None
    if "roleArn" in event:
        role_arn = event["roleArn"]

    # Create s3 client object
    s3_client = S3Client(role_arn)

    payload = event["payload"]

    s3_ref = None
    if "s3Uri" in payload:
        s3_ref = payload["s3Uri"]
        payload = json.loads(s3_client.get_object_from_s3(s3_ref))

    # Perform consolidation
    return do_consolidation(payload, s3_client, labeling_job_arn, label_attribute_name, s3_ref)
