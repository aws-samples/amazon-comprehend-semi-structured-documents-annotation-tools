# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Textract helper class."""
import boto3
import os


class TextractClient(object):
    """Helper Class for Textract operations."""

    def __init__(self):
        """
        Initialize the Textract client.
        :return:
        """
        session = boto3.Session(region_name=os.environ.get('AWS_REGION'))
        self.textract_client = session.client('textract')

    def detect_document_text(self, byte_array):
        """Call Textract's detect_document_text method and return the payload."""
        response = self.textract_client.detect_document_text(
            Document={
                'Bytes': byte_array
            }
        )

        return response
