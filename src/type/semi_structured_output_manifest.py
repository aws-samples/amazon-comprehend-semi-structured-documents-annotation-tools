# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Contains output manifest JSON data types."""

import json
from typing import Optional


class ObjectToDict:
    """Class for inheriting to easily convert object to dictionary."""

    def to_dict(self):
        """Convert object to dictionary."""
        return json.loads(json.dumps(self, default=lambda o: o.__dict__))


class SemiStructuredOutputManifestMetadataObject(ObjectToDict):
    """Class for output manifest JSON object."""

    def __init__(self, metadata: dict = {}):
        """Init."""
        self.pages = metadata.get("pages", "1")
        setattr(self, "use-textract-only", metadata.get("use-textract-only", False))
        self.labels = metadata.get("labels", [])


class SemiStructuredOutputManifestJobMetadataObject:
    """Class for job metadata in a Semi-structured output manifest object."""

    def __init__(self, metadata: dict = {}):
        for key in metadata:
            setattr(self, key, metadata.get(key))


class SemiStructuredOutputManifestJobObject:
    """Class for job object in a Semi-structured output manifest object."""

    def __init__(self, job_object: dict = {}):
        setattr(self, "annotation-ref", job_object.get("annotation-ref"))


class SemiStructuredOutputManifestObject(ObjectToDict):
    """Class for output manifest JSON object created from a JSON object."""

    def __init__(self, json_obj: dict = {}, job_name: Optional[str] = ""):
        """Init."""
        setattr(self, "source-ref", json_obj.get("source-ref"))
        self.page = json_obj.get("page")
        self.metadata = SemiStructuredOutputManifestMetadataObject(metadata=json_obj.get("metadata", {}))

        if job_name:
            job_metadata_key = f"{job_name}-metadata"
            setattr(self, job_metadata_key, SemiStructuredOutputManifestJobMetadataObject(metadata=json_obj.get(job_metadata_key, {})))

            setattr(self, job_name, SemiStructuredOutputManifestJobObject(job_object=json_obj.get(job_name, {})))

        setattr(self, "annotator-metadata", json_obj.get("annotator-metadata"))
        setattr(self, "primary-annotation-ref", json_obj.get("primary-annotation-ref"))
        setattr(self, "secondary-annotation-ref", json_obj.get("secondary-annotation-ref"))
