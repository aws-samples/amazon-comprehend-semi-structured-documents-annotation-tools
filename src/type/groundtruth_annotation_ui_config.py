# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Contains schema data types."""

from typing import List
from pydantic import BaseModel


class BaseAnnotationTaskConfig(BaseModel):
    """Base class for annotation task configuration."""

    annotation_task: str
    tags: List[str] = []
    properties: list = []


class NamedEntitiesAnnotationTaskConfig(BaseAnnotationTaskConfig):
    """Class for named-entity annotation task configuration."""

    annotation_task: str = "NER"


class AnnotationUITaskSchemas(BaseModel):
    """Class for named-entity annotation UI schemas object."""

    named_entity: NamedEntitiesAnnotationTaskConfig = NamedEntitiesAnnotationTaskConfig()


class GroundTruthAnnotationUIConfig(BaseModel):
    """Class for GroundTruth annotation UI configuration JSON."""

    version: str = ""
    schemas: AnnotationUITaskSchemas = AnnotationUITaskSchemas()
    exported_time: str = ""
    uuid: str = ""
