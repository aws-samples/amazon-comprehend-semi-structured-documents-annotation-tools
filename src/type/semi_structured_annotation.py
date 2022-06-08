# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Contains annotation data types."""

from enum import Enum
from typing import List
from pydantic import BaseModel


class SemiStructuredDocumentType(Enum):
    """Enum class for document types."""

    ScannedPDF = "ScannedPDF"
    NativePDF = "NativePDF"


class SemiStructuredDocumentMetadataObject(BaseModel):
    """Class for DocumentMetadata object."""

    Pages: str = "1"
    PageNumber: str = "1"


class SemiStructuredAnnotation(BaseModel):
    """Class for annotation object."""

    Blocks: List[dict] = []
    BlocksS3Ref: str = ""
    DocumentMetadata: SemiStructuredDocumentMetadataObject = SemiStructuredDocumentMetadataObject()
    Version: str = ""
    DocumentType: str = SemiStructuredDocumentType.NativePDF.value
    Entities: list = []
    File: str = "unknown"
