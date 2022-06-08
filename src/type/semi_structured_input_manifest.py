# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Contains input manifest JSON data types."""

from typing import Optional


class SemiStructuredInputManifestObject:
    """Class for input manifest JSON object."""

    def __init__(self, source_ref: str = "", page: str = "1", metadata: dict = {}, annotator_metadata: Optional[dict] = None,
                 primary_annotation_ref: Optional[str] = None, secondary_annotation_ref: Optional[str] = None):
        """Init."""
        setattr(self, "source-ref", source_ref)
        self.page = page
        self.metadata = metadata
        setattr(self, "annotator-metadata", annotator_metadata)
        setattr(self, "primary-annotation-ref", primary_annotation_ref)
        setattr(self, "secondary-annotation-ref", secondary_annotation_ref)
