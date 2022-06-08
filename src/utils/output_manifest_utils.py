# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

"""Output manifest utils helper class."""

from typing import Optional

from type.semi_structured_output_manifest import SemiStructuredOutputManifestObject


def is_an_expired_task(output_manifest_obj: SemiStructuredOutputManifestObject, job_name: Optional[str]):
    """Return whether a task had timed out."""
    job_metadata_obj = getattr(output_manifest_obj, f"{job_name}-metadata", None)
    return job_metadata_obj and "ClientError: Annotation tasks expired" in getattr(job_metadata_obj, 'failure-reason', '')
