from unittest import TestCase
import json

from utils.output_manifest_utils import is_an_expired_task
from type.semi_structured_output_manifest import SemiStructuredOutputManifestObject


class CorrectLabelingJobWithExpiredTasksTest(TestCase):

    def test_is_an_expired_task(self):
        with open('test/unit/resources/output_with_expired_tasks_manifest.jsonl', 'r') as f:
            for i, line in enumerate(f.readlines()):
                task_object = SemiStructuredOutputManifestObject(json_obj=json.loads(line), job_name="mock_job_name")
                if i == 0:
                    self.assertFalse(is_an_expired_task(task_object, "mock_job_name"))
                else:
                    self.assertTrue(is_an_expired_task(task_object, "mock_job_name"))
