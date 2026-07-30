from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import boto3


@dataclass
class AwsReadService:
    enabled: bool
    region: str
    profile: str | None

    def _session(self):
        if not self.enabled:
            raise PermissionError("AWS tools are disabled. Set AGENT_ENABLE_AWS=true with a read-only profile.")
        return boto3.Session(profile_name=self.profile, region_name=self.region)

    def get_glue_job_run(self, job_name: str, run_id: str) -> dict[str, Any]:
        return self._session().client("glue").get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=False)["JobRun"]

    def get_control_item(self, table_name: str, pk: str, sk: str) -> dict[str, Any]:
        table = self._session().resource("dynamodb").Table(table_name)
        return table.get_item(Key={"pk": pk, "sk": sk}, ConsistentRead=False).get("Item", {})

    def read_s3_text(self, bucket: str, key: str, max_bytes: int = 200_000) -> str:
        if max_bytes < 1 or max_bytes > 1_000_000:
            raise ValueError("max_bytes must be between 1 and 1,000,000.")
        body = self._session().client("s3").get_object(Bucket=bucket, Key=key)["Body"].read(max_bytes)
        return body.decode("utf-8", errors="replace")
