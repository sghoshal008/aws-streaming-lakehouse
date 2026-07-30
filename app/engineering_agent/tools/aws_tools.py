from __future__ import annotations
from app.engineering_agent.runtime import services


def get_glue_job_run(job_name: str, run_id: str): return services().aws.get_glue_job_run(job_name, run_id)
def get_control_item(table_name: str, pk: str, sk: str): return services().aws.get_control_item(table_name, pk, sk)
def read_s3_text(bucket: str, key: str, max_bytes: int = 200000): return services().aws.read_s3_text(bucket, key, max_bytes)
