Step Functions starts Glue job
        |
        v
Glue receives MANIFEST_URI
        |
        v
Read manifest.json from S3
        |
        v
Get list of landed CSV files
        |
        v
Validate header of every CSV
        |
        +-----------------------------+
        |                             |
        v                             v
Valid files                    Invalid files
        |                             |
        v                             v
Read using Spark             Create file-level
as one DataFrame             error messages
        |                             |
        v                             v
Add source lineage           Publish to MSK
and ingestion metadata       error topic
        |
        v
Convert every row to JSON
        |
        v
Publish rows to MSK main topic
        |
        v
Send optional SNS summary
        |
        v
Commit Glue job