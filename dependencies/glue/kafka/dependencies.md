# Glue → MSK dependencies

These three tested JARs are intentionally kept in this case-study repository so the demo can be reproduced without downloading a different version at deployment time.

| Dependency | Version | IaC S3 target | Purpose |
|---|---:|---|---|
| `spark-sql-kafka-0-10_2.12-3.5.6.jar` | Spark 3.5.6 / Scala 2.12 | `s3://iata-sales-iac-artifacts-878670310452/glue/jars/` | Spark Kafka integration |
| `spark-token-provider-kafka-0-10_2.12-3.5.6.jar` | Spark 3.5.6 / Scala 2.12 | same | Kafka token provider dependency |
| `aws-msk-iam-auth-2.3.7-all.jar` | 2.3.7 | same | IAM authentication from Glue to MSK |

`infra/scripts/package.sh` uploads these files automatically after Phase 1 creates the artifacts bucket.
