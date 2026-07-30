# MSK Connect — Iceberg Sink Configuration

## Purpose

Amazon MSK Connect runs the Apache Iceberg Kafka Connect sink that consumes
records from Amazon MSK and commits them into the Bronze Iceberg table.

The executable source of truth is:

```text
infra/templates/messaging.yaml
```

This document is a readable reference only.

## Connector

| Setting | Value |
|---|---|
| Connector name | `yt-sales-iac-iceberg-sink` |
| Custom plugin | `yt-sales-iac-iceberg-sink-plugin` |
| Kafka Connect version | `3.7.x` |
| Capacity | Provisioned |
| MCU per worker | `2` |
| Workers | `1` |
| Total MCUs | `2` |
| Autoscaling | Disabled |
| Tasks | `2` |

## Networking

The connector runs in both CloudFormation-created private subnets and uses the
dedicated MSK Connect security group.

```text
PrivateSubnet1
PrivateSubnet2
MSKConnectSecurityGroup
        |
        | TCP 9098
        v
MSKSecurityGroup
        |
        v
Amazon MSK
```

No subnet IDs or security-group IDs are hardcoded in the repository.

The broker listener used by the connector is the IAM/TLS endpoint on port
`9098`.

## Authentication

```text
Authentication: IAM
Transport:      TLS
Unauthenticated: disabled
SASL/SCRAM:     not used
Client certs:   not used
```

The connector service execution role is:

```text
yt-sales-iac-iceberg-sink-connect-role
```

Network access and Kafka authorisation are separate controls:

```text
Security groups  -> can the connector reach TCP 9098?
MSK IAM policy   -> may the connector Connect / ReadData / use consumer groups?
```

## Source and target

Source topic:

```text
yt-sales-iac-records
```

Iceberg control topic:

```text
yt-sales-iac-control-iceberg
```

Connector DLQ:

```text
yt-sales-iac-errors
```

Target table:

```text
iata_sales_iac_bronze.sales_raw
```

Warehouse:

```text
s3://yt-sales-iac-lakehouse-<ACCOUNT_ID>/bronze
```

The Glue Data Catalog is the Iceberg catalog and S3FileIO is used for Iceberg
data/metadata access.

## Custom plugin

CloudFormation registers:

```text
AWS::KafkaConnect::CustomPlugin
```

from:

```text
s3://yt-sales-iac-plugins-<ACCOUNT_ID>/
msk-connect/iceberg/yt-sales-iac-iceberg-sink-plugin.zip
```

The connector then references the custom plugin ARN and its current revision.

The large plugin ZIP is intentionally kept outside normal Git source control.
See:

```text
dependencies/msk-connect/iceberg/plugin-manifest.md
```

## Connector properties

The standalone readable properties are maintained in:

```text
config/msk-connect/iceberg-sink.properties
```

The important settings are:

```properties
connector.class=org.apache.iceberg.connect.IcebergSinkConnector
tasks.max=2
topics=yt-sales-iac-records

iceberg.tables=iata_sales_iac_bronze.sales_raw
iceberg.tables.auto-create-enabled=false
iceberg.tables.evolve-schema-enabled=false

iceberg.catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
iceberg.catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
iceberg.catalog.client.region=ap-southeast-1
iceberg.catalog.warehouse=s3://yt-sales-iac-lakehouse-<ACCOUNT_ID>/bronze

iceberg.control.topic=yt-sales-iac-control-iceberg
iceberg.control.commit.interval-ms=10000

key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false

errors.tolerance=all
errors.deadletterqueue.topic.name=yt-sales-iac-errors
errors.deadletterqueue.context.headers.enable=true
errors.deadletterqueue.topic.replication.factor=2
```

Bronze table auto-creation and schema evolution are disabled because the
Iceberg table contract is bootstrapped explicitly from the version-controlled
SQL under:

```text
infra/sql/iceberg/
```

## Logging

CloudWatch log group:

```text
/aws/msk-connect/yt-sales-iac-iceberg-sink
```

S3 and Firehose connector log delivery are not used.

## Failure behaviour

Kafka Connect owns its consumer offsets. Records that the connector cannot
process under the configured tolerance policy can be routed to:

```text
yt-sales-iac-errors
```

Operational failures are visible in CloudWatch and connector/task metrics.

## CloudFormation ownership

The final IaC owns:

```text
AWS::KafkaConnect::CustomPlugin
        |
        v
AWS::KafkaConnect::Connector
        |
        +-- private subnets
        +-- dedicated SG
        +-- IAM service execution role
        +-- IAM/TLS MSK authentication
        +-- provisioned capacity
        +-- Iceberg configuration
        `-- CloudWatch logging
```

Runtime-dependent values such as subnet IDs, security-group IDs, MSK bootstrap
servers, plugin ARN and role ARN are injected by CloudFormation rather than
hardcoded.
