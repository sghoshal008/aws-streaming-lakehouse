# Apache Iceberg Kafka Connect Plugin Manifest

## Purpose

This plugin is used by Amazon MSK Connect to consume records from the IATA Sales Kafka topic and write them into the Bronze Apache Iceberg table stored in S3 and registered in the AWS Glue Data Catalog.

Pipeline:

```text
Landing S3
    ↓
Glue Landing → MSK
    ↓
Amazon MSK
    ↓
MSK Connect
    ↓
Apache Iceberg Sink Connector
    ↓
Bronze Iceberg Table
    ↓
S3 + Glue Data Catalog
```

---

## Custom Plugin

- Plugin type: `Apache Iceberg Kafka Connect`
- Connector class: `org.apache.iceberg.connect.IcebergSinkConnector`
- MSK Connect custom plugin name: `iata-sales-iac-iceberg-sink-plugin`
- Deployment type: `ZIP`
- Artifact bucket: `iata-sales-iac-plugins-878670310452`

Target S3 location:

```text
s3://iata-sales-iac-plugins-878670310452/msk-connect/iceberg/
```

Target plugin ZIP:

```text
s3://iata-sales-iac-plugins-878670310452/msk-connect/iceberg/iata-sales-iac-iceberg-sink-plugin.zip
```

The plugin ZIP is uploaded to S3 and registered as an Amazon MSK Connect Custom Plugin.

---

## Plugin Contents

The plugin package contains the Iceberg Kafka Connect runtime and its required dependencies.

Repository reference:

```text
dependencies/
└── msk-connect/
    └── iceberg/
        ├── plugin-manifest.md
        └── jars/
            └── <plugin runtime JARs>
```

The actual JAR binaries should not be committed to Git.

The deployment ZIP should contain the complete tested Iceberg Kafka Connect runtime dependency set.

---

## Important Dependency Note

The following JARs are used by the Glue Landing → MSK Spark producer:

```text
spark-sql-kafka-0-10_2.12-3.5.6.jar
spark-token-provider-kafka-0-10_2.12-3.5.6.jar
aws-msk-iam-auth-2.3.7-all.jar
```

These are Glue/Spark Kafka dependencies and should be stored under:

```text
s3://iata-sales-iac-artifacts-878670310452/glue/jars/
```

They are separate from the complete Apache Iceberg Kafka Connect plugin package used by MSK Connect.

---

## MSK Connect Connector

Connector name:

```text
iata-sales-iac-iceberg-sink
```

Custom plugin:

```text
iata-sales-iac-iceberg-sink-plugin
```

IAM execution role:

```text
iata-sales-iac-msk-connect-role
```

Kafka cluster:

```text
iata-sales-iac-msk
```

Tasks:

```text
tasks.max=1
```

---

## Kafka Source Topic

Main topic:

```text
iata-sales-iac-records
```

The connector consumes records produced by the Landing → MSK Glue job.

---

## Iceberg Sink Configuration

Connector class:

```properties
connector.class=org.apache.iceberg.connect.IcebergSinkConnector
```

Maximum tasks:

```properties
tasks.max=1
```

Source topic:

```properties
topics=iata-sales-iac-records
```

---

## Iceberg Catalog

Catalog implementation:

```properties
iceberg.catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
```

S3 FileIO implementation:

```properties
iceberg.catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

Warehouse:

```properties
iceberg.catalog.warehouse=s3://iata-sales-iac-lakehouse-878670310452/bronze
```

---

## Target Iceberg Table

Glue database:

```text
iata_sales_iac_bronze
```

Table:

```text
sales_raw
```

Fully qualified Iceberg table:

```text
iata_sales_iac_bronze.sales_raw
```

Connector configuration:

```properties
iceberg.tables=iata_sales_iac_bronze.sales_raw
```

---

## Iceberg Table Management

Table auto-creation (disabled because bootstrap SQL owns the schema):

```properties
iceberg.tables.auto-create-enabled=false
```

Automatic schema evolution (disabled to enforce the version-controlled contract):

```properties
iceberg.tables.evolve-schema-enabled=false
```

The Bronze table is created before the connector starts by infra/scripts/bootstrap-tables.sh using the DDLs under infra/sql/iceberg/.

---

## Iceberg Control Topic

Control topic:

```text
iata-sales-iac-control-iceberg
```

Configuration:

```properties
iceberg.control.topic=iata-sales-iac-control-iceberg
```

Commit interval:

```properties
iceberg.control.commit.interval-ms=10000
```

This configures an Iceberg commit approximately every:

```text
10 seconds
```

---

## Complete Core Connector Configuration

The target IaC connector configuration is:

```properties
connector.class=org.apache.iceberg.connect.IcebergSinkConnector

tasks.max=1

topics=iata-sales-iac-records

iceberg.catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
iceberg.catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO

iceberg.catalog.warehouse=s3://iata-sales-iac-lakehouse-878670310452/bronze

iceberg.tables=iata_sales_iac_bronze.sales_raw

iceberg.tables.auto-create-enabled=false
iceberg.tables.evolve-schema-enabled=false

iceberg.control.topic=iata-sales-iac-control-iceberg
iceberg.control.commit.interval-ms=10000
```

The exact working connector configuration should also be maintained in:

```text
config/msk-connect/iceberg-sink.properties
```

`plugin-manifest.md` documents the plugin/deployment.

`iceberg-sink.properties` contains the actual connector configuration.

---

## IAM Requirements

MSK Connect execution role:

```text
iata-sales-iac-msk-connect-role
```

The role requires access to:

```text
Amazon MSK
    ↓
Consume iata-sales-iac-records

AWS Glue Data Catalog
    ↓
Read/Create/Update Iceberg metadata

Amazon S3
    ↓
iata-sales-iac-lakehouse-878670310452

CloudWatch Logs
    ↓
Connector operational logs
```

The corresponding IAM definitions are maintained under:

```text
infra/policies/msk-connect/
├── trust-policy.json
└── permissions-policy.json
```

---

## Networking

The MSK Connect worker must be able to communicate with the IaC MSK cluster.

Target resources:

```text
MSK Cluster:
iata-sales-iac-msk

MSK Connect Connector:
iata-sales-iac-iceberg-sink

Security Group:
iata-sales-iac-msk-connect-sg
```

The connector should use the VPC/subnets associated with the MSK deployment.

---

## CloudWatch Logging

MSK Connect worker logs should be enabled.

Recommended log group:

```text
/aws/msk-connect/iata-sales-iac-iceberg-sink
```

These logs are used for:

- Connector startup failures
- Authentication failures
- Kafka consumption failures
- Iceberg commit failures
- Glue Catalog errors
- S3 write errors
- Task failures

---

## Repository vs S3

### Git Repository

Store documentation and configuration:

```text
dependencies/
└── msk-connect/
    └── iceberg/
        └── plugin-manifest.md

config/
└── msk-connect/
    ├── iceberg-sink.properties
    └── connector-settings.md
```

Do not commit the large generated plugin ZIP.

### S3

Store the deployable plugin ZIP:

```text
s3://iata-sales-iac-plugins-878670310452/
└── msk-connect/
    └── iceberg/
        └── iata-sales-iac-iceberg-sink-plugin.zip
```

---

## IaC Resources

The final CloudFormation implementation should create/manage:

| Resource | Target Name |
|---|---|
| Plugin S3 Bucket | `iata-sales-iac-plugins-878670310452` |
| Custom Plugin | `iata-sales-iac-iceberg-sink-plugin` |
| MSK Connect Connector | `iata-sales-iac-iceberg-sink` |
| MSK Connect IAM Role | `iata-sales-iac-msk-connect-role` |
| MSK Cluster | `iata-sales-iac-msk` |
| Kafka Source Topic | `iata-sales-iac-records` |
| Iceberg Control Topic | `iata-sales-iac-control-iceberg` |
| Glue Database | `iata_sales_iac_bronze` |
| Iceberg Table | `sales_raw` |
| Lakehouse Bucket | `iata-sales-iac-lakehouse-878670310452` |

---

## Deployment Flow

```text
Build / obtain Iceberg Kafka Connect runtime
                 ↓
Package complete plugin as ZIP
                 ↓
Upload ZIP
                 ↓
s3://iata-sales-iac-plugins-878670310452/
        msk-connect/iceberg/
                 ↓
Create MSK Connect Custom Plugin
                 ↓
iata-sales-iac-iceberg-sink-plugin
                 ↓
Create MSK Connect Connector
                 ↓
iata-sales-iac-iceberg-sink
                 ↓
Consume
iata-sales-iac-records
                 ↓
Write
iata_sales_iac_bronze.sales_raw
                 ↓
S3 Iceberg Bronze
```