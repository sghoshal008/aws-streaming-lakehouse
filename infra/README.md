FULL DEPLOYMENT FLOW
│
├── STEP 1 — PACKAGE #1
│
│   Command:
│     ./infra/scripts/package.sh
│
│   Script:
│     infra/scripts/package.sh
│
│   Purpose:
│     Validate the repository and upload the nested CloudFormation templates
│     required for the first infrastructure deployment.
│
│   ├── Validate CloudFormation templates
│   │     ├── infra/templates/main.yaml
│   │     ├── infra/templates/network.yaml
│   │     ├── infra/templates/storage.yaml
│   │     ├── infra/templates/iam.yaml
│   │     ├── infra/templates/messaging.yaml
│   │     ├── infra/templates/compute.yaml
│   │     ├── infra/templates/orchestration.yaml
│   │     └── infra/templates/monitoring.yaml
│   │
│   ├── Validate runtime source files
│   │     ├── app/acquisition/iata_sales_acquisition.py
│   │     ├── app/glue/landing-to-msk/iata_sales_landing_to_msk.py
│   │     └── app/glue/bronze-to-silver/iata_sales_bronze_to_silver.py
│   │
│   ├── Validate Glue Kafka dependencies
│   │     └── dependencies/glue/kafka/
│   │           ├── spark-sql-kafka-0-10_2.12-3.5.6.jar
│   │           ├── spark-token-provider-kafka-0-10_2.12-3.5.6.jar
│   │           └── aws-msk-iam-auth-2.3.7-all.jar
│   │
│   ├── Validate MSK Connect plugin ZIP
│   │     └── dependencies/msk-connect/iceberg/
│   │           └── iceberg-kafka-connect-runtime-1.11.0-SNAPSHOT.zip
│   │
│   └── Upload nested CloudFormation templates
│         └── S3 bootstrap bucket:
│             s3://iata-sales-iac-bootstrap-<ACCOUNT_ID>/
│                 iata-sales-iac/cloudformation/
│
│   NOTE:
│     On a fresh deployment, Package #1 stops after uploading the
│     CloudFormation templates because the runtime artifact buckets
│     do not exist yet.
│
│
├── STEP 2 — DEPLOY PHASE 1
│
│   Command:
│     ./infra/scripts/deploy.sh phase1
│
│   Script:
│     infra/scripts/deploy.sh
│
│   Parent template:
│     infra/templates/main.yaml
│
│   Parameters:
│     DeployApplication=false
│     DeployIcebergConnector=false
│     MSKBootstrapServers=""
│
│   Purpose:
│     Create the core AWS platform and infrastructure foundation.
│
│   ├── VPC / Networking
│   │     Template:
│   │       infra/templates/network.yaml
│   │
│   │     ├── VPC
│   │     ├── Internet Gateway
│   │     ├── 2 Public Subnets
│   │     ├── 2 Private Subnets
│   │     ├── NAT Gateway
│   │     ├── Elastic IP for NAT
│   │     ├── Public Route Table
│   │     ├── Private Route Table 1
│   │     ├── Private Route Table 2
│   │     ├── Public default route
│   │     ├── Private default routes through NAT
│   │     ├── Subnet / route-table associations
│   │     ├── S3 Gateway VPC Endpoint
│   │     ├── Lambda Security Group
│   │     ├── Glue Security Group
│   │     ├── MSK Security Group
│   │     ├── MSK Connect Security Group
│   │     ├── Glue → MSK port 9098 rule
│   │     └── MSK Connect → MSK port 9098 rule
│   │
│   ├── Storage / Catalog / Control State
│   │     Template:
│   │       infra/templates/storage.yaml
│   │
│   │     ├── Landing S3 bucket
│   │     │     iata-sales-iac-landing-<ACCOUNT_ID>
│   │     │
│   │     ├── Lakehouse S3 bucket
│   │     │     iata-sales-iac-lakehouse-<ACCOUNT_ID>
│   │     │
│   │     ├── Artifacts S3 bucket
│   │     │     iata-sales-iac-artifacts-<ACCOUNT_ID>
│   │     │
│   │     ├── Plugins S3 bucket
│   │     │     iata-sales-iac-plugins-<ACCOUNT_ID>
│   │     │
│   │     ├── DynamoDB ingestion-control table
│   │     │
│   │     ├── Glue Bronze database
│   │     │     iata_sales_iac_bronze
│   │     │
│   │     ├── Glue Silver database
│   │     │     iata_sales_iac_silver
│   │     │
│   │     └── Glue Quarantine database
│   │
│   ├── IAM
│   │     Template:
│   │       infra/templates/iam.yaml
│   │
│   │     ├── Acquisition Lambda execution role
│   │     ├── Landing → MSK Glue execution role
│   │     ├── Bronze → Silver Glue execution role
│   │     ├── Ingestion Step Functions role
│   │     ├── Bronze → Silver Step Functions role
│   │     └── Iceberg MSK Connect execution role
│   │
│   └── Messaging / Streaming Foundation
│         Template:
│           infra/templates/messaging.yaml
│
│         ├── SNS pipeline alert topic
│         ├── Optional email subscription
│         ├── MSK broker CloudWatch log group
│         ├── Amazon MSK cluster
│         │     iata-sales-iac-msk
│         │
│         ├── Kafka topic
│         │     iata-sales-iac-records
│         │
│         ├── Kafka error topic
│         │     iata-sales-iac-errors
│         │
│         ├── Iceberg control topic
│         │     iata-sales-iac-control-iceberg
│         │
│         ├── Glue Schema Registry
│         ├── Glue Schema
│         └── MSK Connect log group
│
│   NOT CREATED IN PHASE 1:
│
│     ├── Acquisition Lambda
│     ├── Glue jobs
│     ├── Step Functions
│     ├── CloudWatch monitoring dashboard / alarms
│     ├── MSK Connect Custom Plugin
│     └── MSK Connect Iceberg Connector
│
│
├── STEP 3 — PACKAGE #2
│
│   Command:
│     ./infra/scripts/package.sh
│
│   Script:
│     infra/scripts/package.sh
│
│   Purpose:
│     Package and upload all runtime artifacts now that the Phase-1
│     S3 buckets exist.
│
│   ├── Re-upload latest nested CloudFormation templates
│   │     └── infra/templates/*.yaml
│   │
│   ├── Package Acquisition Lambda
│   │     Source:
│   │       app/acquisition/iata_sales_acquisition.py
│   │
│   │     Output:
│   │       build/iata_sales_acquisition.zip
│   │
│   │     Upload:
│   │       s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/
│   │           lambda/iata_sales_acquisition.zip
│   │
│   ├── Upload Landing → MSK Glue script
│   │     Local file:
│   │       app/glue/landing-to-msk/
│   │           iata_sales_landing_to_msk.py
│   │
│   │     Upload:
│   │       s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/
│   │           glue/scripts/iata_sales_landing_to_msk.py
│   │
│   ├── Upload Bronze → Silver Glue script
│   │     Local file:
│   │       app/glue/bronze-to-silver/
│   │           iata_sales_bronze_to_silver.py
│   │
│   │     Upload:
│   │       s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/
│   │           glue/scripts/iata_sales_bronze_to_silver.py
│   │
│   ├── Upload Glue Kafka JARs
│   │     Local:
│   │       dependencies/glue/kafka/
│   │
│   │     ├── spark-sql-kafka-0-10_2.12-3.5.6.jar
│   │     ├── spark-token-provider-kafka-0-10_2.12-3.5.6.jar
│   │     └── aws-msk-iam-auth-2.3.7-all.jar
│   │
│   │     Upload:
│   │       s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/glue/jars/
│   │
│   ├── Upload Iceberg SQL / DDL
│   │     Local:
│   │       infra/sql/iceberg/
│   │
│   │     ├── 001-create-bronze.sql
│   │     ├── 002-set-bronze-data-path.sql
│   │     ├── 003-create-silver.sql
│   │     └── 004-set-silver-data-path.sql
│   │
│   │     Upload:
│   │       s3://iata-sales-iac-artifacts-<ACCOUNT_ID>/sql/iceberg/
│   │
│   └── Upload MSK Connect Iceberg Plugin ZIP
│         Local:
│           dependencies/msk-connect/iceberg/
│               iceberg-kafka-connect-runtime-1.11.0-SNAPSHOT.zip
│
│         Upload:
│           s3://iata-sales-iac-plugins-<ACCOUNT_ID>/
│               msk-connect/iceberg/
│               iata-sales-iac-iceberg-sink-plugin.zip
│
│
├── STEP 4 — DEPLOY PHASE 2
│
│   Command:
│     ./infra/scripts/deploy.sh phase2
│
│   Script:
│     infra/scripts/deploy.sh
│
│   Parent template:
│     infra/templates/main.yaml
│
│   Parameters:
│     DeployApplication=true
│     DeployIcebergConnector=true
│     MSKBootstrapServers=<discovered IAM bootstrap servers>
│
│   Purpose:
│     Bootstrap the Iceberg tables and deploy the complete
│     runtime/application layer.
│
│   ├── Verify runtime S3 artifacts
│   │     Script:
│   │       infra/scripts/deploy.sh
│   │
│   │     ├── Lambda ZIP
│   │     ├── Glue scripts
│   │     ├── Kafka JARs
│   │     ├── SQL files
│   │     └── MSK Connect plugin ZIP
│   │
│   ├── Discover MSK IAM bootstrap servers
│   │     Script:
│   │       infra/scripts/deploy.sh
│   │
│   │     Result:
│   │       b-*.amazonaws.com:9098
│   │
│   ├── Bootstrap Bronze Iceberg table
│   │     Files:
│   │       infra/sql/iceberg/001-create-bronze.sql
│   │       infra/sql/iceberg/002-set-bronze-data-path.sql
│   │
│   │     Result:
│   │       iata_sales_iac_bronze.sales_raw
│   │
│   ├── Bootstrap Silver Iceberg table
│   │     Files:
│   │       infra/sql/iceberg/003-create-silver.sql
│   │       infra/sql/iceberg/004-set-silver-data-path.sql
│   │
│   │     Result:
│   │       iata_sales_iac_silver.sales
│   │
│   ├── Deploy / enable MSK Connect
│   │     Template:
│   │       infra/templates/messaging.yaml
│   │
│   │     ├── AWS::KafkaConnect::CustomPlugin
│   │     │     iata-sales-iac-iceberg-sink-plugin
│   │     │
│   │     └── AWS::KafkaConnect::Connector
│   │           iata-sales-iac-iceberg-sink
│   │
│   │           Flow:
│   │             MSK records topic
│   │               ↓
│   │             Iceberg Sink Connector
│   │               ↓
│   │             Bronze Iceberg table
│   │
│   ├── Deploy Compute Layer
│   │     Template:
│   │       infra/templates/compute.yaml
│   │
│   │     ├── Acquisition Lambda log group
│   │     ├── Acquisition Lambda
│   │     │     Code:
│   │     │       app/acquisition/iata_sales_acquisition.py
│   │     │
│   │     ├── Glue MSK network connection
│   │     │     iata-sales-iac-msk-connection
│   │     │
│   │     ├── Landing → MSK Glue log groups
│   │     ├── Landing → MSK Glue job
│   │     │     Script:
│   │     │       app/glue/landing-to-msk/
│   │     │           iata_sales_landing_to_msk.py
│   │     │
│   │     │     Purpose:
│   │     │       Read landing data
│   │     │         ↓
│   │     │       transform rows to Kafka messages
│   │     │         ↓
│   │     │       publish to iata-sales-iac-records
│   │     │
│   │     ├── Bronze → Silver Glue log groups
│   │     └── Bronze → Silver Glue job
│   │           Script:
│   │             app/glue/bronze-to-silver/
│   │                 iata_sales_bronze_to_silver.py
│   │
│   │           Purpose:
│   │             Bronze Iceberg
│   │               ↓
│   │             validation / transformation / dedup
│   │               ↓
│   │             Silver Iceberg
│   │
│   ├── Deploy Orchestration
│   │     Template:
│   │       infra/templates/orchestration.yaml
│   │
│   │     ├── Ingestion Step Functions log group
│   │     ├── Bronze → Silver Step Functions log group
│   │     │
│   │     ├── Ingestion State Machine
│   │     │
│   │     │     Run Acquisition
│   │     │         ↓
│   │     │     Run Landing → MSK Glue
│   │     │         ↓
│   │     │     Mark Run Completed
│   │     │         ↓
│   │     │     Success
│   │     │
│   │     │     Failure path:
│   │     │       Catch
│   │     │         ↓
│   │     │       Mark Run Failed
│   │     │         ↓
│   │     │       SNS notification
│   │     │         ↓
│   │     │       Fail
│   │     │
│   │     └── Bronze → Silver State Machine
│   │           ↓
│   │         Run Bronze → Silver Glue
│   │           ↓
│   │         Success / SNS failure handling
│   │
│   └── Deploy Monitoring
│         Template:
│           infra/templates/monitoring.yaml
│
│         ├── CloudWatch pipeline dashboard
│         ├── Acquisition Lambda error alarm
│         ├── Ingestion Step Functions failure alarm
│         ├── Bronze → Silver Step Functions failure alarm
│         ├── MSK Connect errored-task alarm
│         └── MSK active-controller alarm
│
│
└── STEP 5 — RUN / TEST THE PIPELINE

    Runtime flow:

    External Sales ZIP
      │
      ▼
    Acquisition Lambda
      │
      ├── inspect source / fingerprint
      ├── DynamoDB ingestion-control check
      ├── archive source
      └── extract files to Landing S3
      │
      ▼
    Landing S3
      │
      ▼
    Landing → MSK Glue Job
      │
      ├── reads source rows
      ├── creates Kafka records
      └── publishes to:
            iata-sales-iac-records
      │
      ▼
    Amazon MSK
      │
      ▼
    MSK Connect Iceberg Sink
      │
      ▼
    Bronze Iceberg
      │
      └── iata_sales_iac_bronze.sales_raw
      │
      ▼
    Bronze → Silver Glue Job
      │
      ├── validation
      ├── transformation
      └── deduplication
      │
      ▼
    Silver Iceberg
      │
      └── iata_sales_iac_silver.sales
      │
      ▼
    Glue Data Catalog
      │
      ▼
    Athena / downstream consumption





Deployment Order

1. ./infra/scripts/package.sh
      → Validate + upload CloudFormation templates

2. ./infra/scripts/deploy.sh phase1
      → Network + S3 + DynamoDB + Glue DBs + IAM + SNS + MSK + topics/schema

3. ./infra/scripts/package.sh
      → Upload Lambda + Glue scripts + JARs + SQL + Iceberg plugin ZIP

4. ./infra/scripts/deploy.sh phase2
      → Iceberg tables + Lambda + Glue + MSK Connect + Step Functions + Monitoring

5. Execute pipeline
      → Source → Landing → Glue → MSK → Iceberg Bronze → Glue → Silver → Athena
      