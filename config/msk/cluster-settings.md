# Amazon MSK Cluster Configuration

## Source of truth

Executable configuration:

```text
infra/templates/messaging.yaml
```

This document is a readable reference.

## Cluster

| Setting | Value |
|---|---|
| Cluster name | `iata-sales-iac-msk` |
| Cluster type | Provisioned |
| Apache Kafka version | `3.6.0` |
| Metadata mode | ZooKeeper |
| Broker instance type | `kafka.t3.small` |
| Broker count | `4` |
| Availability Zones | `2` |
| Brokers per AZ | `2` |
| EBS storage / broker | `10 GiB` |
| Storage autoscaling | Disabled |
| Enhanced monitoring | `DEFAULT` |
| Prometheus exporters | Disabled |

The four-broker value is passed by the parent stack and matches the final IaC
deployment configuration.

## Networking

The cluster is private and deployed across the two CloudFormation-created
private subnets:

```text
PrivateSubnet1 (ap-southeast-1a)
PrivateSubnet2 (ap-southeast-1b)
```

It uses:

```text
iata-sales-iac-msk-sg
```

Public access is not enabled.

Clients reach the IAM/TLS broker listener on:

```text
TCP 9098
```

The final templates reference subnet/security-group resources dynamically;
there are no manual DEV subnet or SG IDs in the configuration.

## Authentication

```text
IAM authentication:       enabled
Unauthenticated access:   disabled
SASL/SCRAM:               not used
TLS client certificates:  not used
```

## Encryption

In transit:

```text
Client -> broker: TLS
Broker -> broker: TLS
Plaintext:          disabled
```

At rest, no customer-managed KMS key is explicitly supplied, so the cluster
uses the AWS-managed MSK encryption configuration.

## Broker logging

CloudWatch broker logging is enabled:

```text
/aws/msk/iata-sales-iac-broker
```

S3 and Firehose broker log delivery are disabled.

## Bootstrap servers

Bootstrap endpoints are runtime-generated after MSK becomes active.

`deploy.sh phase2` retrieves:

```text
BootstrapBrokerStringSaslIam
```

and passes those IAM/TLS `:9098` endpoints to dependent resources.

They are not committed or hardcoded in the repository.

## Topics

| Topic | Partitions | Replication factor | Purpose |
|---|---:|---:|---|
| `iata-sales-iac-records` | 6 | 2 | Main sales record stream |
| `iata-sales-iac-errors` | 3 | 2 | Connector/DLQ errors |
| `iata-sales-iac-control-iceberg` | 1 | 2 | Iceberg connector control |

Topics are managed by CloudFormation as `AWS::MSK::Topic` resources.

## Key clients

```text
Landing-to-MSK Glue
  GlueSecurityGroup
       |
       | TCP 9098 + IAM WriteData
       v
     MSK

MSK Connect Iceberg Sink
  MSKConnectSecurityGroup
       |
       | TCP 9098 + IAM ReadData/group permissions
       v
     MSK
```

Security groups provide network reachability. MSK IAM provides Kafka
authentication/authorisation.
