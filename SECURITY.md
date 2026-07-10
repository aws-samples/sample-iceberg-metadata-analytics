# Security Policy

## Reporting a Vulnerability

If you discover a potential security issue in this project, please notify
AWS/Amazon Security via the
[vulnerability reporting page](https://aws.amazon.com/security/vulnerability-reporting/)
or email aws-security@amazon.com. Please do **not** create a public GitHub
issue.

## Scope and intent

This repository is **sample code** that demonstrates analyzing the Apache
Iceberg metadata layer with AWS Glue and an optional Amazon Bedrock advisory
layer. It is intended as a learning reference and a starting point, not as a
turnkey production system. Review and adapt it to your own security, compliance,
and operational requirements before any production use.

## Security posture

The CloudFormation template and IAM policies follow least privilege:

- **IAM** — every permission is resource-scoped to the effective bucket and Glue
  database; `iam:PassRole` is self-scoped to Glue; confused-deputy protection via
  `aws:SourceAccount` / `aws:SourceArn`; cross-account exfiltration is blocked
  with `aws:ResourceAccount` conditions on S3 access. No AWS-managed broad
  policies are attached.
- **S3** — server-side encryption (SSE-S3), versioning, full public-access
  block, TLS-only bucket policies, and server access logging on the created
  warehouse bucket.
- **Bedrock** — `bedrock:InvokeModel` is scoped to the selected inference profile
  and the specific foundation-model ARNs (Amazon Nova + Anthropic Claude) in US
  regions only, never `bedrock:*`.
- **Lambda** — the deploy-time custom-resource function has a dead-letter queue
  (SQS, SSE-enabled), X-Ray active tracing, a reserved-concurrency cap, a scoped
  execution role, and resource tags.

## Production hardening recommendations

Before running anything like this in production, consider:

1. **Customer-managed KMS keys (SSE-KMS)** for the S3 buckets and a Glue
   [Security Configuration](https://docs.aws.amazon.com/glue/latest/dg/encryption-security-configuration.html)
   on the Glue jobs, instead of the default SSE-S3. This enables key rotation,
   fine-grained key policies, and encryption of Glue job bookmarks/CloudWatch
   logs.
2. **Run the custom-resource Lambda inside a VPC** with an S3 gateway endpoint (and
   interface endpoints for any other services) if your environment requires all
   compute to run in private subnets. Note this adds NAT/endpoint cost.
3. **Restrict network egress** and add S3 bucket policies that limit access to
   specific VPCs / VPC endpoints (`aws:SourceVpce`) where appropriate.
4. **Enable AWS CloudTrail data events** on the S3 buckets and route access logs
   to a centralized logging account.
5. **Enable IAM Access Analyzer** at the account/organization level to
   continuously validate the deployed policies.
6. **Add monitoring/alerting** (CloudWatch alarms) for unusual Glue job or
   Bedrock invocation patterns.

## Known security considerations (accepted debt for this sample)

These items are intentionally left as-is to keep the sample simple. They are
low/medium risk in the sample context and are called out here for transparency.

| Consideration | Severity | Rationale |
|---|---|---|
| Custom-resource Lambda is not in a VPC | Medium | It only runs during stack create/update/delete and talks to AWS service endpoints. Putting it in a VPC would require a NAT gateway plus interface endpoints — disproportionate cost for a sample. |
| No Glue Security Configuration on the 3 Glue jobs | Medium | Data is already encrypted at rest with SSE-S3. Adding a Glue Security Configuration requires provisioning and managing a KMS key (see hardening rec #1). |
| `xray:PutTraceSegments` / `PutTelemetryRecords` use `Resource: "*"` | Low | These X-Ray actions do not support resource-level permissions; `*` is the only valid resource. |
| Inline IAM policies (vs. standalone managed policies) | Low | Acceptable for sample code — the policies are tightly coupled to the resources' lifecycle and are removed with the stack. |

## Cleanup

To remove everything created by a deployment (stack, created data/warehouse
bucket, seed/artifact bucket, and the render Lambda log group):

```bash
cd solution/deploy
./teardown.sh --region <region> --prefix <prefix> --artifact-bucket <seed-bucket> --yes
```

In **NotebookOnly** mode your existing Iceberg bucket and Glue database are never
touched. In **Full** mode the created warehouse bucket is drained and deleted
with the stack. Shared account-wide Glue log groups under `/aws-glue/*` are not
deleted automatically; remove them manually if this account has no other Glue
usage.
