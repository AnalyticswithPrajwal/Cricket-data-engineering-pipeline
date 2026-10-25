# AWS and Snowflake Setup

## Prerequisites

- An AWS account
- An S3 bucket containing cricket JSON files
- A Snowflake account
- An AWS IAM role configured for Snowflake access
- Permissions to configure S3 event notifications and the required SQS destination

## 1. Upload Source Files

Upload cricket JSON files to the configured S3 prefix, for example:

`s3://<S3_BUCKET_NAME>/cricket/`

Replace the placeholder with your own bucket and prefix.

## 2. Configure the Snowflake Storage Integration

Run the storage integration SQL in `sql/02_raw_layer.sql`.

Retrieve the integration details using `DESC INTEGRATION S3_INT` and follow Snowflake's documented instructions to configure the AWS IAM role and trust relationship.

## 3. Create the External Stage

Create `RAW.S3_EXT_STAGE` to reference the S3 location and interpret source files as JSON.

Use `LIST @RAW.S3_EXT_STAGE` to verify that the expected files are visible.

## 4. Configure Snowpipe

Create `RAW.S3_PIPE` with `AUTO_INGEST = TRUE`.

Configure the S3 event notification to send file notifications to the Snowflake-provided SQS notification channel. Ensure the necessary AWS permissions are in place.

## 5. Create the Stream and Tasks

Create `RAW.STREAM_RAW_DATA` on `RAW.RAW_DATA`.

Create the staging and warehouse tasks using the SQL scripts in the `sql/` directory. Resume the task graph after verifying the dependencies and configuration.

## 6. Verify the Pipeline

Run `sql/07_validation_queries.sql` to inspect ingestion, staging, warehouse data, and task history.

## Security

- Never commit AWS access keys, secret keys, passwords, or tokens.
- Replace account-specific identifiers and bucket names with placeholders in public examples.
- Apply least-privilege IAM and Snowflake permissions.
- Do not publish generated notification ARNs unnecessarily.

## Documentation

Refer to the official Snowflake documentation for storage integrations, Snowpipe auto-ingest, and task graphs when configuring your own environment.
