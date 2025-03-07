# Athena Parallel Query Processing Workflow

This project implements an AWS Step Functions workflow for running multiple Athena queries in parallel, processing their results, and optionally transferring the output files to external systems via SFTP or Sharefile.

## Overview

The workflow automates the following process:
1. Loads SQL queries from S3 by file name
2. Executes multiple Athena queries in parallel
3. Formats the results as proper CSV files
4. For multiple queries: zips the results together
5. For single queries: uses the CSV directly
6. Optionally transfers the result file to an external system (SFTP or Sharefile)

## Architecture

The workflow consists of the following components:

### Lambda Functions

1. **query-loader**: Loads SQL queries from S3 based on query names
2. **ExecuteSingleQuery**: Executes an Athena query and formats the result as CSV
3. **ResultsGatherer**: Monitors query execution and collects results
4. **zip-s3-folder**: Combines multiple query results into a zip file
5. **file-transporter-lambda**: Transfers files to external systems (SFTP or Sharefile)
6. **extract-s3-key**: Helper function to parse S3 URIs

### Step Functions State Machine

The workflow is orchestrated by a Step Functions state machine that coordinates the execution of these Lambda functions.

## Setup

### Prerequisites

- AWS Account with permissions to create:
  - Lambda functions
  - Step Functions
  - IAM roles
  - S3 buckets
- S3 bucket for storing:
  - SQL query files
  - Query results
  - Zipped output files

### Deployment

1. Deploy all Lambda functions with appropriate IAM permissions
2. Deploy the Step Functions state machine
3. Upload your SQL query files to S3

## Usage

### Organizing SQL Queries

Place your SQL files in a consistent directory structure:

```
queries/
  ├── test_report.sql
  ├── users_report.sql
  ├── revenue_daily.sql
  └── revenue_monthly.sql
```

### Starting the Workflow

Start the workflow by passing a JSON input with the following parameters:

```json
{
  "query_names": ["test_report", "users_report", "revenue_monthly"],
  "query_index_bucket": "my-query-bucket",
  "query_prefix": "queries/",
  "output_bucket": "my-output-bucket",
  "output_prefix": "athena-results/",
  "zip_prefix": "zipped-results/",
  "database": "my_database",
  "transporter": {
    "type": "sftp",
    "config": {
      "sftp_host": "sftp.example.com",
      "sftp_port": "22",
      "sftp_user": "username",
      "sftp_password": "password",
      "sftp_path": "/uploads"
    }
  }
}
```

Parameters:
- `query_names`: Array of query names to execute
- `query_index_bucket`: S3 bucket containing query files and index
- `query_prefix`: (Optional) Prefix for SQL files
- `output_bucket`: S3 bucket for storing results
- `output_prefix`: Prefix for Athena query results
- `zip_prefix`: Prefix for zipped results
- `database`: Athena database name. Defaults to "default"
- `transporter`: (Optional) Configuration for file transfer

### Transport Options

#### SFTP Transport

```json
"transporter": {
  "type": "sftp",
  "config": {
    "sftp_host": "sftp.example.com",
    "sftp_port": "22",
    "sftp_user": "username",
    "sftp_password": "password",
    "sftp_path": "/uploads"
  }
}
```

#### Sharefile Transport

```json
"transporter": {
  "type": "sharefile",
  "config": {
    "host": "company.sharefile.com",
    "client_id": "client_id_value",
    "client_secret": "client_secret_value",
    "username": "username",
    "password": "password",
    "sharefile_folder": "folder_id"
  }
}
```

## Special Behaviors

### Single Query vs Multiple Queries

- When running a single query, the workflow sends the CSV file directly to the transporter
- When running multiple queries, the workflow zips the files together first

### Directory Creation

The SFTP transporter will attempt to create directories if they don't exist.

### Error Handling

The workflow includes comprehensive error handling:
- Detailed error messages are passed through the state machine
- Error logs are stored in S3
- Transport failures are properly reported with specific error information

## Monitoring

You can monitor the workflow's progress through:
- Step Functions execution history
- CloudWatch Logs for each Lambda function
- S3 manifests and error files

## Extending the Workflow

### Adding a New Transport Method

To add a new transport method:
1. Add a new transporter class to the `file-transporter-lambda` Lambda
2. Update the Lambda handler to recognize the new transport type
3. No changes to the state machine are needed

### Adding Pre/Post Processing Steps

You can extend the workflow by:
1. Adding new Lambda functions for specific processing needs
2. Updating the state machine to include these new processing steps

## Troubleshooting

### Common Issues

1. **Query Execution Failures**
   - Check Athena query syntax
   - Verify database and table permissions

2. **Transport Failures**
   - Verify connectivity to the transport endpoint
   - Check credentials and permissions
   - Inspect the detailed error message in the Step Functions execution

3. **Missing or Invalid Files**
   - Ensure SQL files exist at the specified locations
   - Verify the query index file format is correct

### Debugging

1. Enable detailed CloudWatch logging for Lambda functions
2. Check the Step Functions execution history for state transitions
3. Examine any error files created in the output S3 bucket
