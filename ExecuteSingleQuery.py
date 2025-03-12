import boto3
import time
import os
import io
import csv

def process_csv(
    s3_client, 
    s3_output_bucket, 
    result_file_location, 
    s3_csv_key
):
    """
    Process and save Athena query results as CSV.
    
    Args:
        s3_client: Boto3 S3 client
        s3_output_bucket: S3 bucket for query results
        result_file_location: S3 URL to the query results
        s3_csv_key: S3 key for saving processed CSV
    """
    result_file_key = result_file_location.replace(f"s3://{s3_output_bucket}/", "")
    csv_obj = s3_client.get_object(Bucket=s3_output_bucket, Key=result_file_key)
    csv_content = csv_obj['Body'].read().decode('utf-8').splitlines()
    
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    for row in csv.reader(csv_content):
        writer.writerow(row)
    
    s3_client.put_object(
        Bucket=s3_output_bucket, 
        Key=s3_csv_key, 
        Body=csv_buffer.getvalue(), 
        ContentType='text/csv'
    )
    
    return f"s3://{s3_output_bucket}/{s3_csv_key}"

def lambda_handler(event, context):
    """
    Lambda function to execute a single Athena query
    """
    # Initialize Athena client
    athena_client = boto3.client('athena')
    s3_client = boto3.client('s3')
    
    # Get query parameters
    query = event.get('query')
    query_id = event.get('query_id')
    query_name = event.get('query_name', query_id)  # Use the query name if provided
    database = event.get('database', 'default')
    output_bucket = event.get('output_bucket')
    output_location = event.get('output_location')
    
    try:
        # Execute the query
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': f's3://{output_bucket}/{output_location}'
            }
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"Started query execution with ID: {query_execution_id}")
        
        # Wait for query to complete
        max_execution_time = context.get_remaining_time_in_millis() - 10000  # Leave 10 seconds buffer
        start_time = time.time()
        
        query_status = 'RUNNING'
        while query_status in ['RUNNING', 'QUEUED']:
            # Check if we're about to timeout
            elapsed_time = (time.time() - start_time) * 1000
            if elapsed_time > max_execution_time:
                # We're about to timeout, just return the execution ID for monitoring
                return {
                    'status': 'RUNNING',
                    'query_execution_id': query_execution_id,
                    'query_id': query_id,
                    'output_bucket': output_bucket,
                    'output_location': output_location
                }
            
            # Check query status
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            query_status = response['QueryExecution']['Status']['State']
            
            if query_status in ['RUNNING', 'QUEUED']:
                time.sleep(2)  # Wait before checking again
        
        # Query is no longer running
        if query_status == 'SUCCEEDED':
            result_file = response['QueryExecution']['ResultConfiguration']['OutputLocation']
            print(f"Query {query_id} completed successfully. Results at: {result_file}")
            
            # Process the CSV file
            processed_csv_key = f"{output_location}{query_name}.csv"
            formatted_csv_location = process_csv(
                s3_client,
                output_bucket,
                result_file,
                processed_csv_key
            )
            print(f"Processed CSV saved at: {formatted_csv_location}")
            
            return {
                'status': 'SUCCEEDED',
                'query_execution_id': query_execution_id,
                'query_id': query_id,
                'raw_result_file': result_file,
                'formatted_csv_location': formatted_csv_location,
                'output_bucket': output_bucket,
                'output_location': output_location
            }
        else:
            error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
            print(f"Query {query_id} failed: {error_message}")
            
            return {
                'status': 'FAILED',
                'query_execution_id': query_execution_id,
                'query_id': query_id,
                'error': error_message,
                'output_bucket': output_bucket,
                'output_location': output_location
            }
    
    except Exception as e:
        print(f"Error executing query {query_id}: {str(e)}")
        return {
            'status': 'ERROR',
            'query_id': query_id,
            'error': str(e),
            'output_bucket': output_bucket,
            'output_location': output_location
        }