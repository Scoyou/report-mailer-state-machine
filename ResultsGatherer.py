import boto3
import json
import os
import time

def lambda_handler(event, context):
    """
    Lambda function to gather and monitor Athena query results
    """
    # Initialize Athena client
    athena_client = boto3.client('athena')
    
    # Get input parameters
    query_results = event.get('query_results', [])
    output_bucket = event.get('output_bucket')
    timestamp = event.get('timestamp')
    
    # Check if any queries are still running
    running_queries = []
    completed_queries = []
    failed_queries = []
    
    for result in query_results:
        if result.get('status') == 'RUNNING':
            running_queries.append(result)
        elif result.get('status') == 'SUCCEEDED':
            completed_queries.append(result)
        else:
            failed_queries.append(result)
    
    # If we have running queries, check their status
    if running_queries:
        for query in running_queries:
            query_execution_id = query.get('query_execution_id')
            query_id = query.get('query_id')
            
            try:
                response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
                query_status = response['QueryExecution']['Status']['State']
                
                if query_status == 'SUCCEEDED':
                    result_file = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                    print(f"Query {query_id} completed successfully. Results at: {result_file}")
                    
                    # Since we have updated ExecuteSingleQuery, we need to process the CSV here too
                    s3_client = boto3.client('s3')
                    output_location = query.get('output_location')
                    processed_csv_key = f"{output_location}processed_{query_id}.csv"
                    
                    # Get the raw result and process it
                    result_file_key = result_file.replace(f"s3://{output_bucket}/", "")
                    
                    try:
                        # First check if processed file already exists (might have been created by ExecuteSingleQuery)
                        s3_client.head_object(Bucket=output_bucket, Key=processed_csv_key)
                        formatted_csv_location = f"s3://{output_bucket}/{processed_csv_key}"
                    except:
                        # If not, process it now
                        import io
                        import csv
                        
                        csv_obj = s3_client.get_object(Bucket=output_bucket, Key=result_file_key)
                        csv_content = csv_obj['Body'].read().decode('utf-8').splitlines()
                        
                        csv_buffer = io.StringIO()
                        writer = csv.writer(csv_buffer)
                        for row in csv.reader(csv_content):
                            writer.writerow(row)
                        
                        s3_client.put_object(
                            Bucket=output_bucket, 
                            Key=processed_csv_key, 
                            Body=csv_buffer.getvalue(), 
                            ContentType='text/csv'
                        )
                        
                        formatted_csv_location = f"s3://{output_bucket}/{processed_csv_key}"
                    
                    completed_queries.append({
                        'status': 'SUCCEEDED',
                        'query_execution_id': query_execution_id,
                        'query_id': query_id,
                        'raw_result_file': result_file,
                        'formatted_csv_location': formatted_csv_location,
                        'output_bucket': output_bucket
                    })
                elif query_status in ['FAILED', 'CANCELLED']:
                    error_message = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    print(f"Query {query_id} failed: {error_message}")
                    
                    failed_queries.append({
                        'status': 'FAILED',
                        'query_execution_id': query_execution_id,
                        'query_id': query_id,
                        'error': error_message,
                        'output_bucket': output_bucket
                    })
                else:
                    # Still running, keep in the running list
                    print(f"Query {query_id} is still {query_status}")
                    
            except Exception as e:
                print(f"Error checking query {query_id}: {str(e)}")
                failed_queries.append({
                    'status': 'ERROR',
                    'query_execution_id': query_execution_id,
                    'query_id': query_id,
                    'error': str(e),
                    'output_bucket': output_bucket
                })
    
    # If we still have running queries, we need to wait
    if running_queries and not failed_queries:
        # We have queries still running and no failures
        # Create an updated list of query results
        updated_results = completed_queries + running_queries
        
        # Return the updated results for another check
        return {
            'status': 'RUNNING',
            'query_results': updated_results,
            'timestamp': timestamp,
            'output_bucket': output_bucket
        }
    
    # If we have failed queries, return a failure status
    if failed_queries:
        return {
            'status': 'FAILED',
            'failed_queries': failed_queries,
            'completed_queries': completed_queries,
            'timestamp': timestamp,
            'output_bucket': output_bucket
        }
    
    # All queries completed successfully
    # Get both the raw and formatted result files
    raw_result_files = [query.get('raw_result_file', '') for query in completed_queries]
    formatted_csv_locations = [query.get('formatted_csv_location', '') for query in completed_queries]
    
    # Filter out empty strings
    raw_result_files = [f for f in raw_result_files if f]
    formatted_csv_locations = [f for f in formatted_csv_locations if f]
    
    return {
        'status': 'SUCCEEDED',
        'raw_result_files': raw_result_files,
        'formatted_csv_locations': formatted_csv_locations,
        'timestamp': timestamp,
        'output_bucket': output_bucket
    }