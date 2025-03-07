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
                    
                    completed_queries.append({
                        'status': 'SUCCEEDED',
                        'query_execution_id': query_execution_id,
                        'query_id': query_id,
                        'result_file': result_file,
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
    result_files = [query.get('result_file') for query in completed_queries]
    
    return {
        'status': 'SUCCEEDED',
        'result_files': result_files,
        'timestamp': timestamp,
        'output_bucket': output_bucket
    }