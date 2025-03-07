from datetime import datetime
import boto3
import json
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda function to load SQL queries from individual SQL files
    
    Expected Input:
    {
        "query_names": ["test_report", "report_2"],  # List of query names/keys to run
        "query_index_bucket": "my-query-bucket",     # S3 bucket containing queries
        "query_prefix": "queries/",                  # Prefix for SQL files if not using index (optional)
        "output_bucket": "my-output-bucket",         # Output bucket for results
        "output_prefix": "results/",                 # Prefix for results
        "database": "my_database"                    # Athena database name
    }
    
    """
    query_names = event.get('query_names', [])
    query_index_bucket = event.get('query_index_bucket')
    query_prefix = event.get('query_prefix', '')
        
    output_bucket = event.get('output_bucket')
    output_prefix = event.get('output_prefix', 'athena-results/')
    database = event.get('database', 'default')
    
    queries = []
    for query_name in query_names:
        sql_file_key = f"{query_prefix}{query_name}.sql"
            
        print(f"Loading query '{query_name}' from s3://{query_index_bucket}/{sql_file_key}")
            
        # Load the SQL file content
        try: 
            response = s3.get_object(Bucket=query_index_bucket, Key=sql_file_key)
            sql_content = response['Body'].read().decode('utf-8')
            
            queries.append({
                'name': query_name,
                'sql': sql_content,
                'file_path': sql_file_key
            })
            
            print(f"Successfully loaded query '{query_name}'")
        except Exception as e:
            print(f"Error loading query '{query_name}': {str(e)}")
            return {
                'status': 'error',
                'error': f"Failed to load query '{query_name}': {str(e)}",
                'query_name': query_name
            }
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    query_params = []
    for i, query_info in enumerate(queries):
        query_id = f"{query_info['name']}_{timestamp}"
        output_location = f"{output_prefix}{query_id}/"
        
        query_params.append({
            'query': query_info['sql'],
            'query_id': query_id,
            'query_name': query_info['name'],
            'database': database,
            'output_bucket': output_bucket,
            'output_location': output_location
        })
    
    return {
        'query_params': query_params,
        'timestamp': timestamp,
        'output_bucket': output_bucket,
        'output_prefix': output_prefix,
        'database': database
    }