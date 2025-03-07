import boto3
import json
import os
from datetime import datetime

def lambda_handler(event, context):
    """
    Prepares parameters for parallel execution of Athena queries
    """
    # Get input parameters
    output_bucket = event.get('output_bucket')
    output_prefix = event.get('output_prefix', 'athena-results/')
    database = event.get('database', 'default')
    queries = event.get('queries', [])
    
    # Generate timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Create parameter list for each query
    query_params = []
    for i, query in enumerate(queries):
        query_id = f"query_{i+1}_{timestamp}"
        output_location = f"{output_prefix}{query_id}/"
        
        query_params.append({
            'query': query,
            'query_id': query_id,
            'database': database,
            'output_bucket': output_bucket,
            'output_location': output_location
        })
    
    # Return the parameters for parallel execution
    return {
        'query_params': query_params,
        'timestamp': timestamp,
        'output_bucket': output_bucket,
        'output_prefix': output_prefix,
        'database': database
    }