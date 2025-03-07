import json

def lambda_handler(event, context):
    """
    Helper Lambda to extract S3 key from an S3 URI
    
    Input:
    {
        "s3_uri": "s3://bucket-name/path/to/file.csv"
    }
    
    Output:
    {
        "bucket": "bucket-name",
        "key": "path/to/file.csv",
        "filename": "file.csv"
    }
    """
    s3_uri = event.get('s3_uri', '')
    
    if not s3_uri or not s3_uri.startswith('s3://'):
        return {
            'error': 'Invalid S3 URI format',
            'uri': s3_uri
        }
    
    s3_path = s3_uri.replace('s3://', '')
    
    parts = s3_path.split('/', 1)
    
    if len(parts) < 2:
        return {
            'error': 'Invalid S3 URI format - no key',
            'uri': s3_uri
        }
    
    bucket = parts[0]
    key = parts[1]
    
    filename = key.split('/')[-1]
    
    return {
        'bucket': bucket,
        'key': key,
        'filename': filename
    }