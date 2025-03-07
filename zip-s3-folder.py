import boto3
import io
import zipfile
import json

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda function to zip result files from Athena queries in Step Functions workflow
    
    Expected Input:
    {
        "result_files": ["s3://bucket/path/file1.csv", "s3://bucket/path/file2.csv", ...],
        "timestamp": "20250306_121530",
        "output_bucket": "my-output-bucket",
        "zip_prefix": "zipped-results/"
    }
    """
    # Extract input parameters from Step Functions
    result_files = event.get('result_files', [])
    timestamp = event.get('timestamp', '')
    output_bucket = event.get('output_bucket', '')
    zip_prefix = event.get('zip_prefix', 'zipped-results/')
    
    # Ensure zip_prefix ends with a slash
    if not zip_prefix.endswith('/'):
        zip_prefix += '/'
    
    # Generate output zip filename
    destination_key = f"{zip_prefix}athena_results_{timestamp}.zip"
    
    try:
        # Create zip file in memory
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, file_uri in enumerate(result_files):
                # Parse S3 URI
                if file_uri.startswith('s3://'):
                    parts = file_uri.replace('s3://', '').split('/')
                    source_bucket = parts[0]
                    source_key = '/'.join(parts[1:])
                else:
                    # If not a URI, assume it's a key in the output bucket
                    source_bucket = output_bucket
                    source_key = file_uri
                
                try:
                    # Get the object from S3
                    response = s3.get_object(Bucket=source_bucket, Key=source_key)
                    content = response['Body'].read()
                    
                    # Use just the filename for the zip entry
                    filename = source_key.split('/')[-1]
                    if not filename:
                        filename = f"file_{i}.csv"
                    
                    # Add file to the zip
                    zip_file.writestr(filename, content)
                    print(f"Added file {filename} to zip")
                except Exception as file_error:
                    print(f"Error processing file {file_uri}: {str(file_error)}")
                    # Continue with other files instead of failing completely
        
        # Reset buffer to beginning
        zip_buffer.seek(0)
        
        # Upload the zip to S3
        s3.put_object(
            Body=zip_buffer,
            Bucket=output_bucket,
            Key=destination_key,
            ContentType='application/zip'
        )
        
        print(f"Successfully created and uploaded zip file to s3://{output_bucket}/{destination_key}")
        
        # Create a JSON manifest file with details
        manifest = {
            'status': 'success',
            'timestamp': timestamp,
            'file_count': len(result_files),
            'source_files': result_files,
            'output_location': f"s3://{output_bucket}/{destination_key}"
        }
        
        manifest_key = f"{zip_prefix}manifest_{timestamp}.json"
        s3.put_object(
            Body=json.dumps(manifest),
            Bucket=output_bucket,
            Key=manifest_key,
            ContentType='application/json'
        )
        
        return {
            'status': 'success',
            'output_bucket': output_bucket,
            'output_key': destination_key,
            'file_count': len(result_files),
            'manifest': f"s3://{output_bucket}/{manifest_key}"
        }
        
    except Exception as e:
        error_details = {
            'status': 'failed',
            'error': str(e),
            'timestamp': timestamp
        }
        
        # Log the error to CloudWatch
        print(f"Error creating zip file: {str(e)}")
        
        # Create an error marker in S3
        try:
            error_key = f"{zip_prefix}error_{timestamp}.json"
            s3.put_object(
                Body=json.dumps(error_details),
                Bucket=output_bucket,
                Key=error_key,
                ContentType='application/json'
            )
        except Exception as marker_error:
            print(f"Failed to create error marker: {str(marker_error)}")
        
        # Return error details for Step Functions
        return {
            'status': 'failed',
            'error': str(e),
            'output_bucket': output_bucket,
            'timestamp': timestamp
        }