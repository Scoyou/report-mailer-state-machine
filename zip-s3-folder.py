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
        "raw_result_files": ["s3://bucket/path/file1.csv", "s3://bucket/path/file2.csv", ...],
        "formatted_csv_locations": ["s3://bucket/path/processed_file1.csv", "s3://bucket/path/processed_file2.csv", ...],
        "timestamp": "20250306_121530",
        "output_bucket": "my-output-bucket",
        "zip_prefix": "zipped-results/"
    }
    """
    raw_result_files = event.get('raw_result_files', [])
    formatted_csv_locations = event.get('formatted_csv_locations', [])
    timestamp = event.get('timestamp', '')
    output_bucket = event.get('output_bucket', '')
    zip_prefix = event.get('zip_prefix', 'zipped-results/')
    
    result_files = formatted_csv_locations if formatted_csv_locations else raw_result_files
    
    if not zip_prefix.endswith('/'):
        zip_prefix += '/'
    
    destination_key = f"{zip_prefix}athena_results_{timestamp}.zip"
    
    try:
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for i, file_uri in enumerate(result_files):
                if file_uri.startswith('s3://'):
                    parts = file_uri.replace('s3://', '').split('/')
                    source_bucket = parts[0]
                    source_key = '/'.join(parts[1:])
                else:
                    source_bucket = output_bucket
                    source_key = file_uri
                
                try:
                    response = s3.get_object(Bucket=source_bucket, Key=source_key)
                    content = response['Body'].read()
                    
                    filename = source_key.split('/')[-1]
                    if not filename:
                        filename = f"file_{i}.csv"
                    
                    zip_file.writestr(filename, content)
                    print(f"Added file {filename} to zip")
                except Exception as file_error:
                    print(f"Error processing file {file_uri}: {str(file_error)}")
        
        zip_buffer.seek(0)
        
        s3.put_object(
            Body=zip_buffer,
            Bucket=output_bucket,
            Key=destination_key,
            ContentType='application/zip'
        )
        
        print(f"Successfully created and uploaded zip file to s3://{output_bucket}/{destination_key}")
        
        manifest = {
            'status': 'success',
            'timestamp': timestamp,
            'file_count': len(result_files),
            'source_files': result_files,
            'raw_files': raw_result_files,
            'formatted_files': formatted_csv_locations,
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
        
        return {
            'status': 'failed',
            'error': str(e),
            'output_bucket': output_bucket,
            'timestamp': timestamp
        }