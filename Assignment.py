import boto3
from azure.storage.blob import BlobServiceClient

# AWS S3 credentials
aws_access_key_id = 'YOUR_AWS_ACCESS_KEY_ID'
aws_secret_access_key = 'YOUR_AWS_SECRET_ACCESS_KEY'
aws_bucket_name = 'YOUR_AWS_BUCKET_NAME'

# Azure Blob Storage credentials
azure_connection_string = 'YOUR_AZURE_CONNECTION_STRING'
azure_container_name = 'YOUR_AZURE_CONTAINER_NAME'

# Initialize AWS S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

# Initialize Azure Blob Storage client
blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
blob_container_client = blob_service_client.get_container_client(azure_container_name)

# List objects in AWS S3 bucket
response = s3_client.list_objects_v2(Bucket=aws_bucket_name)

# Transfer objects from AWS S3 to Azure Blob Storage
for obj in response['Contents']:
    aws_object_key = obj['Key']
    azure_blob_name = aws_object_key.split('/')[-1]  # Assuming simple conversion for demonstration
    aws_object = s3_client.get_object(Bucket=aws_bucket_name, Key=aws_object_key)
    azure_blob_client = blob_container_client.get_blob_client(azure_blob_name)
    azure_blob_client.upload_blob(aws_object['Body'].read())

print("Data migration from AWS S3 to Azure Blob Storage completed.")
