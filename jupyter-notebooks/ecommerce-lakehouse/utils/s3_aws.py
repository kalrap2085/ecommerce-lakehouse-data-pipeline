import boto3

_bucket_name = "ecommerce-pooja"

def get_s3_client():
    client = boto3.client('s3')
    return client

#Archives source files from landing for a given dataset and filename 
def archive_landing(filename: str, dataset: str) -> bool:
    
    try:
        client = get_s3_client()
    
        copy_source = {'Bucket' : _bucket_name, 'Key' : f"pipeline/landing/{dataset}/{filename}"}
    
        response = client.copy_object(Bucket = _bucket_name, CopySource = copy_source, Key = f"pipeline/archive/{dataset}/{filename}")
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            client.delete_object(Bucket = _bucket_name, Key = f"pipeline/landing/{dataset}/{filename}")
            return True
        else:
            return False
            
    except Exception as e:
        print("SPARK-APP: Error while archiving objects from landing", e)