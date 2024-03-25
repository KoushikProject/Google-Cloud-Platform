You need storage.objects.delete,storage.buckets.get,storage.objects.list

from google.cloud import bigquery
from google.oauth2 import service_account 
from google.cloud import storage
from datetime import datetime
import pytz

service_account_json = 'C:\\Users\\Anand.Pandian\\Desktop\\hidden-mapper-414810-b8eb70c1117c.json'
 
def delete_file_by_update_time(bucket_name,update_time):
    """Deletes a file from a GCS bucket by its update time.

    Args:
        bucket_name: The name of the GCS bucket.
        file_name: The name of the file to delete.
        update_time: The update time of the file to delete.
    """

    credentials = service_account.Credentials.from_service_account_file(
        service_account_json
    )
    
    
    storage_client = storage.Client.from_service_account_json(service_account_json)

    # Get the bucket.
    bucket = storage_client.get_bucket(bucket_name)

    # List the blobs in the bucket.
    blobs = bucket.list_blobs(max_results=1000)

    # Filter the blobs based on last modified time and delete the blob.

    for blob in blobs:
        if blob.updated <= update_time:
            blob.delete()
            #Print a confirmation message.
            print('File deleted successfully.')


bucket_name = 'gcp-dev-02'

#Create a datetime object for comparison
ist_dt = datetime(2024, 3, 8, 23, 0, 0, tzinfo=pytz.timezone('Asia/Kolkata'))


delete_file_by_update_time(bucket_name, ist_dt)
