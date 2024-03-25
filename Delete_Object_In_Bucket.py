from google.cloud import bigquery
from google.oauth2 import service_account 
from google.cloud import storage
# json oda path kudu
service_account_json = 'C:\\Users\\Anand.Pandian\\Desktop\\hidden-mapper-414810-b8eb70c1117c.json'
 
# give project_ID
project_id = "hidden-mapper-414810"
 

credentials = service_account.Credentials.from_service_account_file(
    service_account_json
)
 
# Client object 
client = bigquery.Client(project=project_id, credentials=credentials)
 
storage_client = storage.Client.from_service_account_json(service_account_json)

# Get the bucket.
bucket = storage_client.get_bucket('gcp-dev-02')

# Get the blob.
blob = bucket.blob('exam.csv')

# Delete the blob.
blob.delete()

# Print a confirmation message.
print('File deleted successfully.')