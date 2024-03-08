from google.cloud import bigquery
from google.oauth2 import service_account 
from google.cloud import secretmanager
import json

def access_secret_version(service_account_json,project_id, secret_id, version_id='latest'):

    credentials = service_account.Credentials.from_service_account_file(service_account_json)

    #SecretManagerServiceClient-Used to make requests to the Secret Manager API.
    client = secretmanager.SecretManagerServiceClient(credentials=credentials)

    # Build the secret version name.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # This triggers a request to Secret Manager to retrieve the details of the specified version of the secret.
    response = client.access_secret_version(name=name)

    #This extracts the payload (the actual secret data) from the response using response.payload.data. 
    #The payload is then decoded as UTF-8 to convert it from bytes to a string.
    payload = response.payload.data.decode('UTF-8')

    return payload


service_account_json = 'C:\\Users\\{UserName}\\Desktop\\service_account_key.json'
secret_id = "service_account_json_key"
project_id = "hidden-mapper-414810"
 

 #Calling the function to retrieve the secret value
secret_value = access_secret_version(service_account_json,project_id, secret_id)
print(secret_value)

#The value returned from the function is string, but service_account_info function expects dictionary-like object
service_account_info = json.loads(secret_value)

#Configuring the credential with the json key retrieved from above steps
credentials = service_account.Credentials.from_service_account_info(service_account_info,scopes=["https://www.googleapis.com/auth/bigquery"])

#Construct a BigQuery client object.
client = bigquery.Client(credentials=credentials, project=project_id)
 
# dataset object give which dataset that you want to list 
dataset_ref = client.dataset('audit')
 
# get dataset
dataset = client.get_dataset(dataset_ref)
 
# List tables in the dataset
tables = list(client.list_tables(dataset))
 
# Print names of tables in the dataset
print("Tables in dataset {}: ".format(dataset.dataset_id))
for table in tables:
    print("\t{}".format(table.table_id))
