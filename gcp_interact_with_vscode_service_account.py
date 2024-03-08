from google.cloud import bigquery
from google.oauth2 import service_account 

service_account_json = 'C:\\Users\\Anand.Pandian\\Desktop\\service_account_key.json'
 
# give project_ID
project_id = "hidden-mapper-414810"
 

credentials = service_account.Credentials.from_service_account_file(
    service_account_json
)
 
# Client object 
client = bigquery.Client(project=project_id, credentials=credentials)
 
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