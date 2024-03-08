from google.cloud import bigquery
from google.oauth2 import service_account 
from google.cloud import storage
import os
from datetime import datetime

service_account_json = 'C:\\Users\\{UserName}\\Desktop\\service_account_key.json'
 
now = datetime.now()
timestamp = now.strftime("%Y%m%d%H%M")
# give project_ID
project_id = "hidden-mapper-414810"
bucket_name = 'gcp-dev-03'
project = "hidden-mapper-414810"
dataset_id = "insertjobdataset"
table_id = "etlauditlog"

credentials = service_account.Credentials.from_service_account_file(
    service_account_json
)
 
# Client object 
client = bigquery.Client(project=project_id, credentials=credentials)

#Set up storage client
storage_client = storage.Client.from_service_account_json(service_account_json)

# Name of the bucket object (file) in the bucket
blob_name = f"{table_id}_{timestamp}.csv"

# Path to save the CSV file temporarily
temp_csv_file = f"{table_id}_{timestamp}.csv"

#query to extract data from table
query = "select * from " + project_id + "." + dataset_id + "." + table_id

#saving the results of the query to a DataFrame 
df_query_result = client.query(query).to_dataframe()
print(df_query_result)

#Write object to a comma-separated values (csv) file.
df_query_result.to_csv(temp_csv_file,index=False)

# Upload the CSV file to the bucket
bucket = storage_client.bucket(bucket_name)
blob = bucket.blob(blob_name)
blob.upload_from_filename(temp_csv_file)

os.remove(temp_csv_file)
