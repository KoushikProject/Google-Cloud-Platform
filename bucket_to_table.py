from google.oauth2 import service_account 
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
import os




def hello_gcs():
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    service_account_json = 'C:\\Users\\Anand.Pandian\\Desktop\\service_account_key.json'

    #Python client to our BigQuery instance
    credentials = service_account.Credentials.from_service_account_file(
                    service_account_json
    )
 
    table_name = 'exam'
    project_id = 'hidden-mapper-414810'
    dataset_id = 'cloud_function'
    file_name = 'exam.csv'

    # Client object 
    client = bigquery.Client(credentials=credentials,project=project_id)
    
    storage_client = storage.Client.from_service_account_json(service_account_json)

    bucket = storage_client.bucket('gcp-dev-02')
    blob = bucket.blob('exam.csv')

    #Configuration options for load jobs.
    #write_disposition - Describes whether a job should overwrite or append the existing destination table if it already exists.
    job_config = bigquery.LoadJobConfig(write_disposition = "WRITE_APPEND")

    
    #Reading the csv file and putting it into a dataframe.

    # with blob.open("r") as f:
    #     print(f.read())
    
    blob.download_to_filename('temp.csv')

    df_data = pd.read_csv('temp.csv')
    print(df_data)

    os.remove('temp.csv')

    #Load contents of a pandas DataFrame to a table.
    job = client.load_table_from_dataframe(df_data,"{}.{}.{}".format(project_id,dataset_id,table_name),job_config=job_config)


hello_gcs()