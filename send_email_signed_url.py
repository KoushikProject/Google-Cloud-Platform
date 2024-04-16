import functions_framework
from google.cloud import bigquery
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from google.cloud import storage
from datetime import datetime,timedelta
from google.oauth2 import service_account 

def generate_signed_url(bucket_name, file_name):
    """Generate a signed URL for a file in Google Cloud Storage."""

    service_account_json = 'C:\\Users\\Anand.Pandian\\Desktop\\hidden-mapper-414810-cd280404d6a9.json'
    credentials = service_account.Credentials.from_service_account_file(
    service_account_json
    )
    expiration = datetime.now() + timedelta(days=1) #datetime.now() + timedelta(minutes=5)
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    signed_url = blob.generate_signed_url(expiration=expiration)
    return signed_url

def send_email(subject, attachment_signed_url):
    # Set up the SMTP server
    smtp_server = "smtp.office365.com"
    smtp_port = 587
    sender_email = "noreply@elait.com"
    receiver_email = "Koushik.Rangarajan@elait.com"
    cc_emails = ["Anand.Pandian@elait.com","Monesh.Pattabi@elait.com","gokul.gururaaj@elait.com"]
    cc_email = ', '.join(cc_emails)
    password = 'your password'

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Cc"] = cc_email
    message["Subject"] = subject

    email_signature = "<p>Best regards,<br>Koushik R<br>Elait</p>"

    # # Add greetings and additional content to the email body
    # email_content = """
    #     <p>Dear User,</p>
    #     <p>Please find below the latest Employee Data Report:</p>
    #     <p>{}</p>
        
    #     {}
    #     """.format(body,email_signature)

  
    # Add the attachment link to the email body
    email_content = """
        <p>Dear User,</p>
        <p>Please find below the latest Employee Data Report:</p>
        <p>Download the attachment from the following link:</p>
        <p><a href="{}">Download Report</a></p>
        {}
        """.format(attachment_signed_url, email_signature)

    # Add body to email
    message.attach(MIMEText(email_content, "html"))

    # Connect to SMTP server and send email
    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(sender_email, password)
        recipients = [receiver_email] + cc_emails
        server.sendmail(sender_email, recipients, message.as_string())


def salary_calc():
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    project_id = "hidden-mapper-414810"
    dataset_id = "assessment_1"
    table_id = "Employee"
    bucket_name = 'gcp-dev-03'
    now = datetime.now()
    timestamp = now.strftime("%Y%m%d%H")
    file_name = 'highest_salary_'+timestamp+'.csv'
    
    service_account_json = 'C:\\Users\\Anand.Pandian\\Desktop\\hidden-mapper-414810-cd280404d6a9.json'


    credentials = service_account.Credentials.from_service_account_file(
    service_account_json
    )
 
    # Client object 
    client = bigquery.Client(project=project_id, credentials=credentials)

    # Query to extract data from table
    query = f"WITH MaxSalaryPerDepartment AS (SELECT Department, MAX(Salary) AS MaxSalary FROM `{project_id}.{dataset_id}.{table_id}` GROUP BY Department) SELECT e.Department, e.Name AS Highest_Salary_Staff, m.MaxSalary AS Highest_Salary FROM `{project_id}.{dataset_id}.{table_id}` e JOIN MaxSalaryPerDepartment m ON e.Department = m.Department AND e.Salary = m.MaxSalary;"

    # Saving the results of the query to a DataFrame
    df_query_result = client.query(query).to_dataframe()

    # Convert DataFrame to CSV format as a string
    csv_string = df_query_result.to_csv(index=False)

    # Create a storage client
    storage_client = storage.Client(credentials=credentials)

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob (file) in the bucket
    blob = bucket.blob(file_name)

    # Upload the CSV string to the blob
    blob.upload_from_string(csv_string, content_type='text/csv')
    # Send email with DataFrame content

    print("above attachment")
    # Generate a signed URL for the file
    attachment_signed_url = generate_signed_url(bucket_name, file_name)

    subject = "Employee Data Report"
    # body = df_query_result.to_html(index=False)
    send_email(subject,attachment_signed_url)

salary_calc()


