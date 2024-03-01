from google.cloud import bigquery
from google.oauth2 import service_account

# Create credentials object from the service account JSON key file
credentials = service_account.Credentials.from_service_account_file(
    "service_account.json")

# Initialize a BigQuery client
client = bigquery.Client(credentials=credentials)




schema = [
    bigquery.SchemaField("sessions", "INTEGER"),
    bigquery.SchemaField("add_to_cart", "INTEGER"),
    bigquery.SchemaField("initiate_checkout", "INTEGER"),
    bigquery.SchemaField("sales", "INTEGER"),
    bigquery.SchemaField("country", "STRING"),
    bigquery.SchemaField("year", "INTEGER"),
    bigquery.SchemaField("month", "INTEGER"),
    bigquery.SchemaField("day", "INTEGER"),
    bigquery.SchemaField("hour", "INTEGER")
]

# Load the JSON data into BigQuery
job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
job_config.schema = schema
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED

def load_data_into_table(data):
    job = client.load_table_from_json(
        data,
        table_id=f"showcase-project-415618.{data['country']}",
        job_config=job_config,
    )

    job.result()  # Wait for the job to complete

if __name__ == "__main__":
    load_data_into_table({'sessions': 12623, 'add_to_cart': 4550, 'initiate_checkout': 1392, 'sales': 266, 'country': 'fr', 'year': '2023', 'month': '01', 'day': '01', 'hour': '01'})
