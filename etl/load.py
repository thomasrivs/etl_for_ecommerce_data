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
        json_rows=data,
        destination="showcase-project-415618.ecommerce_data.test",
        job_config=job_config
    )

    job.result()  # Wait for the job to complete

if __name__ == "__main__":
    load_data_into_table([{'sessions': 12403, 'add_to_cart': 4847, 'initiate_checkout': 1347, 'sales': 410, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 1}, {'sessions': 6767, 'add_to_cart': 1473, 'initiate_checkout': 775, 'sales': 269, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 2}, {'sessions': 14749, 'add_to_cart': 5165, 'initiate_checkout': 2901, 'sales': 683, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 3}, {'sessions': 14240, 'add_to_cart': 4252, 'initiate_checkout': 2799, 'sales': 660, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 4}, {'sessions': 12234, 'add_to_cart': 3079, 'initiate_checkout': 2404, 'sales': 540, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 5}, {'sessions': 15282, 'add_to_cart': 5046, 'initiate_checkout': 1918, 'sales': 162, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 6}, {'sessions': 5897, 'add_to_cart': 1291, 'initiate_checkout': 809, 'sales': 128, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 7}, {'sessions': 15712, 'add_to_cart': 4611, 'initiate_checkout': 2301, 'sales': 653, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 8}, {'sessions': 10775, 'add_to_cart': 3133, 'initiate_checkout': 1535, 'sales': 395, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 9}, {'sessions': 9902, 'add_to_cart': 2670, 'initiate_checkout': 1154, 'sales': 279, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 10}, {'sessions': 8139, 'add_to_cart': 1884, 'initiate_checkout': 1271, 'sales': 256, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 11}, {'sessions': 16007, 'add_to_cart': 3970, 'initiate_checkout': 2180, 'sales': 252, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 12}, {'sessions': 16779, 'add_to_cart': 4488, 'initiate_checkout': 2970, 'sales': 727, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 13}, {'sessions': 14044, 'add_to_cart': 3310, 'initiate_checkout': 2572, 'sales': 656, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 14}, {'sessions': 14253, 'add_to_cart': 3475, 'initiate_checkout': 2259, 'sales': 540, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 15}, {'sessions': 11818, 'add_to_cart': 2961, 'initiate_checkout': 1360, 'sales': 134, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 16}, {'sessions': 11195, 'add_to_cart': 4396, 'initiate_checkout': 1803, 'sales': 379, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 17}, {'sessions': 11229, 'add_to_cart': 4417, 'initiate_checkout': 2002, 'sales': 146, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 18}, {'sessions': 13020, 'add_to_cart': 3700, 'initiate_checkout': 1598, 'sales': 649, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 19}, {'sessions': 16630, 'add_to_cart': 5759, 'initiate_checkout': 2624, 'sales': 412, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 20}, {'sessions': 9481, 'add_to_cart': 2172, 'initiate_checkout': 1888, 'sales': 407, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 21}, {'sessions': 13136, 'add_to_cart': 4123, 'initiate_checkout': 1995, 'sales': 588, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 22}, {'sessions': 12114, 'add_to_cart': 3843, 'initiate_checkout': 1523, 'sales': 384, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 23}, {'sessions': 16213, 'add_to_cart': 4447, 'initiate_checkout': 2749, 'sales': 364, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 2, 'hour': 0}, {'sessions': 14831, 'add_to_cart': 5471, 'initiate_checkout': 1896, 'sales': 565, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 2, 'hour': 1}])
