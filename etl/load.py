from google.oauth2 import service_account
from google.cloud import storage
import json
import csv
import io

# Create credentials object from the service account JSON key file
credentials = service_account.Credentials.from_service_account_file(
    "/Users/thomasrivieres/etl_for_ecommerce_data/service_account.json")


def load_data_into_gcs(data, destination_blob_name):
    # Initialize GCS client
    storage_client = storage.Client(credentials=credentials)

    # Get the bucket
    bucket = storage_client.bucket("data_showcase_project")

    # Convert data list to JSON string
    data = json.dumps(data)

    # Define destination blob
    blob = bucket.blob(destination_blob_name)


    blob.upload_from_string(data)



def transform_data_to_csv():
    try:
        # Initialize GCS client
        storage_client = storage.Client(credentials=credentials)

        bucket = storage_client.bucket("data_showcase_project")

        all_blobs = list(bucket.list_blobs())

        for blob in all_blobs:
            if blob.name.startswith("raw/"):
                content_str = blob.download_as_string().decode('utf-8')
                content_list = json.loads(content_str)

                # Extract fieldnames from the keys of the first dictionary in the content_list
                fieldnames = content_list[0].keys()

                # Write data to CSV file
                csv_string = io.StringIO()
                writer = csv.DictWriter(csv_string, fieldnames=fieldnames)

                # Write header row
                writer.writeheader()

                # Write data rows
                writer.writerows(content_list)

                csv_string.seek(0)

                # Upload CSV string to GCS bucket
                blob_name = f'gold/{blob.name.split("/")[-1].split(".")[0]}.csv'
                blob = bucket.blob(blob_name)
                blob.upload_from_string(csv_string.getvalue())


    except Exception as e:
        print(f"Error: {e}")





