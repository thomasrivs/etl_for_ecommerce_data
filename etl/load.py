from google.oauth2 import service_account
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

# Create credentials object from the service account JSON key file
credentials = service_account.Credentials.from_service_account_file("service_account.json")

def load_data_into_gcs(data, destination_blob_name):
    try:
        # Initialize GCS client
        storage_client = storage.Client(credentials=credentials)

        # Get the bucket
        bucket = storage_client.bucket("data_showcase_project")

        # Convert data list to Parquet file
        array = pa.array(data)
        table = pa.Table.from_arrays([array], names=['data'])


        # Write Parquet data to a temporary file
        with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
            pq.write_table(table, temp_file.name)

            # Define destination blob
            blob = bucket.blob(destination_blob_name)

            # Upload Parquet data from temporary file
            blob.upload_from_filename(temp_file.name)

        print("Upload complete")

    except Exception as e:
        print(f"Error: {e}")

#if __name__ == "__main__":
#    data_test = [{'sessions': 4390, 'add_to_cart': 1700, 'initiate_checkout': 563, 'sales': 167, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 1}, {'sessions': 5661, 'add_to_cart': 1414, 'initiate_checkout': 1083, 'sales': 197, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 2}]
#    load_data_into_gcs(data_test, "test.parquet")

