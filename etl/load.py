from google.oauth2 import service_account
from google.cloud import storage
import json

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



#if __name__ == "__main__":
#    load_data_into_table([{'sessions': 10499, 'add_to_cart': 4091, 'initiate_checkout': 1514, 'sales': 261, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 1}, {'sessions': 15765, 'add_to_cart': 5569, 'initiate_checkout': 1849, 'sales': 389, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 2}, {'sessions': 7453, 'add_to_cart': 1603, 'initiate_checkout': 780, 'sales': 308, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 3}, {'sessions': 13060, 'add_to_cart': 2656, 'initiate_checkout': 2196, 'sales': 376, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 4}, {'sessions': 5968, 'add_to_cart': 2264, 'initiate_checkout': 967, 'sales': 65, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 5}, {'sessions': 9919, 'add_to_cart': 3960, 'initiate_checkout': 1507, 'sales': 425, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 6}, {'sessions': 16683, 'add_to_cart': 4338, 'initiate_checkout': 2362, 'sales': 545, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 7}, {'sessions': 9712, 'add_to_cart': 2442, 'initiate_checkout': 1695, 'sales': 256, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 8}, {'sessions': 13490, 'add_to_cart': 2873, 'initiate_checkout': 1615, 'sales': 372, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 9}, {'sessions': 7768, 'add_to_cart': 2137, 'initiate_checkout': 807, 'sales': 289, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 10}, {'sessions': 7829, 'add_to_cart': 1833, 'initiate_checkout': 1307, 'sales': 116, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 11}, {'sessions': 16279, 'add_to_cart': 5959, 'initiate_checkout': 2291, 'sales': 518, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 12}, {'sessions': 9841, 'add_to_cart': 2876, 'initiate_checkout': 1332, 'sales': 443, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 13}, {'sessions': 16482, 'add_to_cart': 6129, 'initiate_checkout': 2728, 'sales': 632, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 14}, {'sessions': 10082, 'add_to_cart': 2404, 'initiate_checkout': 1359, 'sales': 327, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 15}, {'sessions': 9586, 'add_to_cart': 3041, 'initiate_checkout': 1330, 'sales': 269, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 16}, {'sessions': 9998, 'add_to_cart': 2446, 'initiate_checkout': 1092, 'sales': 135, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 17}, {'sessions': 11145, 'add_to_cart': 3939, 'initiate_checkout': 2159, 'sales': 412, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 18}, {'sessions': 9764, 'add_to_cart': 3194, 'initiate_checkout': 1163, 'sales': 200, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 19}, {'sessions': 13978, 'add_to_cart': 3127, 'initiate_checkout': 1727, 'sales': 180, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 20}, {'sessions': 9487, 'add_to_cart': 2755, 'initiate_checkout': 1738, 'sales': 461, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 21}, {'sessions': 10870, 'add_to_cart': 3964, 'initiate_checkout': 1938, 'sales': 448, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 22}, {'sessions': 6637, 'add_to_cart': 2448, 'initiate_checkout': 1072, 'sales': 243, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 1, 'hour': 23}, {'sessions': 11135, 'add_to_cart': 3495, 'initiate_checkout': 2145, 'sales': 503, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 2, 'hour': 0}, {'sessions': 10584, 'add_to_cart': 3699, 'initiate_checkout': 1693, 'sales': 457, 'country': 'fr', 'year': 2023, 'month': 1, 'day': 2, 'hour': 1}])
