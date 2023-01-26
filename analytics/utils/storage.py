from google.cloud import storage
from google.cloud.storage import Blob
from analytics.utils import CREDENTIALS



def st_reader(bucket_name: str, table_id: str)-> Blob:
    st_client = storage.Client.from_service_account_json(CREDENTIALS)
    bucket = st_client.bucket(bucket_name)
    blob = bucket.blob(f"{table_id}.csv")
    return blob


