from google.cloud import bigquery
from google.cloud.bigquery.job.base import _AsyncJob
from google.cloud.exceptions import NotFound
from logging import Logger
from typing import List


from analytics.utils import SCHEMA
import os
from pandas import DataFrame

from analytics.utils import CREDENTIALS

bq_client = bigquery.Client.from_service_account_json(CREDENTIALS)

def bq_exporter(project: str, dataset_id: str, bucket_name: str, table_id: str, location: str= "EU", format="csv")-> str:

    job_config = bigquery.job.ExtractJobConfig()
    destination = {'avro': bigquery.DestinationFormat.AVRO,
                    'csv': bigquery.DestinationFormat.CSV}
    
    job_config.destination_format = destination[format]
    
    destination_uri = f"gs://{bucket_name}/{table_id}.{format}"
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = bq_client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location=location,
    )
    extract_job.result()

    return destination_uri

def bq_create_table_if_exists(project: str, dataset_id: str, table_id: str, logger: Logger):
    table_path = f"{project}.{dataset_id}.{table_id}"
    try:
        bq_client.get_table(table_path)
    except NotFound:
        logger.warning(f"Table {table_id} is not found.")
        table = bigquery.Table(table_path)
        table = bq_client.create_table(table)



class BQLoader():
    
    def __init__(self, project: str, dataset_id: str, destination_table_id, logger: Logger, schema: List[bigquery.SchemaField], bucket_name: str=None, source_table_id: str=None, format: str='avro') -> None:
        self.project = project
        self.dataset_id = dataset_id
        self.source_table_id = source_table_id
        self.destination_table_id = destination_table_id
        self.table_path = f"{self.project}.{self.dataset_id}.{self.destination_table_id}"
        self.bucket_name = bucket_name
        self.logger = logger
        self.schema = schema
        self.format = format
        self.client = bq_client
        self._check_delete_and_recreate_table
    
    @property
    def _get_format(self):
        return {'avro': bigquery.SourceFormat.AVRO, 'parquet': bigquery.SourceFormat.PARQUET}.get(self.format)

    @property
    def _get_config(self):
        job_config = bigquery.LoadJobConfig(schema=self.schema, source_format=self._get_format,schema_update_options='ALLOW_FIELD_ADDITION')
        return job_config
    
    @property
    def _check_delete_and_recreate_table(self):
        table = self.client.get_table(self.table_path)
        if table.num_rows:
            self.logger.warning(f"Table {self.destination_table_id} has been already populated. Let's delete it and recreate it.")
            self.client.delete_table(self.table_path)
            table = bigquery.Table(self.table_path)
            table = bq_client.create_table(table)
      

    def from_uri(self):

        uri = f"gs://{self.bucket_name}/{self.source_table_id}.avro"
        
        load_job = bq_client.load_table_from_uri(
            uri, self.table_path, location='EU',job_config=self._get_config
        )

        load_job.result()

        destination_table = bq_client.get_table(self.table_path)
        self.logger.info(f"Loaded {destination_table.num_rows} rows from uri.")


    def from_dataframe(self, df: DataFrame):
        job_config = self._get_config
        load_job = bq_client.load_table_from_dataframe(
            df, self.table_path, location='EU',job_config=job_config
        )

        load_job.result()

        destination_table = bq_client.get_table(self.table_path)
        self.logger.info(f"Loaded {destination_table.num_rows} rows from dataframe.")