from django.core.management.base import BaseCommand, CommandError

from analytics.utils.big_query import bq_exporter,bq_create_table_if_exists, BQLoader
from analytics.utils import SCHEMA


import logging
import os


logger = logging.getLogger('main')


def point_7():
    bucket_name = os.environ.get('BUCKET_NAME')
    destination_project = os.environ.get('PROJECT_NAME')
    destination_dataset_id = os.environ.get('DATASET_ID')
    destination_table_id = "GoogleAds"
    source_project = "bigquery-public-data"
    source_dataset_id = "google_analytics_sample"
    source_table_id = "ga_sessions_20170801"

    destination_uri = bq_exporter(source_project,source_dataset_id,bucket_name,source_table_id, location='US', format='avro')
    logger.info(f"Exported {source_project}:{source_dataset_id}.{source_table_id} to {destination_uri}")

    bq_create_table_if_exists(destination_project,destination_dataset_id,destination_table_id, logger)

    bq_loader = BQLoader(destination_project,destination_dataset_id,destination_table_id,logger,SCHEMA,bucket_name,source_table_id)
    bq_loader.from_uri()


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        try:
            point_7()
        except Exception as e:
            logger.error(f"Error while executing the program: {e}")