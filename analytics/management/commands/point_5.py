from django.core.management.base import BaseCommand, CommandError

from analytics.models import TableA
from analytics.utils.big_query import bq_exporter
from analytics.utils.storage import st_reader


import csv
import logging
import os

logger = logging.getLogger('main')

def point_5():
    
    bucket_name = os.environ.get('BUCKET_NAME')
    project = os.environ.get('PROJECT_NAME')
    dataset_id = os.environ.get('DATASET_ID')
    table_id = "TableA"

    destination_uri = bq_exporter(project,dataset_id,bucket_name,table_id)
    logger.info(f"Exported {project}:{dataset_id}.{table_id} to {destination_uri}")

    logger.info(f"Reading {table_id} from bucket {'/'.join(destination_uri.split('/')[:-1])}")
    blob = st_reader(bucket_name,table_id)

    with blob.open("r") as table_a:
        reader = csv.reader(table_a)
        next(reader)

        TableA.objects.all().delete()
        for i, row in enumerate(reader):
            logger.info(f'Writing to TableA n {i+1} row: {row}...')
            tableA = TableA(day=row[0],
                        top_term=row[1],
                        rank=row[2])
            tableA.save()

class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        try:
            point_5()
        except Exception as e:
            logger.error(f"Error while executing the program: {e}")