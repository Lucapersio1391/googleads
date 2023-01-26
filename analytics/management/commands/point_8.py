from django.core.management.base import BaseCommand, CommandError

from google.cloud import bigquery
from analytics.utils import SCHEMA
from analytics.utils.big_query import BQLoader
import pandas as pd
import os
import logging


NEW_FIELDS = [
    bigquery.SchemaField("CpA", "FLOAT"),
    bigquery.SchemaField("CpC", "FLOAT"),

]

CHANNELGROUPING_COSTS = {
                'Organic Search': 0,
                'Direct': 0,
                'Referral': 500000,
                'Paid Search': 1000000,
                'Display': 500000,
                'Social': 600000,
                'Affiliates': 800000
                }
logger = logging.getLogger('main')

def compute_cpa(row)->float:
    try:
        return row['advertisingCosts'] / row['totals']['transactions']
    except:
        return 0.0

def compute_cpc(row)->float:
    try:
        return row['advertisingCosts'] / row['totals']['hits']
    except:
        return 0.0


def point_8():
    
    destination_project = os.environ.get('PROJECT_NAME')
    destination_dataset_id = os.environ.get('DATASET_ID')
    destination_table_id = 'GoogleAds'
    table_path = f'{destination_project}.{destination_dataset_id}.{destination_table_id}'
    query = f"""
    SELECT * FROM {table_path}"""
    df = pd.read_gbq(query, project_id=destination_project)

    df['advertisingCosts'] = df['channelGrouping'].map(lambda x:CHANNELGROUPING_COSTS[x])
    total_revenues = sum(d['transactionRevenue'] for d in df['totals'] if d['transactionRevenue'])
    total_costs = sum(df['advertisingCosts'])
    roas = (total_revenues - total_costs) / total_costs
    logger.info(f'Return on advertising spend is: {roas:.2f}%')
    df['CpA'] = df.apply(lambda row: compute_cpa(row), axis=1)
    df['CpC'] = df.apply(lambda row: compute_cpc(row), axis=1)
    
    SCHEMA.extend(NEW_FIELDS)

    bq_loader = BQLoader(destination_project,destination_dataset_id,destination_table_id,logger,SCHEMA,format='parquet')

    bq_loader.from_dataframe(df)


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        try:
            point_8()
        except Exception as e:
            logger.error(f"Error while executing the program: {e}")