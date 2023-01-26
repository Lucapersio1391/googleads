import os
from google.cloud import bigquery

CREDENTIALS = os.environ.get('CREDENTIALS')

SCHEMA_TOTALS = [
    bigquery.SchemaField("transactions", "INTEGER"),
    bigquery.SchemaField("hits", "INTEGER"),
    bigquery.SchemaField("transactionRevenue", "INTEGER"),
]

SCHEMA_TRAFFICSOURCE = [
    bigquery.SchemaField("campaign", "STRING"),
    bigquery.SchemaField("keyword", "STRING"),
]

SCHEMA = [
    bigquery.SchemaField("fullvisitorId", "STRING"),
    bigquery.SchemaField("totals", "RECORD", fields=SCHEMA_TOTALS),
    bigquery.SchemaField("trafficSource", "RECORD", fields=SCHEMA_TRAFFICSOURCE),
    bigquery.SchemaField("channelGrouping", "STRING"),
]



SCHEMA_TOTALS_DF = [
    {"name": "transactions", "type": "INTEGER"},
    {"name": "hits", "type": "INTEGER"},
    {"name": "transactionRevenue", "type": "INTEGER"}
    ]

SCHEMA_TRAFFICSOURCE_DF = [
    {"name": "campaign", "type": "STRING"},
    {"name": "keyword", "type": "STRING"}
    ]

SCHEMA_DF = [
    {"name": "fullvisitorId", "type": "STRING"},
    {"name": "totals", "type": "RECORD", "fields": SCHEMA_TOTALS_DF},
    {"name": "trafficSource", "type": "RECORD", "fields": SCHEMA_TRAFFICSOURCE_DF},
    {"name": "channelGrouping", "type": "STRING"},
    {"name": "advertisingCosts", "type": "FLOAT"},
    {"name": "CpA", "type": "FLOAT"},
    {"name": "CpC", "type": "FLOAT"}
    ]
