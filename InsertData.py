from google.cloud import bigquery

client = bigquery.Client()

gcs_uri = 'gs://cloud-samples-data/bigquery/us-states/us-states.json'

dataset = client.create_dataset('us_states_dataset')
table = dataset.table('us_states_table')

job_config = bigquery.job.LoadJobConfig()
job_config.schema = [
    bigquery.SchemaField('name', 'STRING'),
    bigquery.SchemaField('post_abbr', 'STRING'),
]
job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

load_job = client.load_table_from_uri(gcs_uri, table, job_config=job_config)

print('JSON file loaded to BigQuery')