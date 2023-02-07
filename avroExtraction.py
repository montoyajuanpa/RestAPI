from google.cloud import bigquery 

client = bigquery.Client()
project = 'globant-376521'
dataset_id = 'globant-376521.Globant'
table_id = 'globant-376521.Globant.jobs'
bucket_name ='globant_bucket'

destination_uri = 'gs://{}/{}'.format(bucket_name, 'fileName')
dataset_ref = client.dataset(dataset_id, project=project)
table_ref = dataset_ref.table(table_id)

job_config = bigquery.job.ExtractJobConfig()
job_config.destination_format = bigquery.DestinationFormat.AVRO

extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        location="EU",
        )  
extract_job.result() 



