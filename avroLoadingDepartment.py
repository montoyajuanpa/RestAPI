import io

from google.cloud import bigquery

client = bigquery.Client()


table_id = "globant-376521.Globant.deparments"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("department", "STRING"),
    ],
)

body = io.BytesIO(b"55,Engineering")
client.load_table_from_file(body, table_id, job_config=job_config).result()
previous_rows = client.get_table(table_id).num_rows
assert previous_rows > 0

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.AVRO,
)

uri = "gs://globant_bucket/Department"
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  

load_job.result() 

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))