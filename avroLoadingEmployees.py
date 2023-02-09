import io

from google.cloud import bigquery

client = bigquery.Client()


table_id = "globant-376521.Globant.hired_employees"

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("datetime", "STRING"),
        bigquery.SchemaField("department_id", "INTEGER"),
        bigquery.SchemaField("job_id", "INTEGER")
    ],
)

body = io.BytesIO(b"12345,Mike,2021-07-27T16:02:08Z,1,2")
client.load_table_from_file(body, table_id, job_config=job_config).result()
previous_rows = client.get_table(table_id).num_rows
assert previous_rows > 0

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.AVRO,
)

uri = "gs://globant_bucket/hired_employees"
load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
)  

load_job.result() 

destination_table = client.get_table(table_id)
print("Loaded {} rows.".format(destination_table.num_rows))