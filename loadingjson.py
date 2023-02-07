from google.cloud import bigquery
from flask import Flask, request, jsonify
import os
import json

app = Flask(__name__)


@app.route("/jobs", methods=["POST"])
def get_jobs():

    print(request.json)
    json_job = request.json
    table_schema = {"id": "id", "job": "STRING"}

    # globant-376521.Globant.jobs
    project_id = "globant-376521"
    dataset_id = "globant-376521.Globant"
    table_id = "globant-376521.Globant.jobs"

    client = bigquery.Client(project=project_id)
    dataset = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema_jobs(table_schema, len(json_job))

    try:
        job = client.load_table_from_json(
            json_job, table, job_config=job_config)
        print(job.result())
        return jsonify({"success": True}), 200
    except Exception as e:
        return f"an error ocurred: {e}"


def format_schema_jobs(schema, schema_len):
    formatted_schema = []
    for i in range(schema_len):
        formatted_schema.append(
            bigquery.SchemaField(schema["id"], schema["job"]))
    return formatted_schema


@app.route("/departments", methods=["POST"])
def get_departments():

    print(request.json)
    json_department = request.json
    table_schema = {"id": "id", "department": "STRING"}

    # globant-376521.Globant.jobs
    project_id = "globant-376521"
    dataset_id = "globant-376521.Globant"
    table_id = "globant-376521.Globant.deparments"

    client = bigquery.Client(project=project_id)
    dataset = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema_departments(
        table_schema, len(json_department))

    try:
        job = client.load_table_from_json(
            json_department, table, job_config=job_config)
        print(job.result())
        return jsonify({"success": True}), 200
    except Exception as e:
        return f"an error ocurred: {e}"


def format_schema_departments(schema, schema_len):
    formatted_schema = []
    for i in range(schema_len):
        formatted_schema.append(bigquery.SchemaField(
            schema["id"], schema["department"]))
    return formatted_schema


@app.route("/employees", methods=["POST"])
def get_employees():

    print(request.json)
    json_employees = request.json
    table_schema = {"name": "STRING", "datetime": "STRING",
                    "department_id": 55, "job_id": 44
                    }

    # globant-376521.Globant.jobs
    project_id = "globant-376521"
    dataset_id = "globant-376521.Globant"
    table_id = "globant-376521.Globant.hired_employees"

    client = bigquery.Client(project=project_id)
    dataset = client.dataset(dataset_id)
    table = dataset.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.schema = format_schema_employees(
        table_schema, len(json_employees))

    try:
        job = client.load_table_from_json(
            json_employees, table, job_config=job_config)
        print(job.result())
        return jsonify({"success": True}), 200
    except Exception as e:
        return f"an error ocurred: {e}"


def format_schema_employees(schema, schema_len):
    formatted_schema = []
    for i in range(schema_len):
        formatted_schema.append(bigquery.SchemaField(
            schema["name"], schema["datetime"], schema["department_id"], schema["job_id"]))
    return formatted_schema


os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = r"globant-376521-89c48f4df4b2.json"

if (__name__) == "__main__":
    app.run(debug=True, port=4000)
