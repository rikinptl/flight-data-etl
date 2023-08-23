from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import json
import requests
import glob
from airflow.models import Variable, XCom
from airflow.exceptions import AirflowException
from google.cloud import storage
from google.cloud import bigquery
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType
import pendulum
from google.oauth2 import service_account
from pyspark import SparkConf, SparkContext

# Importing values of confidential API credentials, path to .json key file, GCP Bucket name and BigQuery Table Id stored in Airflow>> Admin >> Variables
api_id = Variable.get("api_id")
api_pass = Variable.get("api_pass")
key_file = Variable.get("key_file")
bucket_name = Variable.get("bucket_name")
table_id = Variable.get("table_id")

# Set the begginning and ending time intervals
begin_interval = datetime.now() - timedelta(hours=1)
end_interval = datetime.now()

# Defining a function that fetches data using an API and uploads the .jsonl file in GCP bucket in a date-specific directory structure
def save_api_response(**kwargs):

    global begin_interval

    # Convert the timestamps to unix timestamps
    begin = int(begin_interval.timestamp())
    end = int(end_interval.timestamp())

    # Set the path to the directory you want to save the files in
    directory_path = f'Data/{begin_interval.strftime("%Y")}/{begin_interval.strftime("%m")}/{begin_interval.strftime("%d")}'

    # Check if the directory exists and create it if it doesn't
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

    # Set the base URL for the API request
    base_url = f"https://{api_id}:{api_pass}@opensky-network.org/api/flights/all?begin={begin}&end={end}"

    # Set the file name to the current hour
    file_name = begin_interval.strftime("%H") + ".jsonl"

    # Set the file path
    raw_file_path = os.path.join(directory_path, file_name)

    # Pushing the file path using xcom, to let upload_to_gcs() fetch it for further use 
    kwargs['ti'].xcom_push(key='raw_file_path', value=raw_file_path)


    # Make the API request
    response = requests.get(base_url)

    # Exception Handling
    if not response.ok:
        raise AirflowException(
            f"Request failed with status code {response.status_code}"
        )

    # Convert the response text to a JSON object
    data = json.loads(response.text)
    
    def unix_to_datetime(unix_timestamp):
        return datetime.fromtimestamp(unix_timestamp)

    # Save each object to a new line in the file as JSONL
    with open(raw_file_path, "w") as f:
        for item in data:

            # parse the JSON object
            s1 = json.dumps(item)
            obj = json.loads(s1)

            # convert unix timestamp to datetime object
            dt = unix_to_datetime(obj['firstSeen'])

            # add year, month, day, and hour as new fields
            obj['year'] = dt.year
            obj['month'] = dt.month
            obj['day'] = dt.day
            obj['hour'] = dt.hour
            
            # write the updated JSON object to the new file
            f.write(json.dumps(obj) + '\n')
    
    # Uploading locally saved files to a GCP bucket

    storage_client = storage.Client.from_service_account_json(key_file)
    bucket = storage_client.bucket(bucket_name)

    with open(raw_file_path, "rb") as f:
        blob = bucket.blob(raw_file_path)
        blob.upload_from_file(f)

# Defining a function that uses pySpark to change the column names in .jsonl file, store it in a snappy.parquet format in another date-speific directory structure and later load the content of the parquet files in BigQuery external table.

def upload_to_gcs(**kwargs):

    raw_file_path = kwargs['ti'].xcom_pull(key='raw_file_path', task_ids='fetch_data')
    
    # Initiating spark-Session Configuration
    conf = SparkConf() \
    .setMaster("local[8]") \
    .setAppName("JSON_TO_PARQUET") \
    .set("spark.jars", "/home/airflow/.local/lib/python3.8/site-packages/pyspark/gcs-connector-latest-hadoop3.jar")

    sc = SparkContext(conf=conf)

    spark = (
        SparkSession.builder.config(conf=sc.getConf())
        .config(
            "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
        )
        .getOrCreate()
    )

    sc._jsc.hadoopConfiguration().set(
        "fs.gs.auth.service.account.json.keyfile",
        key_file,
    )
    sc._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.enable", "true")
    sc._jsc.hadoopConfiguration().set(
        "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    )
    sc._jsc.hadoopConfiguration().set(
        "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    )

    # Define the schema for the JSONL file
    jsonl_schema = StructType(
        [
            StructField("icao24", StringType()),
            StructField("firstSeen", LongType()),
            StructField("estDepartureAirport", StringType()),
            StructField("lastSeen", LongType()),
            StructField("estArrivalAirport", StringType()),
            StructField("callsign", StringType()),
            StructField("estDepartureAirportHorizDistance", IntegerType()),
            StructField("estDepartureAirportVertDistance", IntegerType()),
            StructField("estArrivalAirportHorizDistance", IntegerType()),
            StructField("estArrivalAirportVertDistance", IntegerType()),
            StructField("departureAirportCandidatesCount", IntegerType()),
            StructField("arrivalAirportCandidatesCount", IntegerType()),
            StructField("year", IntegerType()),
            StructField("month", IntegerType()),
            StructField("day", IntegerType()),
            StructField("hour", IntegerType())
        ]
    )

    # Read the JSONL file with the specified schema

    df = spark.read.json(f"gs://{bucket_name}/{raw_file_path}")

    new_columns = {
        "icao24": "icao",
        "firstSeen": "first_seen",
        "estDepartureAirport": "departure_airport",
        "lastSeen": "last_seen",
        "estArrivalAirport": "arrival_airport",
        "callsign": "call_sign",
        "estDepartureAirportHorizDistance": "departure_horizontal_distance",
        "estDepartureAirportVertDistance": "departure_vertical_distance",
        "estArrivalAirportHorizDistance": "arrival_horizontal_distance",
        "estArrivalAirportVertDistance": "arrival_vertical_distance",
        "departureAirportCandidatesCount": "departure_airport_candidates_count",
        "arrivalAirportCandidatesCount": "arrival_airport_candidates_count",
        "year": "year",
        "month": "month",
        "day": "day",
        "hour": "hour"
    }
    # Rename the columns
    for old_col_name, new_col_name in new_columns.items():
        df = df.withColumnRenamed(old_col_name, new_col_name)
    

    # Saving the modified jsonl file in parquet format
    df.write.format('parquet').partitionBy('year', 'month', 'day', 'hour').mode('append').save(
        f'gs://{bucket_name}/Spark-Data/'
    )
    
    # BigQuery Integration (Running a BQ query on external table automatically adds the newly added files from source bucket into the table)
     
    credentials = service_account.Credentials.from_service_account_file(key_file)
    client = bigquery.Client(credentials = credentials)

    job = client.query(f"SELECT COUNT(*) AS total_rows FROM {table_id} ")
    for row in job.result():
        print(f'The dataset now consists of {row["total_rows"]} number of records')

# Setting up default arguments for DAG  
default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2023, 3, 27, tz="Asia/Calcutta"),
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG specification
dag = DAG(
    "save_api_response_hourly_main_rkn",
    default_args=default_args,
    schedule="@hourly",
    tags=["TH"],
)

# Task Specification
fetch_json_data = PythonOperator(
    task_id="fetch_data", python_callable=save_api_response, dag=dag
)

upload_processed_data_to_gcs = PythonOperator(
    task_id="upload_to_gcs", python_callable=upload_to_gcs, dag=dag
)

fetch_json_data >> upload_processed_data_to_gcs