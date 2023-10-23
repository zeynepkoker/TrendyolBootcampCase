import psycopg2
from datetime import datetime
import csv
from google.cloud import bigquery
from google.oauth2 import service_account
import os
from airflow import DAG
from airflow.operators.python import PythonOperator


def dataExtraction():
    # Database connection parameters
    db_params = {
        'dbname': 'bootcamp',
        'user': 'bootcamp',
        'password': 'boot2023!',
        'host': '34.141.52.186',
        'port': '5432'
    }

    # Connect to the database
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Extract data
    query = "SELECT * FROM bootcamp.bootcamp.product_content"
    cursor.execute(query)
    data = cursor.fetchall()

    # Write data to a CSV file
    filename = f"product_content.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerows(data)

    # Close the database connection
    cursor.close()
    conn.close()



def csvToBigquery():
    key_path = os.path.expanduser("~/airflow/dags/dsmbootcamp-535dfd073f87.json")

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)


    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("product_content_id", "INTEGER", mode="REQUIRED", description="Ürün id'si", ),
            bigquery.SchemaField("product_content_name", "STRING", mode="NULLABLE", description="Ürün adı"),
            bigquery.SchemaField("current_category_id", "INTEGER", description="Kategori id'si"),
            bigquery.SchemaField("current_category_name", "STRING", mode="NULLABLE", description="Kategori adı"),
            bigquery.SchemaField("current_business_unit_id", "INTEGER", description="Business id'si"),
            bigquery.SchemaField("current_business_unit_name", "STRING", mode="NULLABLE", description="Business adı"),
            bigquery.SchemaField("color", "STRING", mode="NULLABLE", description="Tedarikçi rengi"),
            bigquery.SchemaField("mnc_first_price", "FLOAT", description="Fiyat bilgisi"),
        ],
        skip_leading_rows=0,
        # The source format defaults to CSV, so the line below is optional.
        source_format=bigquery.SourceFormat.CSV,
        max_bad_records=10
    )


    table_ref = 'zeynep_koker.product_content'


    with open('product_content.csv', 'rb') as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete



def transformation():

    key_path = os.path.expanduser("~/airflow/dags/dsmbootcamp-535dfd073f87.json")

    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)


    # TODO(developer): Set table_id to the ID of the table to create.

    table_id = "dsmbootcamp.zeynep_koker.product_content"

    table_ref = "zeynep_koker.product_content"


    currentDateAndTime = datetime.now()
    formattedTime = currentDateAndTime.strftime('%Y-%m-%d %H:%M:%S.%f')

    queryAddColumn1 = """
        ALTER TABLE `dsmbootcamp.zeynep_koker.product_content`
        ADD COLUMN IF NOT EXISTS etl_date_create TIMESTAMP;
        ALTER TABLE `dsmbootcamp.zeynep_koker.product_content`
        ALTER COLUMN etl_date_create 
        SET OPTIONS ( description="Satırın dimesion'a geldiği tarih");
        ALTER TABLE `dsmbootcamp.zeynep_koker.product_content`
        ADD COLUMN IF NOT EXISTS etl_date_update TIMESTAMP;
        ALTER TABLE `dsmbootcamp.zeynep_koker.product_content`
        ALTER COLUMN etl_date_update
        SET OPTIONS ( description="Satırın dimesion'da update gördüğü tarih");
    """

    queryAddColumn2 = """
        ALTER TABLE `dsmbootcamp.zeynep_koker.product_content`
        ADD COLUMN IF NOT EXISTS is_deleted_in_source BOOLEAN;
        ALTER TABLE `dsmbootcamp.zeynep_koker.product_content`
        ALTER COLUMN is_deleted_in_source
        SET OPTIONS ( description="Silinen kayıtların işaretleneceği alan");       
        ALTER TABLE `dsmbootcamp.zeynep_koker.product_content`
        ADD COLUMN IF NOT EXISTS product_content_sk INTEGER;
        UPDATE `dsmbootcamp.zeynep_koker.product_content`
        SET product_content_sk = product_content_id WHERE TRUE;
    """

    queryDeletedControl = """
        UPDATE `dsmbootcamp.zeynep_koker.product_content`
        SET is_deleted_in_source = TRUE
        WHERE product_content_id IN (
        SELECT product_content_id
        FROM `dsmbootcamp.zeynep_koker.product_content`
        GROUP BY product_content_id
        HAVING COUNT(product_content_id) = 1
        ) AND etl_date_create is not null;
    """

    queryRemoveDuplicates = """
        CREATE OR REPLACE TABLE `dsmbootcamp.zeynep_koker.product_content` AS ( SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_content_id,product_content_name,current_category_id,current_category_name,current_business_unit_id,current_business_unit_name,color,CAST(mnc_first_price as STRING),product_content_sk ORDER BY product_content_id,product_content_name,current_category_id,current_category_name,current_business_unit_id,current_business_unit_name,color,mnc_first_price,product_content_sk) AS rn
        FROM `dsmbootcamp.zeynep_koker.product_content`
        ) A
        WHERE rn != 1);
        ALTER TABLE `dsmbootcamp.zeynep_koker.product_content`
        DROP COLUMN IF EXISTS rn;
    """

    queryAddCreateDate = """
        UPDATE `dsmbootcamp.zeynep_koker.product_content`
        SET etl_date_create = '""" + formattedTime + """'
        WHERE etl_date_create is null AND product_content_id IN (
        SELECT product_content_id
        FROM `dsmbootcamp.zeynep_koker.product_content`
        GROUP BY product_content_id
        HAVING COUNT(product_content_id) = 1
        );
    """

    queryAddUpdateDate = """
        UPDATE `dsmbootcamp.zeynep_koker.product_content` t1
        SET t1.etl_date_update = '""" + formattedTime + """', t1.etl_date_create = t2.etl_date_create
        FROM `dsmbootcamp.zeynep_koker.product_content` t2
        WHERE t1.product_content_id = t2.product_content_id AND t2.etl_date_create is NOT NULL AND t1.product_content_id IN (
        SELECT product_content_id
        FROM `dsmbootcamp.zeynep_koker.product_content`
        GROUP BY product_content_id
        HAVING COUNT(product_content_id) > 1
        );
        DELETE from `dsmbootcamp.zeynep_koker.product_content`
        WHERE is_deleted_in_source is not null AND product_content_id IN (
        SELECT product_content_id
        FROM `dsmbootcamp.zeynep_koker.product_content`
        GROUP BY product_content_id
        HAVING COUNT(product_content_id) > 1
        );
        UPDATE `dsmbootcamp.zeynep_koker.product_content`
        SET is_deleted_in_source = FALSE
        WHERE is_deleted_in_source is null;
    """

    # Run the query
    query_job = client.query(queryAddColumn1)
    # Wait for the query to complete
    query_job.result()

    # Run the query
    query_job = client.query(queryAddColumn2)
    # Wait for the query to complete
    query_job.result()

    # Run the query
    query_job = client.query(queryDeletedControl)
    # Wait for the query to complete
    query_job.result()

    # Run the query
    query_job = client.query(queryRemoveDuplicates)
    # Wait for the query to complete
    query_job.result()

    # Run the query
    query_job = client.query(queryAddCreateDate)
    # Wait for the query to complete
    query_job.result()

    # Run the query
    query_job = client.query(queryAddUpdateDate)
    # Wait for the query to complete
    query_job.result()



with DAG(
        dag_id="Trendyol_Bootcamp",
        start_date=datetime(2023, 10, 22),
        schedule_interval="@hourly",
        catchup=False
) as dag:
    task1 = PythonOperator(
        task_id="dataExtraction",
        python_callable = dataExtraction
    )
    task2 = PythonOperator(
        task_id = "csvToBigquery",
        python_callable = csvToBigquery
    )
    task3 = PythonOperator(
        task_id = "transformation",
        python_callable = transformation
    )

task1 >> task2 >> task3