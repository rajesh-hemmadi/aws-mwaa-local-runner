import os, re
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
import pandas as pd

DEBUG = False

REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "s3_ingestions"
S3_CONN_ID = 'paystack_s3'


def process_s3_uri(uri, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    redshift_hook = RedshiftSQLHook(postgres_conn_id='redshift_default')

    # Remove the "s3://" prefix from the URI
    uri = uri.replace('s3://', '')

    # Extract bucket and key details from the URI
    bucket, key = uri.strip().split('/', 1)
    folder_name = os.path.dirname(key)
    file_name = os.path.basename(key)
    if '.' in file_name:
        table_name = file_name.replace(' ', '_').split('.')[0]
        file_list = [key]  # Process only the specified file
    else:
        table_name = folder_name.replace(' ', '_').replace('/', '_').replace('.', '_')
        # List all files within the folder
        file_list = [file_key for file_key in s3_hook.list_keys(bucket_name=bucket, prefix=key) if not file_key.endswith('/')]

    # Append current date in yyyymmdd format to the table name
    table_name += '_' + datetime.now().strftime('%Y%m%d')

    for file in file_list:
        # Read the S3 file using pandas to determine column definitions and data types
        df = pd.read_csv(s3_hook.read_key(key=file, bucket_name=bucket))

        # Generate column definitions based on data types
        column_definitions = []
        for column, dtype in df.dtypes.iteritems():
            if dtype == 'float':
                column_definitions.append(f"{column} FLOAT")
            elif dtype == 'datetime64[ns]':
                column_definitions.append(f"{column} TIMESTAMP")
            else:
                column_definitions.append(f"{column} VARCHAR")

        # Create the table if it doesn't exist
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)});"
        redshift_hook.run(create_table_query)

        # Run COPY command to load data from S3 to Redshift
        copy_command = f"COPY {table_name} FROM 's3://{bucket}/{file}' " \
                       f"CREDENTIALS 'aws_iam_role=your-redshift-role-arn' " \
                       f"FORMAT CSV IGNOREHEADER 1;"
        redshift_hook.run(copy_command)

default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}

with DAG(dag_id="s3_adhoc_ingest_to_redshift",
         description="Ingests the data from S3 into redshift on adhoc basic, expects a list of uri's to be processed",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         schedule_interval="45 */6 * * *",
         start_date=datetime(2023, 4, 2, 0, 0, 0, 0),
         catchup=False,
         tags=["s3", "ingestions", "adhoc"]
         ) as dag:
    
    start_task = EmptyOperator(task_id='start_task')
    end_task = EmptyOperator(task_id='end_task')

    uri_list = "{{ dag_run.conf.get('s3_uris') }}".splitlines() #Pending at this stage as we need to find parameter from airflow UI
    if uri_list:
        #uris = uri_list.strip().split('\n')
        for uri in uri_list:
            task_id = re.sub(r'[^a-zA-Z0-9-_\.]', '_', uri)
            process_s3_files = PythonOperator(
                task_id=f'process_s3_uri_{task_id}',
                python_callable=process_s3_uri,
                op_kwargs={'uri': uri},
                provide_context=True
            )
            start_task >> process_s3_files >> end_task
    else:
        raise ValueError('No S3 URIs provided.')



if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
