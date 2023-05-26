from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
from airflow.providers.google.suite.operators.sheets import GSheetsHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator, S3CopyObjectOperator
from airflow.providers.google.cloud.transfers.gdrive_to_local import GoogleDriveToLocalOperator
from googleapiclient.discovery import build
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.exceptions import AirflowException
import io
import os
import logging
from utils.notification import send_notification


from airflow.models import XCom


from datetime import datetime, timedelta
DEBUG = False

REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files'
GDRIVE_CONN = "gdrive_service_account"


mimetype_dict = {
    'csv': 'text/csv',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'txt': 'text/plain'
    # add more file extensions and corresponding MIME types as needed
}


def test_concept(**kwargs):
    upload_file_to_s3(task_id=1, folder_id=1, file_name_with_path='Extraswitch report/202305/MX1049_Paystack_Daily_IPG_Report_2023_05_07.xlsx', local_path='/tmp',top_rows_to_skip=0)



def upload_file_to_s3(**kwargs):
    #Get file specific details
    task_id = kwargs.get('task_id', 'upload_file_to_s3')
    file_name_with_path = kwargs.get('file_name_with_path','') #it has fullpath
    local_path = kwargs.get('local_path', '/tmp')
    base_name, extension = os.path.splitext(file_name_with_path)
    new_local_path_csv = local_path + '/' + base_name + '.csv'
    s3_upload_file_name_with_path = base_name + '.csv'
    unique_load_name = kwargs.get('unique_load_name', '')
    bucket_name = kwargs.get('bucket_name', S3_BUCKET_NAME)
    s3_prefix_start = kwargs.get('s3_prefix_start', S3_PREFIX_START)
    s3_folder_name = '{}/{}'.format(s3_prefix_start, unique_load_name)
    upload_file_df = pd.read_csv(new_local_path_csv)
    target_columns_list = upload_file_df.columns.to_list()
    temp_column_list = ",".join(target_columns_list)
    print(target_columns_list)





default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}
with DAG(dag_id="quick_test",
         description="quick_test",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         schedule_interval="45 */6 * * *",
         start_date=datetime(2023, 4, 2, 0, 0, 0, 0),
         catchup=False,
         tags=["gdrive", "ingestions"]
         ) as dag:
    START_TASK_ID = 'start_processing_files_from_gdrive'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    test_concept = PythonOperator(task_id='test_concept',
                                  python_callable=test_concept,
                                  op_kwargs={'message': 'Test Concept'}

                                  )
    dummy_start >> test_concept 


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
