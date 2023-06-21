from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
from airflow.hooks.base import BaseHook
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
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

from airflow.exceptions import AirflowException
import io
import os
import logging
import fnmatch
from utils.notification import send_notification


from airflow.models import XCom


from datetime import datetime, timedelta
DEBUG = False

REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files'
GDRIVE_CONN = "gdrive_service_account"
SFTP_ACCOUNT = 'sftp_absa_gh'

mimetype_dict = {
    'csv': 'text/csv',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'txt': 'text/plain'
    # add more file extensions and corresponding MIME types as needed
}

def test_concept(**kwargs):
    files = list_files_with_pattern(SFTP_ACCOUNT, '/', 'PAYSTACK_*')
    print(files)
    for fil in files:
        print(fil)
    
    # get_file = SFTPOperator(
    #             task_id="test_sftp",
    #             ssh_conn_id=SFTP_ACCOUNT,
    #             local_filepath="/tmp/PAYSTACK_REPORT_2023-05-19.csv",
    #             remote_filepath="/PAYSTACK_REPORT_2023-05-19.csv",
    #             operation="get",
    #             create_intermediate_dirs=False,
               
    #         )
    # get_file.execute(context=None)

    # get_files_from_s3_bucket(bucket_name=S3_BUCKET_NAME,prefix='gdrive_files/s3_files',depth=1,file_extension='csv')

def list_files_with_pattern(credentials, remote_path, pattern):
    sftp_hook = SFTPHook(ftp_conn_id=credentials)
    files = sftp_hook.list_directory(remote_path)
    for fil in files:
        if fil.endswith('csv'):
            pass
        else:
            print(fil)
    matched_files = [file for file in files if fnmatch.fnmatch(file, pattern)]
    #for fil in matched_files:
    #    print(fil)

    return matched_files
    
def get_files_from_s3_bucket(bucket_name, prefix=None, depth=0, file_extension=None,
                               file_name_pattern=None):
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

    # Retrieve objects from the S3 bucket
    keys = s3_hook.list_keys(bucket_name, prefix=prefix)
    s3_client = s3_hook.get_conn()
    prefix_slash_count = prefix.rstrip('/').count('/') + 1
    # Prepare the list of files with their names and complete file paths
    file_list = []
    for key in keys:
        if key.endswith('/'):
            # Skip folders
            continue

        if file_extension and not key.endswith(file_extension):
            # Skip files that do not match the file extension
            continue

        # Extract the file name from the key
        file_name = key.split('/')[-1]

        if file_name_pattern and not fnmatch.fnmatch(file_name.lower(), file_name_pattern.lower()):
            # Skip files that do not match the file name pattern
            continue
        
        if key.count('/') > (prefix_slash_count + depth):
            # Skip files that are more deeper than specified depth
            continue

        # Append the file name and complete file path to the list
        file_list.append({'name': file_name, 'path': key})


    
    print (file_list)
    return file_list





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
