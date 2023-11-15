from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import sys
import time
import requests
from string import punctuation
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
from airflow.models.param import Param

from airflow.exceptions import AirflowException
import io
import os
import logging
import fnmatch
from utils.notification import send_notification
from datetime import datetime, timedelta
import asyncio
import aiohttp

from airflow.models import XCom
import glob, json



from datetime import datetime, timedelta
DEBUG = False


REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files/s3_files/wema_transactions'
GDRIVE_CONN = "gdrive_service_account"

def upload_file_to_s3(**kwargs):
    try:
        #Get file specific details
        task_id = kwargs.get('task_id', 'upload_file_to_s3')
        file_name_with_path = kwargs.get('file_name_with_path') #it has fullpath
        local_path = kwargs.get('local_path', '/tmp')
        base_name, extension = os.path.splitext(file_name_with_path)
        new_local_path_csv = local_path + '/' + base_name + '.csv'
        s3_bucket_name = kwargs.get('bucket_name', S3_BUCKET_NAME)
        s3_prefix_start = kwargs.get('s3_prefix_start', S3_PREFIX_START)
        s3_folder_name = '{}/{}'.format(s3_prefix_start, file_name_with_path)
        

        s3_upload_operator = LocalFilesystemToS3Operator(
            task_id=task_id,
            aws_conn_id=S3_CONN_ID,
            filename=new_local_path_csv,
            dest_key=s3_folder_name, #+ '/' + s3_upload_file_name_with_path,
            dest_bucket=s3_bucket_name,
            replace=True
        )

        s3_upload_operator.execute(context=None)
        os.remove('{}'.format(new_local_path_csv))


    except Exception as err:
        print(err)
        message = "Function upload_file_to_s3 failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        # raise AirflowException(message)
        raise ValueError(message)
  

def adhoc_run(params,**kwargs):
    gdrive_cred = json.loads(BaseHook.get_connection(GDRIVE_CONN).extra)
    print(gdrive_cred)
    gdrive_cred = json.loads(gdrive_cred["extra__google_cloud_platform__keyfile_dict"])
    # with open('/tmp/64_inward.csv','r') as fil:
    #     print('File exists')
    #     for x in fil:
    #         print(x)
            
    # upload_file_list = glob.glob('/tmp/wema_transactions_*.csv')
    # for filename in upload_file_list:
    #     task_id = 'uploading_'+filename.split('/')[-1]
    #     print(task_id)
    #     upload_file_to_s3(task_id=task_id, file_name_with_path=filename.split('/')[-1], local_path='/tmp',s3_bucket_name=S3_BUCKET_NAME, s3_prefix_start=S3_PREFIX_START)
        

default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}

default_params ={
         "override_dates": 'N',
         "start_date": '1901-01-01',
         "end_date": '1901-01-01'
     }

with DAG(dag_id="adhoc_dag",
         description="adhoc_dag",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         schedule_interval="45 */6 * * *",
         start_date=datetime(2023, 4, 2, 0, 0, 0, 0),
         catchup=False,
         tags=["adhoc_dag", "adhoc_dag"],
         params=default_params
         
         ) as dag:
    START_TASK_ID = 'start_processing_files_from_gdrive'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    adhoc_run = PythonOperator(task_id='test_concept',
                                  python_callable=adhoc_run,
                                  op_kwargs={'message': 'Test Concept'},
                                  )
    dummy_start >> adhoc_run 


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
