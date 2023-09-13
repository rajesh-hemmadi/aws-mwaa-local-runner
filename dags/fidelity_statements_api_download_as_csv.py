from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import sys
import time
import requests
import json
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
import hashlib
import logging
import fnmatch
from utils.notification import send_notification
from datetime import datetime, timedelta

import asyncio
import aiohttp
from airflow.models import XCom


from datetime import datetime, timedelta
DEBUG = True



REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files/s3_files/fidelity_statements'
GDRIVE_CONN = "gdrive_service_account"

MAX_CONCURRENT_REQUESTS = 200

def date_range(START_DATE,END_DATE):
    required_date = START_DATE
    while required_date <= END_DATE:
        yield required_date
        if DEBUG:
            return
        required_date = required_date + timedelta(days=1)


def get_last_max_date():
    redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
    sql = f"select coalesce(MAX(TO_DATE(replace(split_part(split_part(file_name,'/',4),'_',4),'.csv',''),'YYYYMMDD')),'2023-07-31'::date) as max_dt from s3_ingestions.load_process_reference  where file_name like '%fidelity_statements_%'  and processed = True"
    print(sql)
    result = redshift_hook.get_first(sql)
    if result and result[0]:
        result = result[0]
    return (result)

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

def call_api_url(start_date,end_date):
    max_retries = 10
    retry_delay = 15  # 15 seconds
    #End Point
    url = 'https://api.paygateplus.ng/v2/transact'

    #request_ref = '00000012'
    request_ref = datetime.now().strftime('%Y%m%d%H%M%S%f')
    api_key = '7c35jki7JSMuoMUUV8nj_f2a1a607878242ecb736dfd9cc4454d5'
    client_secret = 'kx6ipATNrn1tM4os'

    headers = {
    'Authorization': 'Bearer 7c35jki7JSMuoMUUV8nj_f2a1a607878242ecb736dfd9cc4454d5',
    'Content-Type': 'application/json',
    'Signature': hashlib.md5(f'{request_ref};{client_secret}'.encode()).hexdigest()
    }

    #Post Data
    data = {
    "request_ref": request_ref,
    "request_type": "get_statement",
    "auth": {
        "type": "bank.acount",
        "secure": "rQw2Ssqf5XMW+/DLx6aYfw==",
        "auth_provider": "Fidelity",
        "route_mode": None
    },
    "transaction": {
        "mock_mode": "Live",
        "transaction_ref": request_ref,
        "transaction_desc": "Get Account Statement",
        "transaction_ref_parent": None,
        "amount": 0,
        "customer": {
            "customer_ref": "9020024299",
            "firstname": None,
            "surname": None,
            "email": None,
            "mobile_no": None
        },
        "details": {
        "start_date": start_date,
        "end_date": end_date
    }
    }
}

    for attempt in range(1, max_retries + 1):
        try:
            # Send the POST request with the data
            response = requests.post(url, json=data,headers=headers)

            # Check the response status code
            if response.status_code == 200:
                records_data = response.text
                return records_data
        except (aiohttp.ClientError, KeyError) as e:
            print(f"Attempt {attempt} failed with error: {e}")
            print(f"Attempted url is : {url}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                message = "Max retries reached. Exiting loop."
                print(message)
                raise AirflowException(message)
        except Exception as err:
            print(f"Attempt {attempt} failed with error: {str(err)}")
            print(f"Attempted url is : {url}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                message = "Max retries reached. Exiting loop."
                print(message)
                raise AirflowException(message)


def call_for_single_date(start_date,end_date):
    print(f'Date Supplied is {start_date} {end_date}')
    local_path = '/tmp'
    os.makedirs(local_path, exist_ok=True)
    try:
        response = call_api_url(start_date=start_date,end_date=end_date)
        data  = pd.read_json(response)
        records_data = data['data']['provider_response']['statement_list']

        transactions = pd.json_normalize(records_data, sep='_')
        filename = f'fidelity_statements_'+start_date.replace('-','')+'_'+end_date.replace('-','')+'.csv'

        transactions.columns = [
            (''.join([i for i in c if i not in punctuation.replace('_', '')])).lower().strip().replace(' ', '_')
            for c in transactions.columns]
        transactions['unique_row_number'] = transactions.index

        transactions.to_csv(os.path.join(local_path, filename), index=False)
        task_id = 'uploading_'+filename
        upload_file_to_s3(task_id=task_id, file_name_with_path=filename, local_path='/tmp',s3_bucket_name=S3_BUCKET_NAME, s3_prefix_start=S3_PREFIX_START)
                        
        print(f"Saved file: {filename}")
    except Exception as err:        
        print(err)
        message = "Function call_for_single_date failed,  Could not download data from api for dates " + start_date + " and "+ end_date +" : \n" + \
            str(err)
        send_notification(type="error", message=message)
        #raise AirflowException(message)
        raise ValueError(message)

def fidelity_statements_api_download_as_csv(params,**kwargs):
    try:
        override_dates = params['override_dates']
        expected_run_date = (datetime.now() - timedelta(-1)).date() #This is date

        local_path = kwargs.get('local_path', '/tmp')
        os.makedirs(local_path, exist_ok=True)

        if override_dates.lower() == 'y':
            start_date = params['start_date']
            end_date = params['end_date']
        else:
            prev_max_date = get_last_max_date()
            diff_in_days = (expected_run_date - prev_max_date).days

            if pd.notna(prev_max_date) and override_dates.lower() != 'y' and diff_in_days >= 2:
                latest_start_date = prev_max_date + timedelta(days=1) 
            else:
                latest_start_date = expected_run_date 
            latest_end_date_should_be = expected_run_date
            print(f'Start date used is : {latest_start_date}')
            print(f'Final End date used is : {latest_end_date_should_be}')  
        for single_date in date_range(latest_start_date,latest_end_date_should_be):
            print(f'Single Date  : {single_date}')
            start_date = datetime.strftime(single_date, '%Y-%m-%d')
            end_date = start_date
            call_for_single_date(start_date=start_date,end_date=end_date)

    except Exception as err:
        print(err)
        message = f"Function fidelity_statements_api_download_as_csv failed : \n \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)


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

with DAG(dag_id="fidelity_statements_api_download_as_csv",
         description="fidelity_statements_api_download_as_csv",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=3),
         schedule_interval="*/10 * * * *",
         start_date=datetime(2023, 8, 12, 8, 30),
         catchup=False,
         tags=["wema", "transactions", "download"],
         params=default_params
         
         ) as dag:
    START_TASK_ID = 'initial_empty_task'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    fidelity_statements_api_download_as_csv = PythonOperator(task_id='fidelity_statements_api_download_as_csv',
                                  python_callable=fidelity_statements_api_download_as_csv,
                                  op_kwargs={'local_path': '/tmp'},
                                  )
    dummy_start >> fidelity_statements_api_download_as_csv 


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
