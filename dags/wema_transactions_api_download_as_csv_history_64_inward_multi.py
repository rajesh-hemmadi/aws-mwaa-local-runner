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


from datetime import datetime, timedelta
DEBUG = False

HISTORY_START_DATE_TIME_CSV = '20230705 12.csv'
HISTORY_END_DATE_TIME_CSV = '20230705 15.csv'
HISTORY_START_DATE_TIME = datetime.strptime(HISTORY_START_DATE_TIME_CSV.strip('.csv'), '%Y%m%d %H')
HISTORY_END_DATE_TIME = datetime.strptime(HISTORY_END_DATE_TIME_CSV.strip('.csv'), '%Y%m%d %H')


REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files/s3_files/wema_transactions'
GDRIVE_CONN = "gdrive_service_account"


partner_id ='64'
direction = 'inward'
columns_inward = ['originatoraccountnumber','amount','originatoraccountname','narration','beneficiaryaccountname','paymentreference','bankname','sourcebank','sessionid','craccount','datePosted','nibsresstatus','extResponse']
columns_outward = ['id','transactionReference','sessionID','debitAccount','creditAccount','amount','narration','finacleRespone','vendorID','transactionType','datePosted']

LOCAL_AUDIT_FILE = r'/tmp/'+partner_id+'_'+direction+'.csv'
DELETE_LOCAL_AUDIT_FILE = False

MAX_CONCURRENT_REQUESTS = 200

def date_range(HISTORY_START_DATE_TIME,HISTORY_END_DATE_TIME):
    required_date = HISTORY_START_DATE_TIME
    while required_date <= HISTORY_END_DATE_TIME:
        yield required_date
        required_date = required_date + timedelta(hours=1)

def get_last_max_date_time(partner_id,direction):
    redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
    sql = f"select COALESCE(max(split_part(split_part(file_name, '/',  regexp_count(file_name, '/')+1) ,'_',7) || ' ' || split_part(split_part(file_name, '/',  regexp_count(file_name, '/')+1) ,'_',8)),'{HISTORY_START_DATE_TIME_CSV}') max_dt_tm from s3_ingestions.load_process_reference where file_name like '%wema_transactions_{direction}_{partner_id}%' and processed = True and \
        split_part(split_part(file_name, '/',  regexp_count(file_name, '/')+1) ,'_',7) || ' ' || split_part(split_part(file_name, '/',  regexp_count(file_name, '/')+1) ,'_',8) >= '{HISTORY_START_DATE_TIME_CSV}' and \
            split_part(split_part(file_name, '/',  regexp_count(file_name, '/')+1) ,'_',7) || ' ' || split_part(split_part(file_name, '/',  regexp_count(file_name, '/')+1) ,'_',8) < '{HISTORY_END_DATE_TIME_CSV}'"
    print(sql)
    result = redshift_hook.get_first(sql)
    if result and result[0]:
        result = result[0].strip('.csv')
        result = datetime.strptime(result,'%Y%m%d %H')
        print(result)
    return (result)

def write_processed_files_date_time(partner_id,direction):
    redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
    sql = f"select split_part(split_part(file_name, '/',  regexp_count(file_name, '/')+1) ,'_',7) || '_' || split_part(split_part(split_part(file_name, '/',  regexp_count(file_name, '/')+1) ,'_',8),'.',1) processed_date_time from s3_ingestions.load_process_reference where file_name like '%wema_transactions_{direction}_{partner_id}%' and processed = True"
    result = redshift_hook.get_pandas_df(sql)
    result.to_csv(LOCAL_AUDIT_FILE,index=False)
    

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
  
def return_total_pages_records(partner_id, direction, start_date, end_date, start_time, end_time, page_number=1, page_size=100):
    max_retries = 10
    retry_delay = 15  # 5 seconds
    url = f"https://settlements-engine.private.paystack.co/providers/wema/transactions?pageNumber={page_number}&pageSize={page_size}&startDate={start_date}&endDate={end_date}&startTime={start_time}&endTime={end_time}&partnerId={partner_id}"
    print(url)
    for attempt in range(1, max_retries + 1):
        try:
            data = pd.read_json(url)
            if data['data'][direction]['responseCode'] != 200:
                message=f'There was error getting data, api url is: \n {url}'
                print(message)
                return None,None
            total_pages = data['data'][direction]['raw']['totalPages']
            total_records = data['data'][direction]['raw']['totalRecords']
            
            
            return total_pages,total_records
        except (requests.exceptions.HTTPError,KeyError) as e:
            print(f"Attempt {attempt} failed with error: {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                message = "Max retries reached. Exiting loop."
                print(message)
                return None, None
        except Exception as err:
            print(f"Attempt {attempt} failed with error: {str(err)}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                message = "Max retries reached. Exiting loop."
                print(message)
                return  None

async def call_api_url_with_semaphore(session, semaphore, partner_id, direction, start_date, end_date, start_time, end_time, page_number, page_size=100):
    async with semaphore:
        return await call_api_url(session, partner_id, direction, start_date, end_date, start_time, end_time, page_number, page_size)


async def call_api_url(session, partner_id, direction, start_date, end_date, start_time, end_time, page_number, page_size=100):
    max_retries = 10
    retry_delay = 15  # 5 seconds

    url = f"https://settlements-engine.private.paystack.co/providers/wema/transactions?pageNumber={page_number}&pageSize={page_size}&startDate={start_date}&endDate={end_date}&startTime={start_time}&endTime={end_time}&partnerId={partner_id}"
    
    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(url) as response:
                data = await response.json()
            response_code =  data['data'][direction]['responseCode']   
            if response_code != 200:
                message = f'There was an error getting data, response code is {response_code} API URL is:\n{url}'
                print(message)
                raise Exception

            
            records_data = data['data'][direction]['raw']['data']
            return records_data
        except (aiohttp.ClientError, KeyError) as e:
            print(f"Attempt {attempt} failed with error: {e}")
            print(f"Attempted url is : {url}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                message = "Max retries reached. Exiting loop."
                print(message)
                raise AirflowException(message)
        except Exception as err:
            print(f"Attempt {attempt} failed with error: {str(err)}")
            print(f"Attempted url is : {url}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
            else:
                message = "Max retries reached. Exiting loop."
                print(message)
                raise AirflowException(message)

async def async_call_for_single_date(single_date_time):
    print(f'Date Supplied is {single_date_time}')
    local_path = '/tmp'
    os.makedirs(local_path, exist_ok=True)
    start_date, start_time = datetime.strftime(single_date_time, '%Y-%m-%d'), datetime.strftime(single_date_time, '%H')
    end_date, end_time = start_date, start_time

    num_pages, total_records = return_total_pages_records(partner_id=partner_id,direction=direction,start_date=start_date,end_date=end_date,start_time=start_time,end_time=end_time)
    print(f'Total pages : {num_pages}')
    print(f'Total Records : {total_records}')
    if total_records == 0:
        transactions = pd.DataFrame(columns=columns_inward)
        transactions['unique_row_number'] = transactions.index
        transactions['partnerid'] = partner_id
        transactions['direction'] = direction
        filename = f'wema_transactions_{direction}_{partner_id}_{start_date.replace("-", "")}_{start_time}_{end_date.replace("-", "")}_{end_time}.csv'
        print(filename)
        transactions.to_csv(os.path.join(local_path, filename), index=False)
        print(f"Saved file: {filename}")
        task_id = 'uploading_'+filename
        upload_file_to_s3(task_id=task_id, file_name_with_path=filename, local_path='/tmp',s3_bucket_name=S3_BUCKET_NAME, s3_prefix_start=S3_PREFIX_START)
          
        with open(LOCAL_AUDIT_FILE,'a') as audit:
            audit.write(start_date +'_'+start_time + '\n')
        return

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:  
        tasks = [call_api_url_with_semaphore(session=session, semaphore=semaphore, partner_id=partner_id,direction=direction,start_date=start_date,end_date=end_date,start_time=start_time,end_time=end_time,page_number=page) for page in range(1, num_pages + 1)]
        results = await asyncio.gather(*tasks)

    combined_data = []
    for records_data in results:
        if records_data:
            combined_data.extend(records_data)
    if len(combined_data) == total_records:
        transactions = pd.json_normalize(combined_data, sep='_')


        transactions['unique_row_number'] = transactions.index
        transactions['partnerid'] = partner_id
        transactions['direction'] = direction

        filename = f'wema_transactions_{direction}_{partner_id}_{start_date.replace("-", "")}_{start_time}_{end_date.replace("-", "")}_{end_time}.csv'
        print(filename)
        transactions.to_csv(os.path.join(local_path, filename), index=False)
        task_id = 'uploading_'+filename
        upload_file_to_s3(task_id=task_id, file_name_with_path=filename, local_path='/tmp',s3_bucket_name=S3_BUCKET_NAME, s3_prefix_start=S3_PREFIX_START)
                        
        print(f"Saved file: {filename}")
        with open(LOCAL_AUDIT_FILE,'a') as audit:
            audit.write(start_date +'_'+start_time + '\n')

    else:
        print(f'Number of records do not match, {total_records} is not equal to {len(combined_data)}')


def wema_transactions_api_download_as_csv(params,**kwargs):
    if DELETE_LOCAL_AUDIT_FILE:
        try:
            os.remove(LOCAL_AUDIT_FILE)
        except:
            pass
    try:
        for single_date_time in date_range(HISTORY_START_DATE_TIME,HISTORY_END_DATE_TIME):
            print(f'Single Date Time : {single_date_time}')
            start_date, start_time = datetime.strftime(single_date_time, '%Y-%m-%d'), datetime.strftime(single_date_time, '%H')
            if os.path.exists(LOCAL_AUDIT_FILE):
                with open(LOCAL_AUDIT_FILE, 'r') as audit:
                    if start_date+'_'+start_time in audit.read():
                        continue
            else:
                with open(LOCAL_AUDIT_FILE, 'w'):#create file if not exists
                    pass
            asyncio.run(async_call_for_single_date(single_date_time))
    except Exception as err:
        print(err)
        message = f"Function wema_transactions_api_download_as_csv failed : \n \n" + str(err)
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
         "start_time": '00',
         "end_date": '1901-01-01',
         "end_time": '00'
     }

with DAG(dag_id="wema_transactions_api_download_as_csv_history_64_inward_multi",
         description="wema_transactions_api_download_as_csv_history_64_inward_multi",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=12),
         schedule_interval="*/5 * * * *",
         start_date=datetime(2023, 8, 12, 8, 30),
         catchup=False,
         tags=["wema", "transactions", "download"],
         params=default_params
         
         ) as dag:
    START_TASK_ID = 'initial_empty_task'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    wema_transactions_api_download_as_csv = PythonOperator(task_id='wema_transactions_api_download_as_csv',
                                  python_callable=wema_transactions_api_download_as_csv,
                                  op_kwargs={'local_path': '/tmp'},
                                  )
    dummy_start >> wema_transactions_api_download_as_csv 


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
