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


from airflow.models import XCom


from datetime import datetime, timedelta
DEBUG = False

HISTORY_START_DATE_TIME_CSV = '20230430 23.csv'
HISTORY_END_DATE_TIME_CSV = '20230601 00.csv'
HISTORY_END_DATE_TIME = datetime.strptime(HISTORY_END_DATE_TIME_CSV.strip('.csv'),'%Y%m%d %H')

REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files/s3_files/wema_transactions'
GDRIVE_CONN = "gdrive_service_account"

partner_ids =[ '64']
directions = [ 'outward']
columns_inward = ['originatoraccountnumber','amount','originatoraccountname','narration','beneficiaryaccountname','paymentreference','bankname','sourcebank','sessionid','craccount','datePosted','nibsresstatus','extResponse']
columns_outward = ['id','transactionReference','sessionID','debitAccount','creditAccount','amount','narration','finacleRespone','vendorID','transactionType','datePosted']


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
  

def call_api_url(partner_id,direction,start_date,end_date,start_time,end_time,page_number,page_size=100):
    max_retries = 3
    retry_delay = 5  # 5 seconds
    url = f"https://settlements-engine.private.paystack.co/providers/wema/transactions?pageNumber={page_number}&pageSize={page_size}&startDate={start_date}&endDate={end_date}&startTime={start_time}&endTime={end_time}&partnerId={partner_id}"
    print(url)
    for attempt in range(1, max_retries + 1):
        try:
            data = pd.read_json(url)
            if data['data'][direction]['responseCode'] != 200:
                send_notification(type='Error',message=f'There was error getting data, api url is: \n {url}')
                return None, None
            total_records = data['data'][direction]['raw']['totalRecords']
            records_data = data['data'][direction]['raw']['data']
            return total_records, records_data
        except (requests.exceptions.HTTPError,KeyError,TypeError) as e:
            print(f"Attempt {attempt} failed with error: {e}")
            if attempt < max_retries:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                message = "Max retries reached. Exiting loop."
                print(message)
                raise AirflowException(message)



def wema_transactions_api_download_as_csv(params,**kwargs):
    try:
        override_dates = params['override_dates']
        current_time_minus_required_hours = datetime.now() - timedelta(hours=1) #This is datetime

        local_path = kwargs.get('local_path', '/tmp')
        os.makedirs(local_path, exist_ok=True)

        for partner_id in partner_ids:
            for direction in directions:       
                try:
                   
                    prev_max_date_time = get_last_max_date_time(partner_id,direction)
                    latest_start_date_time = prev_max_date_time + timedelta(hours=1) 
                    latest_end_date_time_should_be = HISTORY_END_DATE_TIME
                    print(f'Start date used is : {latest_start_date_time}')
                    print(f'Final End date used is : {latest_end_date_time_should_be}')  
                    while latest_start_date_time  < latest_end_date_time_should_be:
                        start_date, start_time = datetime.strftime(latest_start_date_time,'%Y-%m-%d') , datetime.strftime(latest_start_date_time,'%H')
                        end_date, end_time = start_date, start_time
                        page_number = 1
                        page_size = 100
                        print(f'Running for Partner id {partner_id}, direction is {direction}')
                        total_records, records_data = call_api_url( partner_id=partner_id,direction=direction,start_date=start_date,end_date=end_date,start_time=start_time,end_time=end_time,page_number=page_number,page_size=page_size)
                        print(f'Total Records are {total_records}')
                        total_calls_needed = total_records // page_size + page_number
                        print(f'Total Calls Needed are {total_calls_needed}')

                        
                        while page_number < total_calls_needed:
                            print('Inside while')
                            page_number += 1
                            total_records, records_data_to_append = call_api_url( partner_id=partner_id,direction=direction,start_date=start_date,end_date=end_date,start_time=start_time,end_time=end_time,page_number=page_number,page_size=page_size)
                           
                            records_data.extend(records_data_to_append)
                            print(len(records_data))



                        transactions = pd.json_normalize(records_data, sep='_')
                        #wema_transactions_54_20230908_00_20230908_00.csv
                        if len(records_data) < 1:
                            if direction == 'inward':
                                transactions = pd.DataFrame(columns=columns_inward)
                            else:
                                transactions = pd.DataFrame(columns=columns_outward)

                        transactions['unique_row_number'] = transactions.index
                        transactions['partnerid'] = partner_id
                        transactions['direction'] = direction

                        filename =  'wema_transactions_'+direction+'_'+partner_id+'_'+start_date.replace('-','')+'_'+start_time+'_'+end_date.replace('-','')+'_'+end_time+'.csv'
                        print(filename)
                        transactions.to_csv(local_path + '/' +filename,index=False)
                        task_id = 'uploading_'+filename
                        upload_file_to_s3(task_id=task_id, file_name_with_path=filename, local_path='/tmp',s3_bucket_name=S3_BUCKET_NAME, s3_prefix_start=S3_PREFIX_START)
                        latest_start_date_time = latest_start_date_time + timedelta(hours=1) #add one hour
                except Exception as err:
                    print(err)
                    message = "Could not download data from api for partner " + partner_id + " and direction "+ direction +" : \n" + \
                        str(err)
                    send_notification(type="error", message=message)
                    #raise AirflowException(message)
                    raise ValueError(message)
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

with DAG(dag_id="wema_transactions_api_download_as_csv_history_64_outward",
         description="wema_transactions_api_download_as_csv_history_64_outward",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=3),
         schedule_interval="0 * * * *",
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
