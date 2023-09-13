from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
import sys
import time
import requests
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

REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files/s3_files/gtb_statements'
GDRIVE_CONN = "gdrive_service_account"

account_numbers =[ '0604724567']

def get_last_max_date(account_number):
    redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
    sql = f"select id, file_name from s3_ingestions.load_process_reference where file_name like '%gtb_statements_{account_number}_%'  and processed = True"
    result = redshift_hook.get_pandas_df(sql)
    result['file_name_only'] = result['file_name'].apply(os.path.basename)
    # Reverse the 'file_name_only' strings and extract text from positions 4 to 12
    result['to_date'] = pd.to_datetime(result['file_name_only'].str.split('_').str[-1].str.replace('.csv',''),format='%Y%m%d')
    max_date = result['to_date'].max()
    return (max_date)

def gtb_statements_api_download_as_csv(params,**kwargs):
    try:
        override_dates = params['override_dates']
        start_date = params['start_date']
        end_date = params['end_date']
        if override_dates.lower() == 'y':
            from_date = start_date
            to_date = end_date
        else:
            from_date = datetime.strftime(datetime.now() - timedelta(1) , '%Y-%m-%d')
            to_date = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')

        local_path = kwargs.get('local_path', '/tmp')
        os.makedirs(local_path, exist_ok=True)

        print(from_date)
        print(to_date)
        for account_number in account_numbers:         
            try:
                prev_max_date = get_last_max_date(account_number)
                if pd.notna(prev_max_date) and override_dates.lower() != 'y' and (datetime.strptime(from_date,'%Y-%m-%d') - prev_max_date).days > 1:
                    from_date = datetime.strftime(prev_max_date + timedelta(1),'%Y-%m-%d')
                    print(f'From date Changed to : {from_date}' , type(from_date)) 
                
                data = pd.DataFrame()
                page_number = 1
                page_size = 5000
                max_retries = 3
                retry_delay = 5  # 5 seconds

                url = f"https://settlements-engine.private.paystack.co/providers/statement?account_number={account_number}&from_date={from_date}&to_date={to_date}&bank_code=058&page_number={page_number}&page_size={page_size}"
                print(url)
                
                for attempt in range(1, max_retries + 1):
                    try:
                        data = pd.read_json(url)

                        total_records = data['data']['Pagination']['totalCount']
                        records_data = data['data']['Transactions']['Transaction']
                        break  # If successful, exit the loop
                    except (requests.exceptions.HTTPError,KeyError) as e:
                        print(f"Attempt {attempt} failed with error: {e}")
                        if attempt < max_retries:
                            print(f"Retrying in {retry_delay} seconds...")
                            time.sleep(retry_delay)
                        else:
                            message = "Max retries reached. Exiting loop."
                            print(message)
                            raise AirflowException(message)
                
             
                total_calls_needed = int(total_records) // page_size + page_number

                while page_number < total_calls_needed:
                    print('Inside while')
                    page_number += 1
                    url = f"https://settlements-engine.private.paystack.co/providers/statement?account_number={account_number}&from_date={from_date}&to_date={to_date}&bank_code=058&page_number={page_number}&page_size={page_size}"
                    print(url)
                    for attempt in range(1, max_retries + 1):
                        try:
                            data = pd.read_json(url)
                            records_data.extend( data['data']['Transactions']['Transaction'])
                            break  # If successful, exit the loop
                        except (requests.exceptions.HTTPError,KeyError) as e:
                            print(f"Attempt {attempt} failed with error: {e}")
                            if attempt < max_retries:
                                print(f"Retrying in {retry_delay} seconds...")
                                time.sleep(retry_delay)
                            else:
                                print("Max retries reached. Exiting loop.")

                filename =  'gtb_statements_'+account_number+'_'+from_date.replace('-','')+'_'+to_date.replace('-','')+'.csv'
                print(filename)
                
                transactions = pd.json_normalize(records_data)
                transactions['rownum'] = range(1, len(transactions) + 1)
                transactions['tra_time'] = transactions['tra_date'].str[11:19]
                transactions = transactions.sort_values(['tra_date','tra_time','rownum'],ascending=[True,True,True])
                transactions['tra_date'] = transactions['tra_date'].str[6:10]+'/'+transactions['tra_date'].str[0:2]+'/'+transactions['tra_date'].str[3:5]
                transactions['val_date'] = transactions['val_date'].str[6:10]+'/'+transactions['val_date'].str[0:2]+'/'+transactions['val_date'].str[3:5]
                #transactions = transactions.sort_index(ascending=False)
                transactions.to_csv(local_path + '/' +filename,index=False)
                task_id = 'uploading_'+filename
                upload_file_to_s3(task_id=task_id, file_name_with_path=filename, local_path='/tmp',s3_bucket_name=S3_BUCKET_NAME, s3_prefix_start=S3_PREFIX_START)
            except Exception as err:
                print(err)
                message = "Could not download data from api for account " + account_number + " : \n" + \
                    str(err)
                send_notification(type="error", message=message)
                #raise AirflowException(message)
                raise ValueError(message)
    except Exception as err:
        print(err)
        message = f"Function gtb_statements_api_download_as_csv failed : \n \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)

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
    

default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}

default_params ={
         "override_dates": 'N',
         "start_date": '1901-01-01',
         "end_date": '1901-01-01',
     }

with DAG(dag_id="gtb_statements_api_download_as_csv",
         description="gtb_statements_api_download_as_csv",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         schedule_interval="0 6 * * *",
         start_date=datetime(2023, 4, 2, 8, 10),
         catchup=False,
         tags=["gtb", "statements", "download"],
         params=default_params
         
         ) as dag:
    START_TASK_ID = 'initial_empty_task'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    gtb_statements_api_download_as_csv = PythonOperator(task_id='gtb_statements_api_download_as_csv',
                                  python_callable=gtb_statements_api_download_as_csv,
                                  op_kwargs={'local_path': '/tmp'},
                                  )
    dummy_start >> gtb_statements_api_download_as_csv 


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
