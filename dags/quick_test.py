from typing import Any, Callable
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
from airflow.providers.sftp.hooks.sftp import SFTPHook as BaseSFTPHook
from airflow.models.param import Param

from airflow.exceptions import AirflowException
import io, stat
import os, sys, re
import logging
import fnmatch
from utils.notification import send_notification
from stat import S_ISDIR, S_ISREG

from airflow.models import XCom
#from utils.pre_processing_scripts import ghipssGenerateDateFolders, remove_preambles
from utils.pre_processing_scripts import *


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

def is_blank(value):
    return value is None or not str(value).strip()


def get_files_from_gdrive_parent_subfolders(parent_folder_id, mimetype, subfolder_level=None,file_name_pattern=None):
    drive_hook = GoogleDriveHook(gcp_conn_id=GDRIVE_CONN)
    # Build the Drive API client using the credentials from the connection
    drive_service = build('drive', 'v3', credentials=drive_hook.get_credentials())

    # Function to retrieve files within a folder and its subfolders recursively
    def retrieve_files(folder_id, current_level, parent_path="", page_token=None):
        parent_folder = drive_service.files().get(fileId=parent_folder_id, fields='name, parents').execute()
        parent_folder_name = parent_folder['name']
        files = []
        query = f"'{folder_id}' in parents and trashed = false"
        print(mimetype)

        if mimetype:
            query += f" and (mimeType = '{mimetype}' or mimeType = 'application/vnd.google-apps.folder')"

        response = drive_service.files().list(
            q=query,
            fields='nextPageToken, files(id, name, parents, mimeType)',
            pageToken=page_token
        ).execute()

        items = response.get('files', [])
        print('All Files')
        print(items)
        for item in items:
            item['parent_path'] = parent_folder_name + '/'
            files.append(item)

        if subfolder_level is not None and current_level >= subfolder_level:
            return files

        subfolders = [item for item in items if item['mimeType'] == 'application/vnd.google-apps.folder']
        print(f"Subfolders in folder '{folder_id}': {len(subfolders)}")

        for subfolder in subfolders:
            subfolder_id = subfolder['id']
            subfolder_name = subfolder['name']
            print(f"Retrieving files in subfolder '{subfolder_id}' at level {current_level + 1}")

            # Append subfolder name to the parent path
            subfolder_path = f"{parent_path}/{subfolder_name}" if parent_path else subfolder_name

            subfolder_files = retrieve_files(subfolder_id, current_level + 1, parent_path=subfolder_path)
            for item in subfolder_files:
                item['parent_path'] = parent_folder_name + '/' + subfolder_name + '/'
                files.append(item)
            # files += subfolder_files

        if 'nextPageToken' in response:
            next_page_token = response['nextPageToken']
            files += retrieve_files(folder_id, current_level, parent_path, page_token=next_page_token)

        return files

    # Retrieve files from the parent folder and its subfolders
    files = retrieve_files(parent_folder_id, 0)

    # Prepare the list of files with their IDs, names, and complete file paths
    file_list = []
    for file in files:
        if file['mimeType'] == 'application/vnd.google-apps.folder': #We do not need folders
            continue
        file_name = file['name']
        #We now check file name pattern if exists or passed
        if not is_blank(file_name_pattern):
            if not fnmatch.fnmatch(file_name.lower(), file_name_pattern.lower()):
                continue

        file_id = file['id']
        file_path = file['parents']
        print('Parent Path = ' + file['parent_path'])
        # Get the immediate parent folder ID
        parent_folder_id = file['parents'][0] if 'parents' in file else None

        # Construct the complete file path including parent folders
        # file_path = get_complete_file_path(file_id, drive_service)

        # Append the file ID, name, complete file path, and parent folder ID to the list
        file_list.append({'id': file_id, 'name': file_name, 'path': file['parent_path'], 'parent_folder_id': parent_folder_id})
    print('File List Below')
    print(file_list)
    return file_list

def download_file_from_gdrive(**kwargs):
    try:
        task_id = kwargs.get('task_id', 'Download')
        folder_id = kwargs.get('folder_id')
        file_name_with_path = kwargs.get('file_name_with_path') #it has fullpath
        local_path = kwargs.get('local_path', '/tmp')
        final_local_path = local_path + '/' + os.path.dirname(file_name_with_path)
        os.makedirs(final_local_path, exist_ok=True)
        #only_file_name = file_name_with_path.split('/')[-1]
        only_file_name = 'NIP\\NIP_695_outwards txnlog.csv'

        download_from_gdrive_to_local = GoogleDriveToLocalOperator(
            gcp_conn_id=GDRIVE_CONN,
            task_id=task_id,
            folder_id=folder_id,  # folder_id
            file_name=only_file_name,
            output_file='{}/{}'.format(final_local_path, only_file_name)
        )
        download_from_gdrive_to_local.execute(context=None)
        print(f'File {file_name_with_path} Downloaded from Gdrive')

    except Exception as err:
        print(err)
        message = "Function download_file_from_gdrive failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        # raise AirflowException(message)
        raise ValueError(message)


# def test_concept(params,**kwargs):
#     parent_folder_id = '1OCaMeQMW384LBivB0E0itW5LqnqOdVcq'
#     mimetype = 'text/csv'
#     subfolder_level = 1
#     source_file_name_pattern = '*outwards txnlog.csv'
#     items = get_files_from_gdrive_parent_subfolders(parent_folder_id,mimetype,subfolder_level,source_file_name_pattern)
#     for item in items:
#         file_name = item['path']  + item['name'] #Complete path
#         file_parent_folder_id = item['parent_folder_id']
#         task_id = "Downloading_file_" + re.sub( r'[\W_]' , '', file_name.split('/')[-1])
#         print('File Name is')
#         print(file_name)
#         download_file_from_gdrive(task_id=task_id, folder_id=file_parent_folder_id, file_name_with_path=file_name, local_path='/tmp')
        
def download_file_from_s3(**kwargs):
    try:
        task_id = kwargs.get('task_id', 'Download')
        file_name_with_path = kwargs.get('file_name_with_path') #it has fullpath
        bucket_name=kwargs.get('bucket_name', S3_BUCKET_NAME)
        local_path = kwargs.get('local_path', '/tmp')
        final_local_path = local_path + '/' + os.path.dirname(file_name_with_path)
        final_local_path_with_file_name = local_path + '/' + file_name_with_path
        pre_processing_script=kwargs.get('pre_processing_script',None)
        os.makedirs(final_local_path, exist_ok=True)
        only_file_name = file_name_with_path.split('/')[-1]
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        print('File will download')
        downloaded_file_name = s3_hook.download_file(bucket_name=bucket_name, key=file_name_with_path, local_path=final_local_path)
        print('File downloaded')
        os.rename(downloaded_file_name,final_local_path+'/'+only_file_name)
        if pre_processing_script:
            formatted_command = manage_pre_processing_script_param(
                pre_processing_script=pre_processing_script,
                final_local_path_with_file_name=final_local_path_with_file_name
            )
            exec(formatted_command)
        print(f'File {file_name_with_path} Downloaded from S3')

    except Exception as err:
        print(err)
        message = "Function download_file_from_s3 failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        # raise AirflowException(message)
        raise ValueError(message)
    
def test_concept(params,**kwargs):
    task_id='Download'
    file_name ='recon/ng/gateways/nip-reports/archive/settlement/NIP/inwards-txnlog/05-02-2021-695-547245.csv'
    #file_name='recon/ng/gateways/nip-reports/archive/settlement/NIP/outwards-txnlog/12-10-2021-695-835668.csv'
    bucket_name = S3_BUCKET_NAME
    #pre_processing_script="remove_preambles(final_local_path_with_file_name=None,num_of_columns=15,first_col_header='S/N')"
    pre_processing_script="remove_preambles(final_local_path_with_file_name=None,num_of_columns=15,first_col_header='S/N')"
    download_file_from_s3(task_id=task_id, file_name_with_path=file_name, local_path='/tmp',bucket_name=bucket_name,pre_processing_script=pre_processing_script)


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

with DAG(dag_id="quick_test",
         description="quick_test",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         schedule_interval="45 */6 * * *",
         start_date=datetime(2023, 4, 2, 0, 0, 0, 0),
         catchup=False,
         tags=["gdrive", "ingestions"],
         params=default_params
         
         ) as dag:
    START_TASK_ID = 'start_processing_files_from_gdrive'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    test_concept = PythonOperator(task_id='test_concept',
                                  python_callable=test_concept,
                                  op_kwargs={'message': 'Test Concept'},
                                  )
    dummy_start >> test_concept 


if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
