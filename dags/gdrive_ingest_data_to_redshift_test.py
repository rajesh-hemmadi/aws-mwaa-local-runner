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
import re
import logging
from utils.notification import send_notification
import fnmatch


from airflow.models import XCom


from datetime import datetime, timedelta
DEBUG = True
REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "gdrive_ingestions_test"
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

def is_blank(value):
    return value is None or not str(value).strip()


def process_df_and_load_to_temp(hook,spreadsheet_id,sheet_range,metadata_columns,temp_table_name):
    try:
        metadata = hook.get_values(
                spreadsheet_id=spreadsheet_id, range_=sheet_range)
        df_metadata = pd.DataFrame(
            metadata[1:], columns=metadata[0])
        df_metadata = df_metadata[metadata_columns]
        df_metadata = df_metadata.astype(
            str)  # convert all columns types to string
        df_metadata['id'].replace('', np.nan, inplace=True)
        df_metadata.dropna(subset=['id'], inplace=True)
        print(df_metadata)
        load_data_to_redshift_temp_temple(
            df_metadata, temp_table_name)
    except Exception as err:
        print(err)
        message = f"Error Loading Metadata : {sheet_range} \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)


def get_gdrive_metadata_from_metadataSheet():
    try:
        # Specify the file path and sheet name of the Google Sheet
        spreadsheet_id = '1e6Dl6G9XqCBlL7I8ct9Cj2xSmNo-xz0MbZrUxOPC5kQ'
        target_table_metadata_sheet_name = "target_table_metadata!A:F"
        target_table_metadata_columns = ['id', 'target_db', 'target_schema', 'target_table', 'target_table_metadata_id']

        target_column_metadata_sheet_name = "target_column_metadata!A:E"
        target_column_metadata_columns = ['id', 'target_table_metadata_id', 'target_column_name', 'target_column_order']
        
        file_metadata_sheet_name = "file_metadata!A:K"
        file_metadata_columns = ['id', 'source_folder_path', 'source_file_type', 'source_file_delimiter', 'source_file_name_pattern','source_sheet_name', 'top_rows_to_skip',
                                 'pre_processing_script','target_table_metadata_id' , 'is_active', 'unique_load_name']
        
        column_mapping_metadata_sheet_name = "column_mapping_metadata!A:E"
        column_mapping_metadata_columns = ['id', 'unique_load_name', 'source_column_name', 'target_column_name']
        
    except Exception as err:
        print(err)
        message = "Error Reading GDrive Sheet : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)
    # Initialize the Google Sheets client using the gsheets_service_account connection
    hook = GSheetsHook(gcp_conn_id=GDRIVE_CONN)    
    process_df_and_load_to_temp(hook=hook,spreadsheet_id=spreadsheet_id,sheet_range=target_table_metadata_sheet_name,metadata_columns=target_table_metadata_columns,temp_table_name='metadata_target_table')
    process_df_and_load_to_temp(hook=hook,spreadsheet_id=spreadsheet_id,sheet_range=target_column_metadata_sheet_name,metadata_columns=target_column_metadata_columns,temp_table_name='metadata_target_column')
    process_df_and_load_to_temp(hook=hook,spreadsheet_id=spreadsheet_id,sheet_range=file_metadata_sheet_name,metadata_columns=file_metadata_columns,temp_table_name='metadata_file')
    process_df_and_load_to_temp(hook=hook,spreadsheet_id=spreadsheet_id,sheet_range=column_mapping_metadata_sheet_name,metadata_columns=column_mapping_metadata_columns,temp_table_name='metadata_column_mapping')

    



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

    return file_list


def load_data_to_redshift_temp_temple(data_frame, target_table):
    try:
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        engine = redshift_hook.get_sqlalchemy_engine()
        data_frame.to_sql(target_table, con=engine,
                          schema=REDSHIFT_SCHEMA, index=False, if_exists='replace')
    except Exception as err:
        print(err)
        message = "Function load_data_to_redshift_temp_temple failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)



def redshift_call_stored_procedure(**kwargs):
    try:
        schema_name = kwargs.get('schema_name')
        sp_name = kwargs.get('sp_name')
        # Even thoough it is expecting dictionery of parameters, it considers only values so be careful of order of paramateres
        params = kwargs.get('params')
        if 'params' not in kwargs:
            params = None
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        # Call stored procedure using the connection
        if params is None:
            redshift_hook.run("CALL {}.{}();".format(schema_name, sp_name))
            # redshift_hook.run(sql_query)
        else:
            # param_str = ", ".join([f"'{key}' => '{value}'" if not isinstance(value, int) else f"'{key}' => {value}" for key, value in params.items()])
            # Even thoough it is expecting dictionery of parameters, it considers only values so be careful of order of paramateres
            param_str = ", ".join([f"'{value}'" if not isinstance(
                value, int) else f"{value}" for value in params.values()])
            sql_query = "CALL {}.{}({});".format(
                schema_name, sp_name, param_str)
            print(sql_query)
            redshift_hook.run(sql_query)
    except Exception as err:
        print(err)
        message = "Function redshift_call_stored_procedure failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)

def retrun_data_from_stored_procedure_as_df(**kwargs):
    try:
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        schema_name = kwargs.get('schema_name')
        sp_name = kwargs.get('sp_name')
        # Even thoough it is expecting dictionery of parameters, it considers only values so be careful of order of paramateres
        params = kwargs.get('params')
        param_str = ", ".join([f"'{value}'" if not isinstance(
            value, int) else f"{value}" for value in params.values()])
        result_name = param_str.split(',')[-1].replace("'", "")

        # columns = ['id','source_folder_path','source_file_type','source_sheet_name','is_active','source_folder_name','target_table','target_db','target_schema','unique_load_name','file_last_extract_datetime']
        # Define the SQL queries to execute
        sql_queries = [
            "CALL {}.{}({});".format(schema_name, sp_name, param_str),
            f"FETCH ALL FROM {result_name};"
        ]

        results = []
        column_names = []
        with redshift_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for sql_query in sql_queries:
                    cursor.execute(sql_query)
                    result = cursor.fetchall()
                    if result:
                        results.append(result)
                        # Extract the column names
                        column_names.append([col_desc[0]
                                            for col_desc in cursor.description])

        # Convert the results to a pandas dataframe
        df = pd.DataFrame(results[1], columns=column_names[1])
        return df
    except Exception as err:
        print(err)
        message = "Function retrun_data_from_stored_procedure_as_df failed : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)

def return_first_cell_data_from_redshift_stored_procedure(**kwargs):
    try:
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        schema_name = kwargs.get('schema_name')
        sp_name = kwargs.get('sp_name')
        # Even thoough it is expecting dictionery of parameters, it considers only values so be careful of order of paramateres
        params = kwargs.get('params')
        param_str = ", ".join([f"'{value}'" if not isinstance(
            value, int) else f"{value}" for value in params.values()])
        result_name = param_str.split(',')[-1].replace("'", "")

        # columns = ['id','source_folder_path','source_file_type','source_sheet_name','is_active','source_folder_name','target_table','target_db','target_schema','unique_load_name','file_last_extract_datetime']
        # Define the SQL queries to execute
        sql_queries = [
            "CALL {}.{}({});".format(schema_name, sp_name, param_str),
            f"FETCH ALL FROM {result_name};"
        ]

        results = []
        with redshift_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for sql_query in sql_queries:
                    cursor.execute(sql_query)
                    result = cursor.fetchall()
                    print('result is below')
                    print(result)
                    if result:
                        results.append(result)

        data = results[1][0][0]
        return data

    except Exception as err:
        print(err)
        message = "Function return_load_reference_id failed : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)


def assign_redshift_sp_output_to_df(**kwargs):
    try:
        schema_name = kwargs.get('schema_name')
        sp_name = kwargs.get('sp_name')
        result_name = kwargs.get('result_name', 'rs_out')
        ti = kwargs['ti']

        # Define the SQL queries to execute
        sql_queries = [
            "CALL {}.{}('{}');".format(schema_name, sp_name, result_name),
            f"FETCH ALL FROM {result_name};"
        ]
        # Instantiate a RedshiftSQLHook to execute the query
        redshift_hook = RedshiftSQLHook(
            redshift_conn_id=REDSHIFT_CONN_ID, auto_commit=False)
        # Execute each SQL query separately
        results = []
        column_names = []
        with redshift_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for sql_query in sql_queries:
                    cursor.execute(sql_query)
                    result = cursor.fetchall()
                    if result:
                        results.append(result)
                        # Extract the column names
                        column_names.append([col_desc[0]
                                            for col_desc in cursor.description])

        # Convert the results to a pandas dataframe
        df = pd.DataFrame(results[1], columns=column_names[1])

        # Print the dataframe
        print(df['unique_load_name'])
       

        json_df = df.to_json(date_format='iso', orient='records')
        # print(json_df)

        ti.xcom_push(key='gdrive_load_details', value=json_df)
    except Exception as err:
        print(err)
        message = "Function assign_redshift_sp_output_to_df failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)

def update_load_process_reference_table(**kwargs):
    try:
        p_unique_load_name = kwargs.get('p_unique_load_name')
        p_file_name = kwargs.get('p_file_name')
        current_date_time = kwargs.get('current_date_time',datetime.now())
        dag_id = kwargs.get('dag_id')
        run_id = kwargs.get('run_id')
        p_airflow__external_trigger = kwargs.get('p_airflow__external_trigger',1)
        p_processed = kwargs.get('p_processed',0)
        p_process_current_status = kwargs.get('p_process_current_status')
        p_first_time_check = kwargs.get('p_first_time_check',0)

        redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_insert_update_load_process_reference',
                                                params={
                                                    'p_unique_load_name': p_unique_load_name,
                                                    'p_file_name': p_file_name,
                                                    'p_file_last_extract_datetime': current_date_time,
                                                    'p_processed': p_processed,
                                                    'p_date_process_started': current_date_time,
                                                    'p_date_process_last_updated': current_date_time,
                                                    'p_airflow__dag_id': dag_id,
                                                    'p_airflow__run_id': run_id,
                                                    'p_airflow__external_trigger': p_airflow__external_trigger,
                                                    'p_airflow__start_date': current_date_time,
                                                    'p_airflow__execution_date': current_date_time,
                                                    'p_process_current_status': p_process_current_status,
                                                    'p_first_time_check':p_first_time_check})
    except Exception as err:
        print(err)
        message = "Function update_load_process_reference_table failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)

def download_file_from_gdrive(**kwargs):
    try:
        task_id = kwargs.get('task_id', 'Download')
        folder_id = kwargs.get('folder_id')
        file_name_with_path = kwargs.get('file_name_with_path') #it has fullpath
        local_path = kwargs.get('local_path', '/tmp')
        final_local_path = local_path + '/' + os.path.dirname(file_name_with_path)
        os.makedirs(final_local_path, exist_ok=True)
        only_file_name = file_name_with_path.split('/')[-1]

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

def prepare_files_to_upload_to_s3(**kwargs):
    try:
        column_metadata_df = kwargs.get('column_metadata_df')
        task_id = kwargs.get('task_id', 'prepare_files_to_upload')
        folder_id = kwargs.get('folder_id')
        file_name_with_path = kwargs.get('file_name_with_path') #it has fullpath
        local_path = kwargs.get('local_path', '/tmp')
        file_extension = kwargs.get('file_extension','csv')
        source_file_delimiter = kwargs.get('source_file_delimiter',',')
        source_sheet_name = kwargs.get('source_sheet_name','Sheet1')
        only_file_name = file_name_with_path.split('/')[-1]
        top_rows_to_skip = kwargs.get('top_rows_to_skip')
        final_local_path = local_path + '/' + file_name_with_path #os.path.dirname(file_name_with_path)
        base_name, extension = os.path.splitext(final_local_path)
        new_local_path = base_name + '.csv_tmp'
        new_local_path_csv = base_name + '.csv'
        #read source file
        if file_extension == 'csv':
            source_df = pd.read_csv(final_local_path,skiprows=int(top_rows_to_skip),sep=source_file_delimiter,keep_default_na=False, na_values=[''],dtype=str)
        elif file_extension == 'xlsx':
            source_df = pd.read_excel(final_local_path, sheet_name=source_sheet_name, skiprows=int(top_rows_to_skip),keep_default_na=False, na_values=[''],dtype=str)
        else:
            print(f'Unknown file extension {file_extension}')
            return False
        #from source df prepare output df and file
        # Select only the desired columns from the source_df
        output_df = source_df[column_metadata_df['source_column_name']]
        #reindex for better readability in order of column
        output_df = output_df.reindex(columns=column_metadata_df['source_column_name'])
        # Rename the columns to target_column_name after converting to lower
        #output_df.columns.str.lower()
        output_df.columns = column_metadata_df['target_column_name'].tolist()
        # Add a new column called "row_number" as the first column
        output_df.insert(0, 'file_row_number', range(1, len(output_df) + 1))
        # Write the output DataFrame to a new CSV file
        output_df.to_csv(new_local_path, index=False, quoting=1)

        os.remove(final_local_path)
        os.rename(new_local_path,new_local_path_csv)

    except Exception as err:
        print(err)
        message = "Function prepare_files_to_upload_to_s3 failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        # raise AirflowException(message)
        raise ValueError(message)



def upload_file_to_s3(**kwargs):
    try:
        #Get file specific details
        task_id = kwargs.get('task_id', 'upload_file_to_s3')
        file_name_with_path = kwargs.get('file_name_with_path') #it has fullpath
        local_path = kwargs.get('local_path', '/tmp')
        base_name, extension = os.path.splitext(file_name_with_path)
        new_local_path_csv = local_path + '/' + base_name + '.csv'
        s3_upload_file_name_with_path = base_name + '.csv'
        unique_load_name = kwargs.get('unique_load_name', '')
        s3_bucket_name = kwargs.get('bucket_name', S3_BUCKET_NAME)
        s3_prefix_start = kwargs.get('s3_prefix_start', S3_PREFIX_START)
        s3_folder_name = '{}/{}'.format(s3_prefix_start, unique_load_name)
        

        s3_upload_operator = LocalFilesystemToS3Operator(
            task_id=task_id,
            aws_conn_id=S3_CONN_ID,
            filename=new_local_path_csv,
            dest_key=s3_folder_name + '/' + s3_upload_file_name_with_path,
            dest_bucket=s3_bucket_name,
            replace=True
        )

        s3_upload_operator.execute(context=None)
        #os.remove('{}/{}'.format(final_local_path, download_file_name))


    except Exception as err:
        print(err)
        message = "Function upload_file_to_s3 failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        # raise AirflowException(message)
        raise ValueError(message)

def load_csv_file_to_redshift(**kwargs):
    try:
        #Get file specific details
        task_id = kwargs.get('task_id', 'load_csv_file_to_redshift')
        file_name_with_path = kwargs.get('file_name_with_path') #it has fullpath
        local_path = kwargs.get('local_path', '/tmp')
        base_name, extension = os.path.splitext(file_name_with_path)
        new_local_path_csv = local_path + '/' + base_name + '.csv'
        s3_upload_file_name_with_path = base_name + '.csv'
        unique_load_name = kwargs.get('unique_load_name', '')
        bucket_name = kwargs.get('bucket_name', S3_BUCKET_NAME)
        s3_prefix_start = kwargs.get('s3_prefix_start', S3_PREFIX_START)
        s3_folder_name = '{}/{}'.format(s3_prefix_start, unique_load_name)
        s3_final_path = s3_folder_name + '/' + s3_upload_file_name_with_path

        redshift_schema = unique_load_name.split('.')[1]
        target_table = unique_load_name.split('.')[2].split('__')[0]
        target_table_temp = target_table + '_temp'
       
        #Create temp table before deleting the file after the s3 upload
        upload_file_df = pd.read_csv(new_local_path_csv, nrows=1)
        target_columns_list = upload_file_df.columns.to_list()
        temp_column_list = ",".join(['"' + column.lower() + '"' for column in target_columns_list])
        redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_create_temp_table_with_columnslist', params={
                                        'p_unique_load_name': unique_load_name,'columns_list' : temp_column_list})
        
        os.remove(new_local_path_csv)

        load_data_task = S3ToRedshiftOperator(
        task_id=task_id,
        s3_bucket=bucket_name,
        s3_key=s3_final_path,
        schema=redshift_schema,
        table=target_table_temp,
        copy_options=["CSV", "DELIMITER ','", "IGNOREHEADER 1", "IGNOREBLANKLINES"],
        aws_conn_id=S3_CONN_ID,
        redshift_conn_id=REDSHIFT_CONN_ID
        )

        s3_delete_object_operator = S3DeleteObjectsOperator(
        task_id='delete_file_from_S3',
        bucket=bucket_name,
        keys=s3_final_path,
        aws_conn_id=S3_CONN_ID,
        )
        load_data_task.execute(context=None)
        load_process_reference_id = return_first_cell_data_from_redshift_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_get_load_reference_id', params={
                                                                   'p_unique_load_name': unique_load_name, 'p_file_name': file_name_with_path, 'rs_out': 'rs_out'})
        redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_read_temp_load_target', params={
                                        'p_unique_load_name': unique_load_name,'p_columns_list' : temp_column_list, 'p_load_process_reference_id' :load_process_reference_id  })
        
        s3_delete_object_operator.execute(context=None)
        

    except Exception as err:
        print(err)
        message = "Function load_csv_file_to_redshift failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        # raise AirflowException(message)
        raise ValueError(message)





def list_files_to_transfer(**kwargs):
    try:
        dag_id = kwargs['dag'].dag_id
        run_id = kwargs['run_id']
        ti = kwargs['ti']
        json_df = ti.xcom_pull(key='gdrive_load_details')
        df = pd.read_json(json_df)
      
        file_metadata_df = df[['source_folder_path', 'source_file_type', 'source_file_delimiter', 'source_file_name_pattern','source_sheet_name', 'top_rows_to_skip', 'pre_processing_script', 'target_table_metadata_id', 'is_active', 'unique_load_name']].drop_duplicates()
        for index, row in file_metadata_df.iterrows():
            unique_load_name = row['unique_load_name']
            column_metadata_df = df[df['unique_load_name'] == unique_load_name][['unique_load_name', 'source_column_name','target_column_name']]
        
            parent_folder_id = row['source_folder_path'].split('/')[-1]
            source_file_name_pattern = row['source_file_name_pattern']
            file_extension = row['source_file_type']
            source_sheet_name = row['source_sheet_name']
            print(f'Source Sheet Name is : {source_sheet_name}')
            if source_sheet_name == '' or source_sheet_name is None:
                source_sheet_name = 0
            top_rows_to_skip = row['top_rows_to_skip']
            if top_rows_to_skip is None or top_rows_to_skip == 0 or top_rows_to_skip == '':
                top_rows_to_skip = 0
            else:
                top_rows_to_skip = top_rows_to_skip 
            print('Top rows to Skip : ' , top_rows_to_skip)
            source_file_delimiter = row['source_file_delimiter']
            
                
            mimetype = mimetype_dict.get(file_extension)
            if mimetype is None:
                message = f"No MimeType found for File Extension '{file_extension}'"
                logging.info(message)
                # May be send an email
                # continue to next load
                send_notification(type="info", message=message)
                continue

            subfolder_level = 1 #For future purpose, now it is hard coded to go only one level deep
            items = get_files_from_gdrive_parent_subfolders(parent_folder_id,mimetype,subfolder_level,source_file_name_pattern)

            #At this stage read all processed files data from redshift so we don't have to run v_is_file_processed task one by one
            processed_files_list = retrun_data_from_stored_procedure_as_df(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_get_list_of_processed_files_for_unique_load_name', params={
                                                                   'p_unique_load_name': row['unique_load_name'], 'rs_out': 'rs_out'})

            # Filter items
            filtered_items = [item for item in items if f"{item['path']}{item['name']}" not in processed_files_list['file_name'].tolist()]

            print('Filtered Items are as below')
            print(filtered_items)

            items = filtered_items
            # Iterate through the list of files and print their names and modified times
            if DEBUG:  # When set debug deal with only one file rather than all files
                items = items[:1]

            for item in items:
                current_date_time = datetime.now()
                file_name = item['path']  + item['name'] #Complete path
                file_parent_folder_id = item['parent_folder_id']
                task_id = "Downloading_file_" + re.sub( r'[\W_]' , '', file_name.split('/')[-1])
                # print(f"{item['name']} modified at {item['modifiedTime']}")
                #if we have already processed the same file based on unique_load_name and file_name and processed = True then we skip and move to next file
                #v_is_file_processed = return_first_cell_data_from_redshift_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_check_if_file_processed', params={
                #                                                   'p_unique_load_name': row['unique_load_name'], 'p_file_name': file_name, 'rs_out': 'rs_out'})
                #if file is already processed continue to next file
                #if v_is_file_processed:
                #    continue

                update_load_process_reference_table(p_unique_load_name = unique_load_name,p_file_name= file_name, p_airflow__dag_id= dag_id, p_airflow__run_id= run_id, p_first_time_check=1, p_process_current_status='Load Process is in Progress' )
                try:
                    download_file_from_gdrive(task_id=task_id, folder_id=file_parent_folder_id, file_name_with_path=file_name, local_path='/tmp')
                    prepare_files_to_upload_to_s3(task_id=task_id, folder_id=file_parent_folder_id, file_name_with_path=file_name, local_path='/tmp',file_extension=file_extension,source_file_delimiter=source_file_delimiter, top_rows_to_skip=top_rows_to_skip,source_sheet_name=source_sheet_name,column_metadata_df=column_metadata_df)
                    upload_file_to_s3(task_id=task_id, file_name_with_path=file_name, local_path='/tmp',unique_load_name=unique_load_name,s3_bucket_name=S3_BUCKET_NAME, s3_prefix_start=S3_PREFIX_START)
                    load_csv_file_to_redshift(task_id=task_id,file_name_with_path=file_name,local_path='/tmp',unique_load_name=unique_load_name,s3_bucket_name=S3_BUCKET_NAME, s3_prefix_start=S3_PREFIX_START)
                    update_load_process_reference_table(p_unique_load_name = unique_load_name,p_file_name= file_name, p_airflow__dag_id= dag_id, p_airflow__run_id= run_id, p_processed = 1, p_process_current_status='Load Process Completed Successfully' )
                except:
                    update_load_process_reference_table(p_unique_load_name = unique_load_name,p_file_name= file_name, p_airflow__dag_id= dag_id, p_airflow__run_id= run_id, p_process_current_status='Load Process has Failed, Please check Airflow log for more details' )
                    continue #Move to next file
                
    except Exception as err:
        print(err)
        message = "Function list_files_to_transfer failed : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)





def test_concept(**kwargs):
    print(kwargs.get('sp_name'))


default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}
with DAG(dag_id="gdrive_ingest_data_to_redshift_test",
         description="Ingests the data from GDrive into redshift",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=1),
         schedule_interval="*/15 * * * *",
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
    get_gdrive_metadata_from_metadataSheet = PythonOperator(task_id='get_gdrive_metadata_from_metadataSheet',
                                                            python_callable=get_gdrive_metadata_from_metadataSheet
                                                            )

    run_sp_to_create_target_tables_based_on_metadata_table = PythonOperator(task_id='run_sp_to_create_target_tables_based_on_metadata_table',
                                                                            python_callable=redshift_call_stored_procedure,
                                                                            op_kwargs={
                                                                                'schema_name': REDSHIFT_SCHEMA, 'sp_name': 'gdrive_process_create_target_table_based_on_metadata'},
                                                                            )
    run_sp_to_update_target_tables_based_on_metadata_table = PythonOperator(task_id='run_sp_to_update_target_tables_based_on_metadata_table',
                                                                            python_callable=redshift_call_stored_procedure,
                                                                            op_kwargs={
                                                                                'schema_name': REDSHIFT_SCHEMA, 'sp_name': 'gdrive_process_update_target_table_based_on_metadata'},
                                                                            )
    get_unique_load_details_from_metadata_table = PythonOperator(task_id='get_unique_load_details_from_metadata_table',
                                                                 python_callable=assign_redshift_sp_output_to_df,
                                                                 op_kwargs={
                                                                     'schema_name': REDSHIFT_SCHEMA, 'sp_name': 'gdrive_process_read_file_metadata_and_column_mapping_metadata', 'result_name': 'rs_out'}

                                                                 )
    list_and_transfer_files_from_drive_to_s3 = PythonOperator(task_id='list_and_transfer_files_from_drive_to_s3',
                                                              python_callable=list_files_to_transfer,
                                                              op_kwargs={
                                                                  'run_id': '{{ run_id }}'},
                                                              provide_context=True
                                                              )


    dummy_start >> test_concept >> get_gdrive_metadata_from_metadataSheet   \
        >> run_sp_to_create_target_tables_based_on_metadata_table >> run_sp_to_update_target_tables_based_on_metadata_table >> get_unique_load_details_from_metadata_table \
        >> list_and_transfer_files_from_drive_to_s3
    # >> load_files_from_S3_to_RedShift

if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
