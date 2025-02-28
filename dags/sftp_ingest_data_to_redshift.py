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
from airflow.providers.sftp.hooks.sftp import SFTPHook as BaseSFTPHook
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.exceptions import AirflowException
import io
import stat
import os
import re
import logging
from utils.notification import send_notification
import fnmatch
from typing import Any, Callable

from airflow.models import XCom
from utils.pre_processing_scripts import *


from datetime import datetime, timedelta
DEBUG = True
REDSHIFT_CONN_ID = "paystack_redshift"
REDSHIFT_DATABASE = "paystackharmonyredshift"
REDSHIFT_SCHEMA = "sftp_ingestions"
S3_CONN_ID = 'paystack_s3'
S3_BUCKET_NAME = 'paystack-datalake'
S3_PREFIX_START = 'gdrive_files/s3_files_upload'
GDRIVE_CONN = "gdrive_service_account"


mimetype_dict = {
    'csv': 'text/csv',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'txt': 'text/plain'
    # add more file extensions and corresponding MIME types as needed
}



class SFTPHookCustom(BaseSFTPHook):
    def walktree(
        self,
        path: str,
        fcallback: Callable[[str], Any | None],
        dcallback: Callable[[str], Any | None],
        ucallback: Callable[[str], Any | None],
        recurse: bool = True,
    ) -> None:
        #print('Inside custom walktree')
        """Recursively descend, depth first, the directory tree at ``path``.

        This calls discrete callback functions for each regular file, directory,
        and unknown file type.

        :param str path:
            root of remote directory to descend, use '.' to start at
            :attr:`.pwd`
        :param callable fcallback:
            callback function to invoke for a regular file.
            (form: ``func(str)``)
        :param callable dcallback:
            callback function to invoke for a directory. (form: ``func(str)``)
        :param callable ucallback:
            callback function to invoke for an unknown file type.
            (form: ``func(str)``)
        :param bool recurse: *Default: True* - should it recurse
        """
        conn = self.get_conn()
        for entry in self.list_directory(path):
            pathname = os.path.join(path, entry)
            try:
                mode = conn.stat(pathname).st_mode
            except:
                print(f'Error related to path {pathname}, Continuing to next dir')
                continue
            if stat.S_ISDIR(mode):  # type: ignore
                # It's a directory, call the dcallback function
                dcallback(pathname)
                if recurse:
                    # now, recurse into it
                    self.walktree(pathname, fcallback, dcallback, ucallback)
            elif stat.S_ISREG(mode):  # type: ignore
                # It's a file, call the fcallback function
                fcallback(pathname)
            else:
                # Unknown file type
                ucallback(pathname)

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


def get_sftp_metadata_from_metadataSheet():
    try:
        # Specify the file path and sheet name of the Google Sheet
        spreadsheet_id = '18Jc0HD_OzzebBaFUUd9QVLOGeBaxuHFtblFvL71jtRs'
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

    



def get_file_names_from_sftp_server(connection, remote_path=None, depth=0, file_extension=None,
                               file_name_pattern=None):
    #Depth will not have impact here
    remote_path = remote_path if remote_path.endswith('/') else remote_path + '/'
    try:
        sftp_hook = SFTPHookCustom(ftp_conn_id=connection)

        # Retrieve objects from the SFTP server 
        if connection == 'sftp_ghipss_reports':
            files = []
            dir_list = ghipssGenerateDateFolders()
            for dir in dir_list:
                path = remote_path + dir + '/'
                try:
                    fil_list = sftp_hook.list_directory(path)
                except Exception as err:
                    print(f'There was an error listing directory : {path} \n {err}')
                    continue
                fil_list_with_dir_path = [path + item for item in fil_list]
                files.extend(fil_list_with_dir_path)
        elif remote_path == '/': #Here when you do not need recurse 
            files = sftp_hook.list_directory(remote_path)
        else:
            files = sftp_hook.get_tree_map(path=remote_path) #Tree map brings 3 list of tuples
            files = files[0]
        print(files)
        file_list = []
        for fil in files:
            if file_extension and not fil.endswith(file_extension):
                # Skip files that do not match the file extension
                continue
            
            file_name_only = fil.split('/')[-1]
            if file_name_pattern and not fnmatch.fnmatch(file_name_only.lower(), file_name_pattern.lower()):
                # Skip files that do not match the file name pattern
                continue  
            # Append the file name and complete file path to the list
            file_list.append({'name': file_name_only, 'path': connection + (':' if fil.startswith('/') else ':/')  + fil})
        
        print (file_list)
        return file_list
    except Exception as err:
        print(err)
        message = "Function get_file_names_from_sftp_server failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        #raise AirflowException(message)
        raise ValueError(message)


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

        ti.xcom_push(key='sftp_load_details', value=json_df)
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

        redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='sftp_process_insert_update_load_process_reference',
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

def download_file_from_sftp(**kwargs):
    try:
        task_id = kwargs.get('task_id', 'Download')
        file_name_with_path = kwargs.get('file_name_with_path') #it has fullpath
        local_path = kwargs.get('local_path', '/tmp')
        pre_processing_script=kwargs.get('pre_processing_script',None)
        final_local_path = local_path + '/' + os.path.dirname(file_name_with_path)
        final_local_path_with_file_name = local_path + '/' + file_name_with_path
        os.makedirs(final_local_path, exist_ok=True)
        only_file_name = file_name_with_path.split('/')[-1]
        ssh_conn_id , remote_filepath= file_name_with_path.split(':',1)
       
        get_file = SFTPOperator(
                task_id=task_id,
                ssh_conn_id=ssh_conn_id,
                local_filepath=final_local_path+ '/' +only_file_name,
                remote_filepath=remote_filepath,
                operation="get",
                create_intermediate_dirs=False,
               
            )
        get_file.execute(context=None)
        
        if pre_processing_script:
            formatted_command = manage_pre_processing_script_param(
                pre_processing_script=pre_processing_script,
                final_local_path_with_file_name=final_local_path_with_file_name
            )
            exec(formatted_command)
        print(f'File {file_name_with_path} Downloaded from SFTP')
        #This is only for zenith_gh statements files
        # if ssh_conn_id == 'sftp_zenith_gh' and final_local_path.lower().split('/')[-1] == 'statement':
        #     combine_excel_sheets_using_pandas(final_local_path+ '/' +only_file_name)

    except Exception as err:
        print(err)
        message = "Function download_file_from_sftp failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        # raise AirflowException(message)
        raise ValueError(message)

def prepare_files_to_upload_to_s3(**kwargs):
    try:
        column_metadata_df = kwargs.get('column_metadata_df')
        task_id = kwargs.get('task_id', 'prepare_files_to_upload')
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
        #filter is used so if there are 
        output_df = source_df.filter(items=column_metadata_df['source_column_name'])
        # Add missing columns with null values
        missing_columns = set(column_metadata_df['source_column_name']) - set(output_df.columns)
        if missing_columns:
            message = "Missing Columns Error, following are the missing columns : \n " +  ','.join(missing_columns)
            send_notification(type="info", message=message)
            output_df = output_df.reindex(columns=output_df.columns.union(missing_columns), fill_value=None)

        #reindex for better readability in order of column
        output_df = output_df.reindex(columns=column_metadata_df['source_column_name'])
        # Rename the columns to target_column_name
        output_df.columns = column_metadata_df['target_column_name'].tolist()
        # Add a new column called "file_row_number" as the first column
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
        s3_folder_name = '{}/{}'.format(s3_prefix_start, file_name_with_path)
        s3_final_path = s3_folder_name#s3_folder_name + '/' + s3_upload_file_name_with_path

        redshift_schema = unique_load_name.split('.')[1]
        target_table = unique_load_name.split('.')[2].split('__')[0]
        target_table_temp = target_table + '_temp'
       
        #Create temp table before deleting the file after the s3 upload
        upload_file_df = pd.read_csv(new_local_path_csv, nrows=1)
        target_columns_list = upload_file_df.columns.to_list()
        temp_column_list = ",".join(['"' + column.lower() + '"' for column in target_columns_list])
        redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='sftp_process_create_temp_table_with_columnslist', params={
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
        load_process_reference_id = return_first_cell_data_from_redshift_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='sftp_process_get_load_reference_id', params={
                                                                   'p_unique_load_name': unique_load_name, 'p_file_name': file_name_with_path, 'rs_out': 'rs_out'})
        redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='sftp_process_read_temp_load_target', params={
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
        json_df = ti.xcom_pull(key='sftp_load_details')
        df = pd.read_json(json_df)
      
        file_metadata_df = df[['source_folder_path', 'source_file_type', 'source_file_delimiter', 'source_file_name_pattern','source_sheet_name', 'top_rows_to_skip', 'pre_processing_script', 'target_table_metadata_id', 'is_active', 'unique_load_name']].drop_duplicates()
        for index, row in file_metadata_df.iterrows():
            unique_load_name = row['unique_load_name']
            column_metadata_df = df[df['unique_load_name'] == unique_load_name][['unique_load_name', 'source_column_name','target_column_name']]
            source_folder_path = row['source_folder_path']
            parent_folder_id = row['source_folder_path'].split('/')[-1]
            source_file_name_pattern = row['source_file_name_pattern']
            file_extension = row['source_file_type']
            source_sheet_name = row['source_sheet_name']
            pre_processing_script = row['pre_processing_script']
            print(f'Unique Load Name is : {unique_load_name}')
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
            # above has no impact for sftp

            connection, remote_path = source_folder_path.split(':',1)


            items = get_file_names_from_sftp_server(connection=connection,remote_path=remote_path,depth=subfolder_level,file_extension=file_extension,file_name_pattern=source_file_name_pattern)

            #At this stage read all processed files data from redshift so we don't have to run v_is_file_processed task one by one
            processed_files_list = retrun_data_from_stored_procedure_as_df(schema_name=REDSHIFT_SCHEMA, sp_name='sftp_process_get_list_of_processed_files_for_unique_load_name', params={
                                                                   'p_unique_load_name': row['unique_load_name'], 'rs_out': 'rs_out'})

            # Filter items
            filtered_items = [item for item in items if f"{item['path']}" not in processed_files_list['file_name'].tolist()]

            print('Filtered Items are as below')
            print(filtered_items)

            items = filtered_items
            # Iterate through the list of files and print their names and modified times
            if DEBUG:  # When set debug deal with only one file rather than all files
                items = items[:1]

            for item in items:
                current_date_time = datetime.now()
                file_name = item['path']  #Complete path
             
                task_id = "Downloading_file_" + re.sub( r'[\W_]' , '', file_name.split('/')[-1])
                # print(f"{item['name']} modified at {item['modifiedTime']}")
                #if we have already processed the same file based on unique_load_name and file_name and processed = True then we skip and move to next file
                #v_is_file_processed = return_first_cell_data_from_redshift_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='sftp_process_check_if_file_processed', params={
                #                                                  'p_unique_load_name': row['unique_load_name'], 'p_file_name': file_name, 'rs_out': 'rs_out'})
                #if file is already processed continue to next file
                #if v_is_file_processed:
                #    continue

                update_load_process_reference_table(p_unique_load_name = unique_load_name,p_file_name= file_name, p_airflow__dag_id= dag_id, p_airflow__run_id= run_id, p_first_time_check=1, p_process_current_status='Load Process is in Progress' )
                try:
                    download_file_from_sftp(task_id=task_id, file_name_with_path=file_name, local_path='/tmp',pre_processing_script=pre_processing_script)
                    prepare_files_to_upload_to_s3(task_id=task_id, file_name_with_path=file_name, local_path='/tmp',file_extension=file_extension,source_file_delimiter=source_file_delimiter, top_rows_to_skip=top_rows_to_skip,source_sheet_name=source_sheet_name,column_metadata_df=column_metadata_df)
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
with DAG(dag_id="sftp_ingest_data_to_redshift",
         description="Ingests the data from GDrive into redshift",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=3),
         schedule_interval="0 * * * *",
         start_date=datetime(2023, 4, 2, 0, 0, 0, 0),
         catchup=False,
         tags=["sftp", "ingestions"]
         ) as dag:
    START_TASK_ID = 'start_processing_files_from_gdrive'

    dummy_start = EmptyOperator(task_id=START_TASK_ID)

    test_concept = PythonOperator(task_id='test_concept',
                                  python_callable=test_concept,
                                  op_kwargs={'message': 'Test Concept'}

                                  )
    get_sftp_metadata_from_metadataSheet = PythonOperator(task_id='get_sftp_metadata_from_metadataSheet',
                                                            python_callable=get_sftp_metadata_from_metadataSheet
                                                            )

    run_sp_to_create_target_tables_based_on_metadata_table = PythonOperator(task_id='run_sp_to_create_target_tables_based_on_metadata_table',
                                                                            python_callable=redshift_call_stored_procedure,
                                                                            op_kwargs={
                                                                                'schema_name': REDSHIFT_SCHEMA, 'sp_name': 'sftp_process_create_target_table_based_on_metadata'},
                                                                            )
    run_sp_to_update_target_tables_based_on_metadata_table = PythonOperator(task_id='run_sp_to_update_target_tables_based_on_metadata_table',
                                                                            python_callable=redshift_call_stored_procedure,
                                                                            op_kwargs={
                                                                                'schema_name': REDSHIFT_SCHEMA, 'sp_name': 'sftp_process_update_target_table_based_on_metadata'},
                                                                            )
    get_unique_load_details_from_metadata_table = PythonOperator(task_id='get_unique_load_details_from_metadata_table',
                                                                 python_callable=assign_redshift_sp_output_to_df,
                                                                 op_kwargs={
                                                                     'schema_name': REDSHIFT_SCHEMA, 'sp_name': 'sftp_process_read_file_metadata_and_column_mapping_metadata', 'result_name': 'rs_out'}

                                                                 )
    list_and_transfer_files_from_sftp_to_redshift = PythonOperator(task_id='list_and_transfer_files_from_sftp_to_redshift',
                                                              python_callable=list_files_to_transfer,
                                                              op_kwargs={
                                                                  'run_id': '{{ run_id }}'},
                                                              provide_context=True
                                                              )


    dummy_start >> test_concept >> get_sftp_metadata_from_metadataSheet   \
        >> run_sp_to_create_target_tables_based_on_metadata_table >> run_sp_to_update_target_tables_based_on_metadata_table >> get_unique_load_details_from_metadata_table \
        >> list_and_transfer_files_from_sftp_to_redshift
    # >> load_files_from_S3_to_RedShift

if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
