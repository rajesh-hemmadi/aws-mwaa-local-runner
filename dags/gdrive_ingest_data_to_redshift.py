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
DEBUG = True

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

def is_single_ascii(character):
    # Get the Unicode code point of the character
    code_point = ord(character)

    # Check if the code point is within the ASCII range (0-127)
    return code_point >= 0 and code_point <= 127

def get_gdrive_metadata_from_metadataSheet():
    try:
        # Specify the file path and sheet name of the Google Sheet
        spreadsheet_id = '1Y14A2lfHOTCS1GqtQBsYfHtqihziJzjyyrNwAOw7tng'
        file_metadata_sheet_name = "file_metadata!A:M"
        file_metadata_columns = ['id', 'source_folder_path', 'source_file_type', 'source_file_delimiter', 'source_sheet_name', 'top_rows_to_skip',
                                 'pre_processing_script', 'target_table', 'is_active',   'target_db', 'target_schema', 'unique_load_name']
        column_metadata_sheet_name = "column_metadata!A:J"
        column_metadata_columns = ['id', 'file_metadata_id', 'source_column_name', 'target_column_name',
                                   'target_column_order', 'target_data_type', 'target_length', 'source_format', 'target_format']

        # Initialize the Google Sheets client using the gsheets_service_account connection
        hook = GSheetsHook(gcp_conn_id=GDRIVE_CONN)
        # Read all data from the sheet and store it in a list of lists
        file_metadata = hook.get_values(
            spreadsheet_id=spreadsheet_id, range_=file_metadata_sheet_name)
        df_file_metadata = pd.DataFrame(
            file_metadata[1:], columns=file_metadata[0])
        df_file_metadata = df_file_metadata[file_metadata_columns]
        df_file_metadata = df_file_metadata.astype(
            str)  # convert all columns types to string
        df_file_metadata['id'].replace('', np.nan, inplace=True)
        df_file_metadata.dropna(subset=['id'], inplace=True)
        print(df_file_metadata)
        load_data_to_redshift_temp_temple(
            df_file_metadata, 'file_metadata_temp')

    except Exception as err:
        print(err)
        message = "Error Loading File Metadata : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)

    try:
        column_metadata = hook.get_values(
            spreadsheet_id=spreadsheet_id, range_=column_metadata_sheet_name)
        df_column_metadata = pd.DataFrame(
            column_metadata[1:], columns=column_metadata[0])
        df_column_metadata = df_column_metadata[column_metadata_columns]
        df_column_metadata = df_column_metadata.astype(
            str)  # convert all columns types to string
        df_column_metadata['id'].replace('', np.nan, inplace=True)
        df_column_metadata.dropna(subset=['id'], inplace=True)
        print(df_column_metadata)

        load_data_to_redshift_temp_temple(
            df_column_metadata, 'column_metadata_temp')
    except Exception as err:
        print(err)
        message = "Error Loading Column Metadata : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)

def xlsx_to_csv(input_file, input_sheet_name, output_file):
    # Read the XLSX file
    df = pd.read_excel(input_file, sheet_name=input_sheet_name)
    
    # Write the DataFrame to a CSV file with comma separator
    df.to_csv(output_file, index=False, sep=',', quoting=0)

    os.remove(input_file)
    


def get_files_from_gdrive_parent_subfolders(parent_folder_id, mimetype, subfolder_level=None):
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
        file_id = file['id']
        file_name = file['name']
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


def read_column_metadata_final_produce_df(**kwargs):
    try:
        redshift_hook = RedshiftSQLHook(redshift_conn_id=REDSHIFT_CONN_ID)
        schema_name = kwargs.get('schema_name')
        sp_name = kwargs.get('sp_name')
        # Even thoough it is expecting dictionery of parameters, it considers only values so be careful of order of paramateres
        params = kwargs.get('params')
        param_str = ", ".join([f"'{value}'" if not isinstance(
            value, int) else f"{value}" for value in params.values()])
        result_name = param_str.split(',')[-1].replace("'", "")
        print("CALL {}.{}({});".format(schema_name, sp_name, param_str))
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
                    print('result is below')
                    print(result)
                    if result:
                        results.append(result)
                        # Extract the column names
                        column_names.append([col_desc[0]
                                            for col_desc in cursor.description])

        data = results[1]
        column_list = [
            element for nested_list in data for element in nested_list]
        return column_list
    except Exception as err:
        print(err)
        message = "Function read_column_metadata_final_produce_df failed : \n" + \
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


def read_s3_csv_first_row(bucket_name, key, source_file_delimiter=',',top_rows_to_skip=1):
    try:
        print(bucket_name)
        print(key)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        obj = s3_hook.get_key(key, bucket_name)
        print('Object')
        print(obj)
        line_number = 1
        for data in obj.get()['Body']._raw_stream:
            if line_number == top_rows_to_skip:
                required_header_line =  data.decode("utf-8")
                break
            line_number += 1
        print('Required Line')
        print(required_header_line)
        print('Required Header Line')
        print(required_header_line)
        required_header_line = required_header_line.split(source_file_delimiter)
        required_header_line = [s.strip('\ufeff\r\n') for s in required_header_line]
        return required_header_line
    except Exception as err:
        print(err)
        message = "Function read_s3_csv_first_row failed : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)


def read_s3_xlsx_first_row(bucket_name, key):
    # This is not complete
    s3_hook = S3Hook()
    obj = s3_hook.get_key(key, bucket_name)
    df = pd.read_excel(obj, sheet_name=0, header=None, nrows=1)
    return df


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

        # columns = ['id','source_folder_path','source_file_type','source_sheet_name','is_active','source_folder_name','target_table','target_db','target_schema','unique_load_name','file_last_extract_datetime']
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
        # for index, row in df.iterrows():
        #     print(row['file_last_extract_datetime'])

        json_df = df.to_json(date_format='iso', orient='records')
        # print(json_df)

        ti.xcom_push(key='unique_load_details', value=json_df)
    except Exception as err:
        print(err)
        message = "Function assign_redshift_sp_output_to_df failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)


def transfer_file_from_gdrive_to_s3(**kwargs):
    try:
        drive_hook = GoogleDriveHook(gcp_conn_id=GDRIVE_CONN)
        task_id = kwargs.get('task_id', 'Download')
        folder_id = kwargs.get('folder_id')
        file_name = kwargs.get('file_name') #it has fullpath
        file_extension = kwargs.get('file_extension')
        source_sheet_name = kwargs.get('source_sheet_name')
        only_file_name = file_name.split('/')[-1]
        download_file_name = only_file_name
        if  file_extension == 'xlsx':
            base_name, extension = os.path.splitext(file_name)
            s3_upload_file_name_with_path = base_name + '.csv'
            upload_file_name = base_name.split('/')[-1] + '.csv'
        else:
            s3_upload_file_name_with_path = file_name
            upload_file_name = download_file_name
        local_path = kwargs.get('local_path', '/tmp')
        bucket_name = kwargs.get('bucket_name', S3_BUCKET_NAME)
        s3_prefix_start = kwargs.get('s3_prefix_start', S3_PREFIX_START)
        unique_load_name = kwargs.get('unique_load_name', '')
        s3_folder_name = '{}/{}'.format(s3_prefix_start, unique_load_name)
        print('Here I am ')
        print(bucket_name)
        print(s3_folder_name)
        final_local_path = local_path + '/' + os.path.dirname(file_name)
        print('Final local path is :', final_local_path)
        os.makedirs(final_local_path, exist_ok=True)
        # Define GoogleDriveFileDownloadOperator to download file
        download_from_gdrive_to_local = GoogleDriveToLocalOperator(
            gcp_conn_id=GDRIVE_CONN,
            task_id=task_id,
            folder_id=folder_id,  # folder_id
            file_name=download_file_name,
            # /tmp/MX1049_Paystack_Daily_IPG_Report_2023_03_01_090151.csv',
            output_file='{}/{}'.format(final_local_path, download_file_name)
        )

        #s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        # # Check if the folder already exists
        # if not s3_hook.check_for_prefix(bucket_name=bucket_name, prefix=s3_folder_name, delimiter='/'):
        #     s3_hook.create(bucket_name, s3_folder_name)

        s3_upload_operator = LocalFilesystemToS3Operator(
            task_id='upload_file_to_s3',
            aws_conn_id=S3_CONN_ID,
            filename='{}/{}'.format(final_local_path, upload_file_name),
            dest_key=s3_folder_name + '/' + s3_upload_file_name_with_path,
            dest_bucket=bucket_name,
            replace=True
        )

        try:
            download_from_gdrive_to_local.execute(context=None)
            print('File Downloaded from Gdrive')
            if file_extension == 'xlsx':
                xlsx_to_csv('{}/{}'.format(final_local_path, download_file_name),source_sheet_name, '{}/{}'.format(final_local_path, upload_file_name))
            s3_upload_operator.execute(context=None)
            #os.remove('{}/{}'.format(final_local_path, download_file_name))
            os.remove('{}/{}'.format(final_local_path, upload_file_name))
        except Exception as e:
            logging.error(f"An error occurred while downloading the file: {e}")
            raise e

        print('Process Completed')
    except Exception as err:
        print(err)
        message = "Function transfer_file_from_gdrive_to_s3 failed : \n" + \
            str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)


def list_files_to_transfer(**kwargs):
    try:
        dag_id = kwargs['dag'].dag_id
        run_id = kwargs['run_id']
        ti = kwargs['ti']
        json_df = ti.xcom_pull(key='unique_load_details')
        df = pd.read_json(json_df)
        print(df['file_last_extract_datetime'])
        
        # Specify the modified timestamp in RFC 3339 format
        for index, row in df.iterrows():
            # last_processed_datetime = (datetime.now() - timedelta(days=7)).isoformat() + 'Z'
            parent_folder_id = row['source_folder_path'].split('/')[-1]
            # print(row['file_last_extract_datetime'])
            last_processed_datetime = row['file_last_extract_datetime']
            file_extension = row['source_file_type']
            source_sheet_name = row['source_sheet_name']
            top_rows_to_skip = row['top_rows_to_skip']
            if top_rows_to_skip is None or top_rows_to_skip == 0 or top_rows_to_skip == '':
                top_rows_to_skip = 1
            else:
                top_rows_to_skip = top_rows_to_skip + 1
            print('Top rows to Skip : ' , top_rows_to_skip)
            source_file_delimiter = row['source_file_delimiter']
            if len(source_file_delimiter) > 1:
                is_delimiter_single_ascii = False
            else:
                is_delimiter_single_ascii = is_single_ascii(source_file_delimiter)
            if not is_delimiter_single_ascii:
                # We need to introduce function to convert this multi delimiter to single delimiter as COPY command wont handle multi delimiter
                # This process needs to happen once file gets downloaded to airflow server
                # Another approach to handle it is we load data as one row and delimit in redshift using pure sql
                # For now we will print message and continue to next file in loop
                print(f"We cannot process this delimiter : {source_file_delimiter}")
                continue 
                
            mimetype = mimetype_dict.get(file_extension)
            if mimetype is None:
                message = f"No MimeType found for File Extension '{file_extension}'"
                logging.info(message)
                # May be send an email
                # continue to next load
                send_notification(type="info", message=message)
                continue

            # # Call the Drive API to list files in the folder that have been modified after the specified timestamp
            # query = f"'{folder_id}' in parents and trashed = false and mimeType = '{mimetype}'"
            # # query = f"'{folder_id}' in parents and trashed = false and modifiedTime > '{last_processed_datetime}' and mimeType = '{mimetype}'"
            # # query = f"'{folder_id}' in parents and trashed = false and modifiedTime > '{last_processed_datetime}' and fullText contains '{file_extension}'"
            # results = drive_service.files().list(
            #     q=query, fields="nextPageToken, files(id, name, modifiedTime)").execute()
            # print(folder_id)
            # print('Below is expected list of files')
            # print(results)
            # items = results.get('files', [])
            subfolder_level = 1 #For future purpose, now it is hard coded to go only one level deep
            items = get_files_from_gdrive_parent_subfolders(parent_folder_id,mimetype,subfolder_level)

            # Iterate through the list of files and print their names and modified times
            if DEBUG:  # When set debug deal with only one file rather than all files
                items = items[:1]

            for item in items:
                current_date_time = datetime.now()
                file_name = item['path']  + item['name'] #Complete path
                file_parent_folder_id = item['parent_folder_id']
                task_id = f"Downloading_file_{file_name.split('/')[-1].replace(' ','_')}"
                # print(f"{item['name']} modified at {item['modifiedTime']}")
                #if we have already processed the same file based on unique_load_name and file_name and processed = True then we skip and move to next file
                v_is_file_processed = return_first_cell_data_from_redshift_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_check_if_file_processed', params={
                                                                   'p_unique_load_name': row['unique_load_name'], 'p_file_name': file_name, 'rs_out': 'rs_out'})
                #if file is already processed continue to next file
                if v_is_file_processed:
                    continue
                
                redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
                                               params={
                                                   'p_unique_load_name': row['unique_load_name'],
                                                   'p_file_name': file_name,
                                                   'p_file_last_extract_datetime': current_date_time,
                                                   'p_processed': 0,
                                                   'p_date_process_started': current_date_time,
                                                   'p_date_process_last_updated': current_date_time,
                                                   'p_airflow__dag_id': dag_id,
                                                   'p_airflow__run_id': run_id,
                                                   'p_airflow__external_trigger': 1,
                                                   'p_airflow__start_date': current_date_time,
                                                   'p_airflow__execution_date': current_date_time,
                                                   'p_process_current_status': 'File Transfer In Progress',
                                                   'p_first_time_check':1})
                transfer_file_from_gdrive_to_s3(task_id=task_id, folder_id=file_parent_folder_id, file_name=file_name, local_path='/tmp',
                                                bucket_name=S3_BUCKET_NAME, s3_prefix_start=S3_PREFIX_START, unique_load_name=row['unique_load_name'],file_extension=file_extension,source_sheet_name=source_sheet_name)
                current_date_time = datetime.now()
                # update gdrive_ingestions.unique_load_file_last_extract_datetime
                redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
                                               params={
                                                   'p_unique_load_name': row['unique_load_name'],
                                                   'p_file_name': file_name,
                                                   'p_file_last_extract_datetime': current_date_time,
                                                   'p_processed': 0,
                                                   'p_date_process_started': current_date_time,
                                                   'p_date_process_last_updated': current_date_time,
                                                   'p_airflow__dag_id': dag_id,
                                                   'p_airflow__run_id': run_id,
                                                   'p_airflow__external_trigger': 1,
                                                   'p_airflow__start_date': current_date_time,
                                                   'p_airflow__execution_date': current_date_time,
                                                   'p_process_current_status': 'File Transfer Completed',
                                                   'p_first_time_check':0})
                file_name_csv  = file_name
                if file_extension == 'xlsx':
                    base_name, extension = os.path.splitext(file_name)
                    file_name_csv = base_name + '.csv'
                load_files_from_S3_to_RedShift_one_file_at_a_time(dag_id=dag_id, run_id=run_id, unique_load_name=row['unique_load_name'], source_file_type=file_extension,source_file_delimiter=source_file_delimiter,top_rows_to_skip=top_rows_to_skip, target_table=row['target_table'],
                                                                  target_schema=row['target_schema'], file_name=file_name,file_name_csv=file_name_csv)
    except Exception as err:
        print(err)
        message = "Function list_files_to_transfer failed : \n" + str(err)
        send_notification(type="error", message=message)
        raise AirflowException(message)


def load_files_from_S3_to_RedShift_one_file_at_a_time(**kwargs):
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    S3_BUCKET = S3_BUCKET_NAME
    S3_PREFIX_FOLDER = S3_PREFIX_START
    dag_id = kwargs['dag_id']
    run_id = kwargs['run_id']
    unique_load_name = kwargs['unique_load_name']
    source_file_type = kwargs['source_file_type']
    source_file_delimiter = kwargs['source_file_delimiter']
    top_rows_to_skip = kwargs['top_rows_to_skip']
    target_table = kwargs['target_table']
    target_table_temp = target_table + '_temp'
    target_schema = kwargs['target_schema']
    file_name = kwargs['file_name']
    file_name_csv = kwargs['file_name_csv']
    fil = S3_PREFIX_FOLDER + '/'+unique_load_name+'/'+file_name_csv
    # Get Column list from redshift table
    required_column_list = read_column_metadata_final_produce_df(schema_name='gdrive_ingestions', sp_name='gdrive_process_read_column_metadata_final', params={
                                                                 'unique_load_name': unique_load_name, 'rs_out': 'rs_out'})
    file_columns = read_s3_csv_first_row(S3_BUCKET, fil, source_file_delimiter,top_rows_to_skip)
    for col_name in required_column_list:
        if col_name not in file_columns:
            print('Source Column Name ' + col_name + ' Not present in file ' + fil)
            print('There should be option to send email here')
            current_date_time = datetime.now()
            redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
                                           params={
                                               'p_unique_load_name': unique_load_name,
                                               'p_file_name': file_name,
                                               'p_file_last_extract_datetime': current_date_time,
                                               'p_processed': 0,
                                               'p_date_process_started': current_date_time,
                                               'p_date_process_last_updated': current_date_time,
                                               'p_airflow__dag_id': dag_id,
                                               'p_airflow__run_id': run_id,
                                               'p_airflow__external_trigger': 1,
                                               'p_airflow__start_date': current_date_time,
                                               'p_airflow__execution_date': current_date_time,
                                               'p_process_current_status': 'Column Name ' + col_name + ' Not present in file ' + file_name,
                                               'p_first_time_check':0})
            return
    # redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_create_temp_table_based_on_metadata', params={
    #                                'p_unique_load_name': unique_load_name})
    temp_column_list = ",".join(file_columns)
 
    redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_create_temp_table_with_columnslist', params={
                                    'p_unique_load_name': unique_load_name,'columns_list' : temp_column_list})



    # Copy data to temp table
    print('Copying File ' + fil)
    load_data_task = S3ToRedshiftOperator(
        task_id='load_data_to_redshift',
        s3_bucket=S3_BUCKET,
        s3_key=fil,
        schema='gdrive_ingestions',
        table=target_table_temp,
        copy_options=["CSV", f"DELIMITER '{source_file_delimiter}'", f"IGNOREHEADER {top_rows_to_skip}", "IGNOREBLANKLINES"],
        aws_conn_id=S3_CONN_ID,
        redshift_conn_id=REDSHIFT_CONN_ID
    )


    s3_copy_pbject_operator = S3CopyObjectOperator(
        task_id='Copy_file_to_Processed_folder_on_S3',
        source_bucket_name=S3_BUCKET,
        dest_bucket_name=S3_BUCKET,
        source_bucket_key=fil,
        dest_bucket_key=S3_PREFIX_FOLDER + '/Processed/' + unique_load_name+ '/' +file_name_csv,
        aws_conn_id=S3_CONN_ID

    )

    s3_delete_object_operator = S3DeleteObjectsOperator(
        task_id='delete_file_from_S3',
        bucket=S3_BUCKET,
        keys=fil,
        aws_conn_id=S3_CONN_ID,
    )
    current_date_time = datetime.now()
    redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
                                                       params={
                                                           'p_unique_load_name': unique_load_name,
                                                           'p_file_name': file_name,
                                                           'p_file_last_extract_datetime': current_date_time,
                                                           'p_processed': 0,
                                                           'p_date_process_started': current_date_time,
                                                           'p_date_process_last_updated': current_date_time,
                                                           'p_airflow__dag_id': dag_id,
                                                           'p_airflow__run_id': run_id,
                                                           'p_airflow__external_trigger': 1,
                                                           'p_airflow__start_date': current_date_time,
                                                           'p_airflow__execution_date': current_date_time,
                                                           'p_process_current_status': 'Data Loading from S3 to Redshift in progress',
                                                           'p_first_time_check':0
                                                           })
    load_data_task.execute(context=None)
    p_load_process_reference_id = return_first_cell_data_from_redshift_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_get_load_reference_id', params={
                                                                   'p_unique_load_name': unique_load_name, 'p_file_name': file_name, 'rs_out': 'rs_out'})
    redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_read_temp_load_target', params={
                                   'p_unique_load_name': unique_load_name, 'p_schema_name': target_schema, 'p_target_table': target_table, 'p_load_process_reference_id': p_load_process_reference_id})
    current_date_time = datetime.now()
    redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
                                                       params={
                                                           'p_unique_load_name': unique_load_name,
                                                           'p_file_name': file_name,
                                                           'p_file_last_extract_datetime': current_date_time,
                                                           'p_processed': 1,
                                                           'p_date_process_started': current_date_time,
                                                           'p_date_process_last_updated': current_date_time,
                                                           'p_airflow__dag_id': dag_id,
                                                           'p_airflow__run_id': run_id,
                                                           'p_airflow__external_trigger': 1,
                                                           'p_airflow__start_date': current_date_time,
                                                           'p_airflow__execution_date': current_date_time,
                                                           'p_process_current_status': 'Process Completed',
                                                           'p_first_time_check':0})
    # Copy file to processed folder
    s3_copy_pbject_operator.execute(context=None)
    # Delete file from S3
    s3_delete_object_operator.execute(context=None)


# def load_files_from_S3_to_RedShift(**kwargs):
#     try:
#         dag_id = kwargs['dag'].dag_id
#         run_id = kwargs['run_id']
#         S3_BUCKET = S3_BUCKET_NAME
#         S3_PREFIX_FOLDER = S3_PREFIX_START
#         # Using the same dataframe of unique load files list files inside S3 bucket
#         ti = kwargs['ti']
#         json_df = ti.xcom_pull(key='unique_load_details')
#         df = pd.read_json(json_df)

#         s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
#         for index, row in df.iterrows():
#             unique_load_name = row['unique_load_name']
#             source_file_type = row['source_file_type']
#             target_table = row['target_table']
#             target_table_temp = target_table + '_temp'
#             target_schema = row['target_schema']
#             # Get Column list from redshift table
#             required_column_list = read_column_metadata_final_produce_df(schema_name='gdrive_ingestions', sp_name='gdrive_process_read_column_metadata_final', params={
#                                                                          'unique_load_name': row['unique_load_name'], 'rs_out': 'rs_out'})

#             # get list of files
#             keys = s3_hook.list_keys(
#                 S3_BUCKET, prefix=f'{S3_PREFIX_FOLDER}/{unique_load_name}')
#             print('All files')
#             print(keys)

#             to_continue = True

#             # Earlier there was strange behaviour of ignoring first value so had to use keys[1:] like this, now that has vanished
#             for fil in keys:
#                 print('File Name is :', fil)
#                 if source_file_type == 'csv':
#                     file_columns = read_s3_csv_first_row(S3_BUCKET, fil)
#                 elif source_file_type == 'xlsx':
#                     file_columns = read_s3_xlsx_first_row(S3_BUCKET, fil)
#                 else:
#                     logging.info('Invalid File Type :' + source_file_type)
#                     continue
#                 # Check if all columns exist in source file as expected
#                 # compare required_column_list vs file_columns
#                 print(required_column_list)
#                 print(file_columns)

#                 for col_name in required_column_list:
#                     if col_name not in file_columns:
#                         print('Column Name ' + col_name +
#                               ' Not present in file ' + fil)
#                         print('There should be option to send email here')
#                         current_date_time = datetime.now()
#                         redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
#                                                        params={
#                                                            'p_unique_load_name': row['unique_load_name'],
#                                                            'p_file_name': fil.split('/')[-1],
#                                                            'p_file_last_extract_datetime': current_date_time,
#                                                            'p_processed': 0,
#                                                            'p_date_process_started': current_date_time,
#                                                            'p_date_process_last_updated': current_date_time,
#                                                            'p_airflow__dag_id': dag_id,
#                                                            'p_airflow__run_id': run_id,
#                                                            'p_airflow__external_trigger': 1,
#                                                            'p_airflow__start_date': current_date_time,
#                                                            'p_airflow__execution_date': current_date_time,
#                                                            'p_process_current_status': 'Column Name ' + col_name + ' Not present in file ' + fil,
#                                                            'p_first_time_check':0})
#                         to_continue = False

#                 if to_continue:
#                     # Create temp table
#                     try:
#                         current_date_time = datetime.now()
#                         redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
#                                                        params={
#                                                            'p_unique_load_name': row['unique_load_name'],
#                                                            'p_file_name': fil.split('/')[-1],
#                                                            'p_file_last_extract_datetime': current_date_time,
#                                                            'p_processed': 0,
#                                                            'p_date_process_started': current_date_time,
#                                                            'p_date_process_last_updated': current_date_time,
#                                                            'p_airflow__dag_id': dag_id,
#                                                            'p_airflow__run_id': run_id,
#                                                            'p_airflow__external_trigger': 1,
#                                                            'p_airflow__start_date': current_date_time,
#                                                            'p_airflow__execution_date': current_date_time,
#                                                            'p_process_current_status': 'Temp table creation in  progress',
#                                                            'p_first_time_check':0})
#                         redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_create_temp_table_based_on_metadata', params={
#                                                        'p_unique_load_name': unique_load_name})
#                         redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
#                                                        params={
#                                                            'p_unique_load_name': row['unique_load_name'],
#                                                            'p_file_name': fil.split('/')[-1],
#                                                            'p_file_last_extract_datetime': current_date_time,
#                                                            'p_processed': 0,
#                                                            'p_date_process_started': current_date_time,
#                                                            'p_date_process_last_updated': current_date_time,
#                                                            'p_airflow__dag_id': dag_id,
#                                                            'p_airflow__run_id': run_id,
#                                                            'p_airflow__external_trigger': 1,
#                                                            'p_airflow__start_date': current_date_time,
#                                                            'p_airflow__execution_date': current_date_time,
#                                                            'p_process_current_status': 'Temp table creation completed',
#                                                            'p_first_time_check':0})
#                     except Exception as e:
#                         message = f"An error occurred while loading the file: {e}"
#                         redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
#                                                        params={
#                                                            'p_unique_load_name': row['unique_load_name'],
#                                                            'p_file_name': fil.split('/')[-1],
#                                                            'p_file_last_extract_datetime': current_date_time,
#                                                            'p_processed': 0,
#                                                            'p_date_process_started': current_date_time,
#                                                            'p_date_process_last_updated': current_date_time,
#                                                            'p_airflow__dag_id': dag_id,
#                                                            'p_airflow__run_id': run_id,
#                                                            'p_airflow__external_trigger': 1,
#                                                            'p_airflow__start_date': current_date_time,
#                                                            'p_airflow__execution_date': current_date_time,
#                                                            'p_process_current_status': message,
#                                                            'p_first_time_check':0})
#                         send_notification(type="error", message=message)
#                         # raise AirflowException(message)

#                     # Copy data to temp table
#                     print('Copying File ' + fil)
#                     load_data_task = S3ToRedshiftOperator(
#                         task_id='load_data_to_redshift',
#                         s3_bucket=S3_BUCKET,
#                         s3_key=fil,
#                         schema='gdrive_ingestions',
#                         table=target_table_temp,
#                         copy_options=["CSV", "IGNOREHEADER 1",
#                                       "IGNOREBLANKLINES"],
#                         aws_conn_id=S3_CONN_ID,
#                         redshift_conn_id=REDSHIFT_CONN_ID
#                     )
#                     s3_copy_pbject_operator = S3CopyObjectOperator(
#                         task_id='Copy_file_to_Processed_folder_on_S3',
#                         source_bucket_name=S3_BUCKET,
#                         dest_bucket_name=S3_BUCKET,
#                         source_bucket_key=fil,
#                         dest_bucket_key=S3_PREFIX_FOLDER + '/Processed/' +
#                         fil.split('/')[-2] + '/' + fil.split('/')[-1],
#                         aws_conn_id=S3_CONN_ID

#                     )

#                     s3_delete_object_operator = S3DeleteObjectsOperator(
#                         task_id='delete_file_from_S3',
#                         bucket=S3_BUCKET,
#                         keys=fil,
#                         aws_conn_id=S3_CONN_ID,
#                     )
#                     try:
#                         load_data_task.execute(context=None)
#                         current_date_time = datetime.now()
#                         redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
#                                                        params={
#                                                            'p_unique_load_name': row['unique_load_name'],
#                                                            'p_file_name': fil.split('/')[-1],
#                                                            'p_file_last_extract_datetime': current_date_time,
#                                                            'p_processed': 0,
#                                                            'p_date_process_started': current_date_time,
#                                                            'p_date_process_last_updated': current_date_time,
#                                                            'p_airflow__dag_id': dag_id,
#                                                            'p_airflow__run_id': run_id,
#                                                            'p_airflow__external_trigger': 1,
#                                                            'p_airflow__start_date': current_date_time,
#                                                            'p_airflow__execution_date': current_date_time,
#                                                            'p_process_current_status': 'Data Loaded to temp Table',
#                                                            'p_first_time_check':0})
#                     except Exception as e:
#                         message = f"An error occurred while loading the file: {e}"
#                         send_notification(type="error", message=message)
#                         current_date_time = datetime.now()
#                         redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
#                                                        params={
#                                                            'p_unique_load_name': row['unique_load_name'],
#                                                            'p_file_name': fil.split('/')[-1],
#                                                            'p_file_last_extract_datetime': current_date_time,
#                                                            'p_processed': 0,
#                                                            'p_date_process_started': current_date_time,
#                                                            'p_date_process_last_updated': current_date_time,
#                                                            'p_airflow__dag_id': dag_id,
#                                                            'p_airflow__run_id':  run_id,
#                                                            'p_airflow__external_trigger': 1,
#                                                            'p_airflow__start_date': current_date_time,
#                                                            'p_airflow__execution_date': current_date_time,
#                                                            'p_process_current_status': message,
#                                                            'p_first_time_check':0})
#                         send_notification(type="error", message=message)
#                         continue
#                     # Create temp table
#                     try:
#                         p_load_process_reference_id = return_load_process_reference_id(schema_name='gdrive_ingestions', sp_name='gdrive_process_get_load_reference_id', params={
#                                                                                        'p_unique_load_name': row['unique_load_name'], 'p_file_name': fil.split('/')[-1], 'rs_out': 'rs_out'})
#                         redshift_call_stored_procedure(schema_name=REDSHIFT_SCHEMA, sp_name='gdrive_process_read_temp_load_target', params={
#                                                        'p_unique_load_name': unique_load_name, 'p_schema_name': target_schema, 'p_target_table': target_table, 'p_load_process_reference_id': p_load_process_reference_id})
#                         current_date_time = datetime.now()
#                         redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
#                                                        params={
#                                                            'p_unique_load_name': row['unique_load_name'],
#                                                            'p_file_name': fil.split('/')[-1],
#                                                            'p_file_last_extract_datetime': current_date_time,
#                                                            'p_processed': 1,
#                                                            'p_date_process_started': current_date_time,
#                                                            'p_date_process_last_updated': current_date_time,
#                                                            'p_airflow__dag_id': dag_id,
#                                                            'p_airflow__run_id': run_id,
#                                                            'p_airflow__external_trigger': 1,
#                                                            'p_airflow__start_date': current_date_time,
#                                                            'p_airflow__execution_date': current_date_time,
#                                                            'p_process_current_status': 'Process Completed',
#                                                            'p_first_time_check':0})
#                         # Copy file to processed folder
#                         s3_copy_pbject_operator.execute(context=None)
#                         # Delete file from S3
#                         s3_delete_object_operator.execute(context=None)

#                     except Exception as e:
#                         message = f"An error occurred while running SP  gdrive_process_read_temp_load_target: {e}"
#                         redshift_call_stored_procedure(schema_name='gdrive_ingestions', sp_name='gdrive_process_insert_update_load_process_reference',
#                                                        params={
#                                                            'p_unique_load_name': row['unique_load_name'],
#                                                            'p_file_name': fil.split('/')[-1],
#                                                            'p_file_last_extract_datetime': current_date_time,
#                                                            'p_processed': 0,
#                                                            'p_date_process_started': current_date_time,
#                                                            'p_date_process_last_updated': current_date_time,
#                                                            'p_airflow__dag_id': '1',
#                                                            'p_airflow__run_id': '1',
#                                                            'p_airflow__external_trigger': 1,
#                                                            'p_airflow__start_date': current_date_time,
#                                                            'p_airflow__execution_date': current_date_time,
#                                                            'p_process_current_status': message,
#                                                            'p_first_time_check':0})
#                         send_notification(type="error", message=message)
#                         continue
#                 else:
#                     print('For Loop will still continue')
#                     continue

#                 # Find file type, if file type is xslx convert it to csv
#     except Exception as err:
#         print(err)
#         message = "Function load_files_from_S3_to_RedShift failed : \n" + \
#             str(err)
#         send_notification(type="error", message=message)
#         raise AirflowException(message)


def test_concept(**kwargs):
    print(kwargs.get('sp_name'))


default_args = {
    "owner": "data engineers",
    "retries": 0,
    'on_failure_callback': ''
}
with DAG(dag_id="gdrive_ingest_data_to_redshift",
         description="Ingests the data from GDrive into redshift",
         default_args=default_args,
         max_active_runs=1,
         dagrun_timeout=timedelta(hours=3),
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

    run_sp_to_update_metadata_final_table = PythonOperator(task_id='run_sp_to_update_metadata_final_table',
                                                           python_callable=redshift_call_stored_procedure,
                                                           op_kwargs={
                                                               'schema_name': REDSHIFT_SCHEMA, 'sp_name': 'gdrive_process_insert_update_metadata'}
                                                           )
    run_sp_to_create_target_tables_based_on_metadata_table = PythonOperator(task_id='run_sp_to_create_target_tables_based_on_metadata_table',
                                                                            python_callable=redshift_call_stored_procedure,
                                                                            op_kwargs={
                                                                                'schema_name': REDSHIFT_SCHEMA, 'sp_name': 'gdrive_process_create_update_target_table_based_on_metadata'},

                                                                            )
    get_unique_load_details_from_metadata_table = PythonOperator(task_id='get_unique_load_details_from_metadata_table',
                                                                 python_callable=assign_redshift_sp_output_to_df,
                                                                 op_kwargs={
                                                                     'schema_name': REDSHIFT_SCHEMA, 'sp_name': 'gdrive_process_read_file_metadata_final', 'result_name': 'rs_out'}

                                                                 )
    list_and_transfer_files_from_drive_to_s3 = PythonOperator(task_id='list_and_transfer_files_from_drive_to_s3',
                                                              python_callable=list_files_to_transfer,
                                                              op_kwargs={
                                                                  'run_id': '{{ run_id }}'},
                                                              provide_context=True
                                                              )
    # load_files_from_S3_to_RedShift = PythonOperator(task_id='load_files_from_S3_to_RedShift',
    #                                              python_callable=load_files_from_S3_to_RedShift,
    #                                               op_kwargs={'run_id': '{{ run_id }}'},
    #                                               provide_context=True
    #                                              )

    dummy_start >> test_concept >> get_gdrive_metadata_from_metadataSheet >> run_sp_to_update_metadata_final_table  \
        >> run_sp_to_create_target_tables_based_on_metadata_table >> get_unique_load_details_from_metadata_table \
        >> list_and_transfer_files_from_drive_to_s3
    # >> load_files_from_S3_to_RedShift

if __name__ == '__main__':
    dag.clear(reset_dag_runs=True)
    dag.run()
