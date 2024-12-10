# REFERENCES:
    # https://github.com/hnawaz007/pythondataanalysis/blob/main/ETL%20Pipeline/automate_etl_with_airflow.py
    # https://www.youtube.com/watch?v=ZET50M20hkU&ab_channel=AmazonWebServices

from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, time
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine
from utils import transform_time_inout, create_working_flag, create_remaining_hours, alert_employee_shortage 
import pandas as pd
import io


@task()
def fetch_emp_avail_from_s3():
    # Initialize S3Hook with the default AWS connection
    s3_hook = S3Hook(aws_conn_id='bookstore_aws')
    
    # Fetch the file content from S3
    file_content = s3_hook.read_key(
        key='Processed/Daily Employee Availability/Emp_Availability_Initial.csv',  # Specify the S3 path
        bucket_name='bookstore-scheduling-bucket',  # Specify your S3 bucket
    )

    # Validate that content is not None or empty
    if not file_content:
        raise ValueError("File content is empty or missing")
    
    # Convert the file content to a pandas DataFrame
    # We use StringIO to treat the string as file-like for pandas to read
    file_like_object = io.StringIO(file_content)
    
    # Read the content into a DataFrame
    df = pd.read_csv(file_like_object) #, names=['Name', 'Responsibility', 'Time in', 'Time out'])
    
    # Return the DataFrame
    return df

@task()
def fetch_shift_req_from_s3():
    # Initialize S3Hook with the default AWS connection
    s3_hook = S3Hook(aws_conn_id='bookstore_aws')
    
    # Fetch the file content from S3
    file_content = s3_hook.read_key(
        key='Processed/Daily Shift Requirements/Emp_Count_Requirement.csv',  # Specify the S3 path
        bucket_name='bookstore-scheduling-bucket',  # Specify your S3 bucket
    )
    
    # Validate that content is not None or empty
    if not file_content:
        raise ValueError("File content is empty or missing")
    
    # Convert the file content to a pandas DataFrame
    # We use StringIO to treat the string as file-like for pandas to read
    file_like_object = io.StringIO(file_content)
    
    # Read the content into a DataFrame
    emp_count_req = pd.read_csv(file_like_object)
    
    # Return the DataFrame
    return emp_count_req

@task()
def prepare_emp_aval(df):
    # 1. Read Emp Availability table, filter and format Time columns
    # path="00_Input/01_Emp_Availability_Initial.xlsx" # TODO: Read from S3
    # df = pd.read_excel(path, names=['Name', 'Responsibility', 'Time in', 'Time out'])
    filtered_df = df[~df['Responsibility'].isin(['Technology', 'Office Work']) & ~df['Name'].str.contains('Available')] # Filter out not-required roles and names
    filtered_df= transform_time_inout(filtered_df)

    # 2. Create Working Flag and Remaining Hours Left
    work_status_df = create_working_flag(filtered_df)
    work_status_df= work_status_df[work_status_df['Working Flag']==1] # Filter only working hours for every employee
    work_status_df= create_remaining_hours(work_status_df, filtered_df)
    work_status_df['Start_time'] = pd.to_datetime(work_status_df['Start_time'], format='%H:%M:%S').dt.time
    work_status_df['End_time'] = pd.to_datetime(work_status_df['End_time'], format='%H:%M:%S').dt.time

    return work_status_df

@task()
def prepare_shift_req(work_status_df, emp_count_req):
    # 1. Read Shift Req table, format Time columns
    # emp_count_req= pd.read_excel('00_Input/02_Emp_Count_Requirement.xlsx') # TODO: Read from S3
    emp_count_req['From_Time'] = pd.to_datetime(emp_count_req['From_Time'], format='%H:%M:%S').dt.time
    emp_count_req['To_Time'] = pd.to_datetime(emp_count_req['To_Time'], format='%H:%M:%S').dt.time

    # 2. Alert if available employees are insufficient to satisfy the required count
    emp_requirements= alert_employee_shortage(work_status_df, emp_count_req)

    return emp_requirements

@task()
def store_work_status_in_rds(work_status_df):
    # Define the RDS connection string
    connection_string = "postgresql+psycopg2://<username>:<password>@<host>:<port>/<database>"
    
    # Create a SQLAlchemy engine
    engine = create_engine(connection_string)
    
    # Store the DataFrame in the RDS database
    with engine.connect() as connection:
        work_status_df.to_sql(
            name='work_status',  # Replace with your table name
            con=connection,
            if_exists='replace',  # Use 'append' to add rows instead of replacing
            index=False
        )

@task()
def store_emp_requirements_in_rds(emp_requirements):
    # Define the RDS connection string
    connection_string = "postgresql+psycopg2://<username>:<password>@<host>:<port>/<database>"
    
    # Create a SQLAlchemy engine
    engine = create_engine(connection_string)
    
    # Store the DataFrame in the RDS database
    with engine.connect() as connection:
        emp_requirements.to_sql(
            name='employee_requirements',  # Replace with your table name
            con=connection,
            if_exists='replace',  # Use 'append' to add rows instead of replacing
            index=False
        )

with DAG(dag_id="Bookstore_Scheduling_DAG", schedule_interval="0 9 * * *", start_date=datetime(2022, 3, 5), catchup=False, tags=["Bookstore"]) as dag:

    with TaskGroup("ETL_Input_Data", tooltip="Extract & transform Employee Availability and Shift Requirement data") as extract_trans_emp_aval:
        df = fetch_emp_avail_from_s3()
        work_status_df = prepare_emp_aval(df)
        emp_count_req = fetch_shift_req_from_s3()
        emp_requirements = prepare_shift_req(work_status_df, emp_count_req)
        
        # Define tasks to store transformed data in RDS
        store_work_status_task = store_work_status_in_rds(work_status_df)
        store_emp_requirements_task = store_emp_requirements_in_rds(emp_requirements)

        # Define the task order
        [df, emp_count_req] >> work_status_df >> emp_requirements
        emp_requirements >> [store_work_status_task, store_emp_requirements_task]

    extract_trans_emp_aval

