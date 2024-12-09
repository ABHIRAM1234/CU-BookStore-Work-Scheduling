import pandas as pd
from datetime import datetime, timedelta, time
import json
from utils import transform_time_inout, create_working_flag, create_remaining_hours, alert_employee_shortage 
# To ignore warnings
import warnings
warnings.filterwarnings("ignore")

########## Prepare Emp Availability dataset with Working Flag and Time Remaining ##########

# 1. Read Emp Availability table, filter and format Time columns
path="00_Input/01_Emp_Availability_Initial.xlsx" # TODO: Read from S3
df = pd.read_excel(path, names=['Name', 'Responsibility', 'Time in', 'Time out'])
filtered_df = df[~df['Responsibility'].isin(['Technology', 'Office Work']) & ~df['Name'].str.contains('Available')] # Filter out not-required roles and names
filtered_df= transform_time_inout(filtered_df)

# 2. Create Working Flag and Remaining Hours Left
work_status_df = create_working_flag(filtered_df)
work_status_df= work_status_df[work_status_df['Working Flag']==1] # Filter only working hours for every employee
work_status_df= create_remaining_hours(work_status_df, filtered_df)
work_status_df['Start_time'] = pd.to_datetime(work_status_df['Start_time'], format='%H:%M:%S').dt.time
work_status_df['End_time'] = pd.to_datetime(work_status_df['End_time'], format='%H:%M:%S').dt.time

########## Prepare Shift Requirement dataset and alert emp shortage ##########

# 1. Read Shift Req table, format Time columns
emp_count_req= pd.read_excel('00_Input/02_Emp_Count_Requirement.xlsx') # TODO: Read from S3
emp_count_req['From_Time'] = pd.to_datetime(emp_count_req['From_Time'], format='%H:%M:%S').dt.time
emp_count_req['To_Time'] = pd.to_datetime(emp_count_req['To_Time'], format='%H:%M:%S').dt.time

# 2. Alert if available employees are insufficient to satisfy the required count
emp_requirements= alert_employee_shortage(work_status_df, emp_count_req)

# TODO: Save the following files in S3
print('work_status_df')
print(work_status_df.head())
print('emp_requirements')
print(emp_requirements.head()) # Formally emp_demand_check


