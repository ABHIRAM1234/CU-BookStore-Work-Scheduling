
import pandas as pd
from greeter_allocation import allocate_greeter
from register_salesfloor_acclocation import allocate_register_salesfloor

# Read required dataframes
# work_status_df=
# emp_requirements= # Formally emp_demand_check
work_status_df= pd.read_csv(r'C:\Users\niran\OneDrive - UCB-O365\01. Study\00. Revision & Self Study\01. Portfolio Projects\04. CU BookStore Scheduling\CU-BookStore-Work-Scheduling\Final_Code\test\work_status_df.csv')
emp_requirements= pd.read_csv(r'C:\Users\niran\OneDrive - UCB-O365\01. Study\00. Revision & Self Study\01. Portfolio Projects\04. CU BookStore Scheduling\CU-BookStore-Work-Scheduling\Final_Code\test\emp_requirements.csv')
########## Greeter Allocation ##########

# Format time objects in work_status_df
work_status_df['Start_time'] = pd.to_datetime(work_status_df['Start_time'], format='%H:%M:%S').dt.time
work_status_df['End_time'] = pd.to_datetime(work_status_df['End_time'], format='%H:%M:%S').dt.time
emp_requirements['From_Time'] = pd.to_datetime(emp_requirements['From_Time'], format='%H:%M:%S').dt.time
emp_requirements['To_Time'] = pd.to_datetime(emp_requirements['To_Time'], format='%H:%M:%S').dt.time

# Allocate Greeter
print("\nAllocating Greeters...\n")
greeter_assignment, greeter_shift_done_dict = allocate_greeter(work_status_df, emp_requirements)
print("\n Greeters Allocated!\n")

########## Register and Salesfloor Allocation ##########
print("\nAllocating Registers and Salesfloor...\n")
register_allocation= allocate_register_salesfloor(emp_requirements, work_status_df, greeter_assignment)
print("\n Registers and Salesfloor Allocated!!\n")

########## Merge Allocation ##########
final_allocation= pd.merge(greeter_assignment, register_allocation, how='outer', left_on=['From_Time', 'To_Time'], right_on=['From_Time', 'To_Time'])
final_allocation.to_excel(r'C:\Users\niran\OneDrive - UCB-O365\01. Study\00. Revision & Self Study\01. Portfolio Projects\04. CU BookStore Scheduling\CU-BookStore-Work-Scheduling\Final_Code\test\final_allocation.xlsx', index= False)
print("\n Allocated Merged and Saved!!\n")

