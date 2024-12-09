
import pandas as pd
from greeter_allocation import allocate_greeter
from register_salesfloor_acclocation import allocate_register_salesfloor

# Read required dataframes
work_status_df=
emp_requirements= # Formally emp_demand_check

########## Greeter Allocation ##########

# Format time objects in work_status_df
work_status_df['Start_time'] = pd.to_datetime(work_status_df['Start_time'], format='%H:%M:%S').dt.time
work_status_df['End_time'] = pd.to_datetime(work_status_df['End_time'], format='%H:%M:%S').dt.time

# Allocate Greeter
print("\nAllocating Greeters...\n")
greeter_assignment, greeter_shift_done_dict = allocate_greeter(work_status_df, emp_requirements)
print("\n Greeters Allocated!\n")

########## Register and Salesfloor Allocation ##########
print("\nAllocating Registers and Salesfloor...\n")
register_allocation= allocate_register_salesfloor(emp_requirements, work_status_df, greeter_assignment)
print("\n Registers and Salesfloor Allocated!!\n")


