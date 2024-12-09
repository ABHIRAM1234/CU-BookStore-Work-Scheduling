
import pandas as pd
from greeter_allocation import allocate_greeter

# Read required dataframes
work_status_df=
filtered_df=

########## Greeter Allocation ##########

# Convert 'Start_time' and 'End_time' to time objects in the main dataframe
work_status_df['Start_time'] = pd.to_datetime(work_status_df['Start_time'], format='%H:%M:%S').dt.time
work_status_df['End_time'] = pd.to_datetime(work_status_df['End_time'], format='%H:%M:%S').dt.time

# Allocate Greeter
greeter_assignment, greeter_shift_done_dict = allocate_greeter(work_status_df, filtered_df)
