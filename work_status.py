#!/usr/bin/env python
# coding: utf-8

# In[380]:


import pandas as pd
from datetime import timedelta


# In[381]:


path="Book1.xlsx"
df=pd.read_excel(path,names=['Name', 'Roles', 'Responsibility', 'Time in', 'Time out'])
df.head(15)


# In[382]:


# Convert 'Time in' and 'Time out' to datetime first
df['Time in'] = pd.to_datetime(df['Time in'], format='%H:%M:%S')
df['Time out'] = pd.to_datetime(df['Time out'], format='%H:%M:%S')


# In[383]:


df.head()


# In[385]:


filtered_df = df[~df['Responsibility'].isin(['Technology', 'Office Work'])& ~df['Name'].str.contains('Available')]

# Display the filtered DataFrame
filtered_df.head(5)


# In[387]:


new_rows = []
time_interval=30

first_start_time = filtered_df['Time in'].min()
max_end_time = filtered_df['Time out'].max()

# Get unique names from filtered_df
unique_names = filtered_df['Name'].unique()

# Iterate through each individual
for name in unique_names:
    start_time = first_start_time
    end_time = max_end_time
    
    # Generate 30 minute intervals
    while start_time < end_time:
        new_row = {
            'Name': name,
            'Start_time': start_time.time(),
            'End_time': (start_time + timedelta(minutes=time_interval)).time()
        }
        new_rows.append(new_row)
        start_time += timedelta(minutes=30)

# Create the new DataFrame
work_status_df = pd.DataFrame(new_rows)

# Display the new DataFrame
work_status_df.head(15)


# In[392]:


def get_working_flag(name, start_time, end_time):
    # Get all working hours for the specific name
    employee_records = filtered_df[filtered_df['Name'] == name]
    
    # Iterate through all records for the employee
    for index, record in employee_records.iterrows():
        # Convert Time in and Time out to time objects
        time_in = record['Time in'].time()  # Use .time() to get the time object
        time_out = record['Time out'].time()  # Use .time() to get the time object

        # Check if the Start_time and End_time fall within the working hours
        if (time_in <= start_time < time_out) or (time_in < end_time <= time_out) or (start_time <= time_in and end_time >= time_out):
            return 1  # Working
    
    return 0  # Not working if none of the records match

# Add the 'Working Flag' column to work_status_df
work_status_df['Working Flag'] = work_status_df.apply(
    lambda row: get_working_flag(row['Name'], row['Start_time'], row['End_time']), axis=1
)

# Display the updated DataFrame
work_status_df.head(30)



# In[396]:


work_status_df[work_status_df['Name'].str.contains('Kayhaan Rashiq', case=False)]


# In[394]:


df[df['Name'].str.contains('Saaijeesh Sottalu Naresh', case=False)]


# In[ ]:




