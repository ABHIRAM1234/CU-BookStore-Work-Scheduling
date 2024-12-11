import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

def convert_scheduled_data_to_json():
    # 1. Create database connection
    db_url = "postgresql://saaijeesh_rds:SAAI18max@dcsc-scheduling-db.cr6saecsqga3.ap-south-1.rds.amazonaws.com:5432/postgres"
    engine = create_engine(db_url)
    print("RDS Database Connection Successfull")

    # 2. Query the table and convert to dataframe
    query= 'SELECT * FROM scheduled.final_allocation;'
    with engine.connect() as conn:
        sql_query = pd.read_sql(
            sql=query,
            con=conn.connection
        )
    df = pd.DataFrame(sql_query)
    print("Final Allocation data query Successfull")

    # 3. Format by time and sort ascending

    # Step 1: Convert to datetime objects
    df["from_time"] = pd.to_datetime(df["from_time"], format="%H:%M:%S")
    df["to_time"] = pd.to_datetime(df["to_time"], format="%H:%M:%S")

    # Step 2: Sort by `from_time` in ascending order
    df = df.sort_values(by="from_time")

    # Step 3: Convert to AM/PM format
    def convert_time_format(time_obj):
        return time_obj.strftime("%I:%M%p").lstrip("0")  # Remove leading 0 for hours
    df["from_time"] = df["from_time"].apply(convert_time_format)
    df["to_time"] = df["to_time"].apply(convert_time_format)
    print("Data sorting and formating Successfull")

    # 4. Convert to dictionary
    dynamo_db_input=dict()

    email_lookup={
        'Carson Turk': 'saaijeesh23@gmail.com'
    }

    def check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name):
        if (emp_name not in dynamo_db_input): # Add name to dictionary if not exist and create template
            email=''
            if emp_name in email_lookup:  # Add email if exist
                email= email_lookup[emp_name]
            dynamo_db_input[emp_name]= {
                "Name": emp_name,
                "Data": [],
                "Email": email
            }
            return dynamo_db_input
        else:
            return dynamo_db_input

    for index,row in df.iterrows():
        # Convert Greeter Up
        if row['upstairs_greeter'] !='': 
            emp_name= row['upstairs_greeter']
            dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
            # Data row to append
            data_row={
                'Location': 'Greeter Upstair',
                'TimeIn': row['from_time'],
                'TimeOut': row['to_time']
                }
            dynamo_db_input[emp_name]['Data'].append(data_row)

        # Convert Greeter Down
        if row['downstairs_greeter'] !='': 
            emp_name= row['downstairs_greeter']
            dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
            # Data row to append
            data_row={
                'Location': 'Greeter Downstair',
                'TimeIn': row['from_time'],
                'TimeOut': row['to_time']
                }
            dynamo_db_input[emp_name]['Data'].append(data_row)

        # Convert Register Up
        if row['register_up'] !='': 
            emp_names= row['register_up'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Register Upstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
        # Convert Register Down
        if row['register_down'] !='': 
            emp_names= row['register_down'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Register Downstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
        # Convert Salesfloor Up
        if row['sf_up'] !='': 
            emp_names= row['sf_up'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Salesfloor Upstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
        if row['sf_down'] !='': 
            emp_names= row['sf_down'].split(", ")
            for emp_name in emp_names:
                dynamo_db_input= check_and_assign_new_name_dynamo_dict(dynamo_db_input,emp_name)
                # Data row to append
                data_row={
                    'Location': 'Salesfloor Downstair',
                    'TimeIn': row['from_time'],
                    'TimeOut': row['to_time']
                    }
                dynamo_db_input[emp_name]['Data'].append(data_row)
        
    # Remove names as keys and convert to a list of dictionaries
    dynamo_db_input_records= []

    for key,value in dynamo_db_input.items():
        dynamo_db_input_records.append(value)

    print("Data Conversion Successfull")

    return dynamo_db_input_records


print(convert_scheduled_data_to_json())