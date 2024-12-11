# Reference: https://www.youtube.com/watch?v=M2NzvnfS-hI
# !pip install sqlalchemy
# !pip install psycopg2

# Insert data into RDS
from sqlalchemy import create_engine

# 1. Format datatype and convert the dataframe into list of tuple
emp_demand_check["From_Time"] = emp_demand_check["From_Time"].astype(str)
emp_demand_check["To_Time"] = emp_demand_check["To_Time"].astype(str)
#emp_demand_check.info()
#emp_demand_check.head()

insert_values= [] # Will be a list of tuples
for index,row in emp_demand_check.iterrows():
    insert_values.append(tuple(row.values))

insert_values

# 2. Create database connection
db_url = "postgresql://saaijeesh_rds:SAAI18max@dcsc-scheduling-db.cr6saecsqga3.ap-south-1.rds.amazonaws.com:5432/postgres"
engine = create_engine(db_url)

# 3. Truuncate the table and insert new data row by row
truncate_query= "TRUNCATE TABLE transformed.emp_req; "
insert_query= "INSERT INTO transformed.emp_req  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
with engine.connect() as conn:
    conn.execute(truncate_query) 
    for record in insert_values:
        conn.execute(insert_query, record)

print("Data replaced successfully.")

# 4. Query the table and convert to dataframe
query= 'SELECT * FROM transformed.emp_req;'
with engine.connect() as conn:
    sql_query = pd.read_sql(
        sql=query,
        con=conn.connection
    )
df_test = pd.DataFrame(sql_query)
df_test
