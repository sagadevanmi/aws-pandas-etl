# import code

# install vs code
# install python latest version - 3.10.7

# pip install required libraries

# First read from SQL server using pandas -> (AWS credentials need to be changed, and database name in connection string needs to be changed)
# Pandas dataframe is in chunks of size 1000000
# So if count of records in table is greater than 10L then we will have multiple files created in S3
# Once we have data in S3, we can run the copy command to load data into Redshift(But DDLs need to be created beforehand)

# COPY stg.schema.tablename
# FROM 's3://dev-s3-bucket/static/database/tablename/' 
# IAM_ROLE 'arn:aws:iam:::role/redshift-service-role' 
# FORMAT AS PARQUET;

import decimal
import pyodbc
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import time
from datetime import timedelta

start_time = time.monotonic()


# Create boto3 Session using creadentials from AWS Login Page
aws_access_key_id=''
aws_secret_access_key=''
aws_session_token=''


session = boto3.Session(
    aws_access_key_id = aws_access_key_id,
    aws_secret_access_key = aws_secret_access_key,
    aws_session_token = aws_session_token
)

# S3 config
bucket = 'stage-s3-bucket'
bucket = 'dev-s3-bucket'
prefix = 'static/dbo/'

conn_str = (
            r'Driver=SQL Server;'
            r'Server={host_ip};' # edit {host_ip}
            r'Trusted_Connection=yes;'
            r'Database={database};' # edit {database}
            )
cnxn = pyodbc.connect(conn_str)
cnxn2 = pyodbc.connect(conn_str)
cursor = cnxn2.cursor()
print("MS SQL Server - PyODBC connection created successfully")

sql_schema = 'dbo'
table_str = """volunteers"""

table_list = table_str.split('\n')
table_count = 0

for table in table_list:
    print(table_count)
    print(f'Processing started for table: {table}')
    chunk_no = 1
    pd_query = f'SELECT * FROM {sql_schema}.{table}'
    for chunk_dataframe in pd.read_sql(pd_query, cnxn, chunksize=1000000):
        print(f'Got dataframe chunk{chunk_no} w/{len(chunk_dataframe)} rows')
        # print(chunk_dataframe.info())
        schema = create_pyarrow_schema(table)
        print(f'Schema created for table: {table}')  

        # Bit columns need to be cast to Int16
        bit_list_query = f"""
            declare @object_id int;

            select @object_id = object_id from sys.tables where name = '{table}' and schema_id = SCHEMA_ID('{sql_schema}');

            select  
            c.name + ', '
            from sys.tables t
            join sys.all_columns c
            on t.object_id = c.object_id
            join sys.types tt
            on c.user_type_id = tt.user_type_id
            where t.object_id = @object_id  and tt.name in ('bit', 'tinyint', 'boolean')  FOR XML PATH('')
        """

        res = cursor.execute(bit_list_query)
        res = cursor.fetchall()
        bit_col_list = []
        if len(res) != 0:
            bit_col_str = res[0][0]
            bit_col_list = bit_col_str.split(",")
            bit_col_list = [x.strip() for x in bit_col_list]
            if '' in bit_col_list:
                bit_col_list.remove('')
            print(bit_col_list)
        
        if len(bit_col_list) != 0:
            for col in bit_col_list:
                # print(col)
                chunk_dataframe[f'{col}'] = chunk_dataframe[f'{col}'].astype('bool').astype('Int16')
        print(f'Bit columns casted to Int16 for table: {table} chunk{chunk_no}')


        # Decimal columns need to be cast to Decimal
        decimal_list_query = f"""
            declare @object_id int;

            select @object_id = object_id from sys.tables where name = '{table}' and schema_id = SCHEMA_ID('{sql_schema}');

            select  
            c.name + ', '
            from sys.tables t
            join sys.all_columns c
            on t.object_id = c.object_id
            join sys.types tt
            on c.user_type_id = tt.user_type_id
            where t.object_id = @object_id  and tt.name in ('decimal', 'numeric', 'money')  FOR XML PATH('')
        """

        res = cursor.execute(decimal_list_query)
        res = cursor.fetchall()
        decimal_col_list = []
        if len(res) != 0:
            decimal_col_str = res[0][0]
            decimal_col_list = decimal_col_str.split(",")
            decimal_col_list = [x.strip() for x in decimal_col_list]
            if '' in decimal_col_list:
                decimal_col_list.remove('')
            print(decimal_col_list)
        # print(decimal_col_list)
        
        if len(decimal_col_list) != 0:
            for col in decimal_col_list:
                # print(col)
                chunk_dataframe[f'{col}'] = chunk_dataframe[f'{col}'].astype('str')
                chunk_dataframe.loc[chunk_dataframe[f'{col}'] == 'None', f'{col}'] = 'NaN'
                chunk_dataframe[f'{col}'] = chunk_dataframe[f'{col}'].map(decimal.Decimal)
        print(f'Decimal columns casted for table: {table} chunk{chunk_no}')

        # Date columns need to be cast to datetime64[ns]
        date_list_query = f"""
            declare @object_id int

            select @object_id = object_id from sys.tables where name = '{table}' and schema_id = SCHEMA_ID('{sql_schema}')

            select  
            c.name + ', '
            from sys.tables t
            join sys.all_columns c
            on t.object_id = c.object_id
            join sys.types tt
            on c.user_type_id = tt.user_type_id
            where t.object_id = @object_id  and tt.name in ('date')  FOR XML PATH('')
        """

        res = cursor.execute(date_list_query)
        res = cursor.fetchall()
        date_col_list = []
        if len(res) != 0:
            date_col_str = res[0][0]
            date_col_list = date_col_str.split(",")
            date_col_list = [x.strip() for x in date_col_list]
            if '' in date_col_list:
                date_col_list.remove('')
            print(date_col_list)
        
        if len(date_col_list) != 0:
            for col in date_col_list:
                chunk_dataframe[f'{col}'] = chunk_dataframe[f'{col}'].astype('datetime64[ns]', errors='ignore').dt.date
        print(f'Date columns converted to datetime64[ns] for table: {table} chunk{chunk_no}')

        # Create PyArrow table from pandas Dataframe and enforce schema
        pa_table = pa.Table.from_pandas(chunk_dataframe, schema=schema, preserve_index=False)

        # Create parquet buffer which can be written to S3
        writer = pa.BufferOutputStream()
        pq.write_table(pa_table, writer)
        body = bytes(writer.getvalue())

        s3 = session.client('s3', region_name='us-west-1')
        s3.put_object(Body=body, Bucket=bucket, Key=prefix + f'{table}/2022/11/29/05/{table}_{chunk_no}.parquet')
        chunk_no += 1
    table_count += 1

end_time = time.monotonic()
print("Time taken: " + str(timedelta(seconds=end_time - start_time)))