# aws-pandas-pyarrow-etl

## RDBMS to Redshift via S3
1. Read from RDBMS source like MS SQL Server or Postgres in chunks using Pandas
2. Cast columns to ensure compatability with Redshift data types
3. Enforce schema on these chunks using pyarrow
4. Write these chunks to Amazon S3 in parquet format
5. Load these chunks into Redshift using COPY command

## Why schema enforcement?
- Pyarrow schema can be created either based on source(RDBMS) or target(Redshift) DDLs
- The script currently generates schema based on the Redshift DDL, but it can be tweaked to generate pyarrow schema based on MSSQL source
- This schema enforcement step is necessary because without it Redshift COPY command might fail for certain data types

## Where to execute
This script can be executed in an EC2 instance which has 
  1. necessary permissions(either through IAM instance profile or access keys) to 
    a. write to S3
    b. run COPY command 
    c. fetch secrets from Secrets Manager
  2. security groups configured to create a connection with the Redshift cluster
  3. connection permisssions for connecting with the RDBMS source

This script can also be run locally if:
  1. access keys are configured 
  2. Redshift cluster is publicly accesible 
  3. Security groups allow inbound access to your machine's IP address

## How to run
1. Clone the repo, install all required libraries
2. Update `config.yaml` with all necessary changes
3. Ensure DDLs are created in Redshift
4. Run `python3 main.py`
