import pg8000 as pg

database = 'ods'

dev_host = 'dev-redshift.skamdd.us-west-1.redshift.amazonaws.com'
stg_host = 'stage-redshift.skamdd.us-west-1.redshift.amazonaws.com'
prod_host = 'prod-redshift.skamdd.us-west-1.redshift.amazonaws.com'

con = None
con = pg.connect(
        database = database,
        user = 'admin',
        password = 'password',
        host = dev_host,
        port = '5439',
        ssl_context = True
    )
print("Redshift connection created successfully")
cursor = con.cursor()



stmt = (
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name ILIKE '%s' AND "
        "table_schema ILIKE '%s' ORDER BY ordinal_position;"
        % ('volunteers', 'dbo')
    )

cursor.execute(stmt)
res_column_list = cursor.fetchall()
column_list = [x[0].lower() for x in res_column_list]
column_datatype = {k[0]: k[1] for k in res_column_list}
# print(column_list)
# print(column_datatype)

dtype_mapping = {
    'timestamp without time zone': 'timestamp("ms")',
    'character varying': 'string()',
    'bigint': 'int64()',
    'integer': 'int32()',
    'smallint': 'int16()',
    'date': 'date32()',
    'character': 'string()',
}

pre_schema_str = ''
for col, dtype in column_datatype.items():
    pre_schema_str += f"pa.field('{col}', pa.{dtype_mapping[f'{dtype}']}, True), "

pre_schema_str = pre_schema_str[:-2]
schema_str = 'pa.schema([' + pre_schema_str + '])'
print(schema_str)