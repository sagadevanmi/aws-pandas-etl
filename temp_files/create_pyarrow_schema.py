import pyarrow as pa
import pg8000 as pg

def create_redshiftconn(database, username, password, host, port):
	"""
	Create a pg8000 Redshift connection object and return it
	:return con: pg8000 connection object
	"""
	try:
		con = pg.connect(
		database=database,
		user=username,
		password=password,
		host=host,
		port=port,
		ssl_context=True,
		)
	except Exception as exc:
		print(f"Some error while create pg8000 connection object: {exc}")
		raise exc
	return con

def get_pyarrow_schema(tablename):
	"""
	Function to create pyarrow schema object to enforce on parquet files
	:param tablename: Table for which schema needs to be created
	:return schema: pyarrow schema object
	"""
	try:
		stmt = f"""
				SELECT column_name, data_type, numeric_precision, numeric_scale 
				FROM information_schema.columns 
				WHERE table_name ILIKE '{tablename}' 
					AND table_schema ILIKE '{schema}' 
				ORDER BY ordinal_position;
			"""
		
		# add config details
		database = ""
		username = ""
		password = ""
		host = "" 
		port = 5439
		
		print("Creating Redshift Connection")
		conn = create_redshiftconn(database, username, password, host, port)
		print("Created Redshift Connection successfully")
		cursor = conn.cursor()

		cursor.execute(stmt)
		res_column_list = cursor.fetchall()
		column_list = [x[0].lower() for x in res_column_list]
		column_datatype = {k[0]: k[1] for k in res_column_list}
		column_scale = {k[0]: f"({k[2]}, {k[3]})" for k in res_column_list}

		# Not an exhaustive list, might need updates to handle other redshift datatypes
		dtype_mapping = {
				'timestamp without time zone': 'timestamp("ms")',
				'character varying': 'string()',
				'varchar': 'string()',
				'double precision': 'decimal128',
				'numeric': 'decimal128',
				'bigint': 'int64()',
				'integer': 'int32()',
				'smallint': 'int16()',
				'date': 'date32()',
				'character': 'string()',
				'real': 'float32()',
				'varbinary()': 'binary()',
		}

		pre_schema_str = ""
		for col, dtype in column_datatype.items():
				if dtype != 'numeric' and dtype != 'double precision':
					pre_schema_str += f"pa.field('{col}', pa.{dtype_mapping[f'{dtype}']}, True), "
				else:
					# Add precision and scale for numeric and double precision columns
					pre_schema_str += f"pa.field('{col}', pa.{dtype_mapping[f'{dtype}']}{column_scale[f'{col}']}, True), "
			
		pre_schema_str = pre_schema_str[:-2]
		schema_str = 'pa.schema([' + pre_schema_str + '])'
		schema = pa.schema(eval(schema_str))
		return schema
	except Exception as exc:
		print(f"Exception: {str(exc)} in get_col_dtype_mapping")
		raise exc