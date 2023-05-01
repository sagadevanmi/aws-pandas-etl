import pg8000 as pg
import pyarrow as pa

class RedshiftOperations:
    """
    Class which contains Redshift related operations
    """
    def __init__(
        self,
        url=None,
        cluster=None,
        database=None,
        connection_type=None,
        username=None,
        password=None,
        database_engine=None,
        table=None,
        source=None,
        iam_role=None,
        host=None,
        port=None,
        schema=None
    ) -> None:
        """
        Constructor
        """
        self.url = url
        self.cluster = cluster
        self.database = database
        self.connection_type = connection_type
        self.username = username
        self.password = password
        self.database_engine = database_engine
        self.table = table
        self.source = source
        self.iam_role = iam_role
        self.host = host
        self.port = port
        self.schema = schema


    def create_redshiftconn(self, log_redshift, log_extra, database=None):
        """
            Create a pg8000 Redshift connection object and return it
        :return con: pg8000 connection object
        """
        try:
            if database:
                con = pg.connect(
                    database = database,
                    user=self.username,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    ssl_context=True,
                )
            else:
                con = pg.connect(
                    database=self.database,
                    user=self.username,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    ssl_context=True,
                )
        except Exception as exc:
            log_redshift.error(f"Some error while create pg8000 connection object: {exc}", extra=log_extra)
            raise exc
        return con


    def get_pyarrow_schema(self, tablename, log_redshift, log_extra):
        """
        Function to create pyarrow schema object to enforce on parquet files
        :param tablename: Table for which schema needs to be created
        :return schema: pyarrow schema object
        """
        try:
            stmt = (
                    "SELECT column_name, data_type, numeric_precision, numeric_scale FROM information_schema.columns WHERE table_name ILIKE '%s' AND "
                    "table_schema ILIKE '%s' ORDER BY ordinal_position;"
                    % (tablename, self.schema)
                )
            log_redshift.info("Creating Redshift Connection")
            conn = self.create_redshiftconn(log_redshift, log_extra)
            log_redshift.info("Created Redshift Connection successfully")
            cursor = conn.cursor()

            cursor.execute(stmt)
            res_column_list = cursor.fetchall()
            column_list = [x[0].lower() for x in res_column_list]
            column_datatype = {k[0]: k[1] for k in res_column_list}
            column_scale = {k[0]: f"({k[2]}, {k[3]})" for k in res_column_list}

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
            }

            pre_schema_str = ''
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
            log_redshift.info(f"Exception: {str(exc)} in get_col_dtype_mapping")
            raise exc


    def load_data(self, s3_location, redshift_table, log_redshift, log_extra):
        """
        Function which runs Redshift COPY command
        :param s3_location: file which we are looking to load
        :param redshift_table: destination table
        :return: load_status
        """
        log_redshift.info("Starting COPY command")
        load_status = None
        try:

            log_redshift.info(f"Loading data from s3 parquet files to redshift table {redshift_table}")

            # Create a Redshift connection, cursor
            log_redshift.info("Creating Redshift Connection")
            conn = self.create_redshiftconn(log_redshift, log_extra)
            log_redshift.info("Created Redshift Connection successfully")
            cur = conn.cursor()

            # Truncate the table before loading
            truncate_query = f"""TRUNCATE TABLE {self.schema}.{redshift_table};"""
            cur.execute(truncate_query)

            query = f"""
            COPY {self.schema}.{redshift_table}
            FROM '{s3_location}' 
            IAM_ROLE '{self.iam_role}' 
            FORMAT AS PARQUET;
            """

            # log_redshift.info(query)
            log_redshift.info("COPY command started")
            cur.execute(query)
            cur.execute("""SELECT PG_LAST_COPY_COUNT();""")
            affected_rows_count = cur.fetchone()
            conn.commit()
            conn.close()

            log_redshift.info(f"Affected Rows Count for {redshift_table}: {affected_rows_count}")
            load_status = True

        except Exception as exc:
            load_status = None
            log_redshift.error(f"Exception {exc} occurred while executing Redshift COPY command", extra=log_extra)
            raise exc

        return load_status, affected_rows_count
