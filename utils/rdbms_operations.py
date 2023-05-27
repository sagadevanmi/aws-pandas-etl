import hashlib
from utils.dataframe_operations import DataframeOperations
import pymssql
import pandas as pd
import pyarrow as pa

class RDBMSOperations:
    """
    Class which contains RDBMS source related operations
    """

    def __init__(
        self,
        src_db_engine=None,
        src_db_host=None,
        src_db_port=None,
        src_database=None,
        src_db_username=None,
        src_db_password=None,
        src_schema=None,
        source=None,
        is_local_run=True
    ):
        """

        :param src_url: URL for the src database connection
        :param database: database name
        :param user_name: user name of the database
        :param password: password of the database
        :param schema: schema of database
        :param source: source of database
        :param is_local_run: flag which indicates whther script is run within aws env or not
        """
        # Dynamically creating src db url
        self.src_db_url = (
            "jdbc:" + src_db_engine + "://" + src_db_host + ":" + src_db_port + ";"
        )
        self.src_database = src_database
        self.src_db_host = src_db_host
        self.src_db_username = src_db_username
        self.src_db_password = src_db_password
        self.src_schema = src_schema
        self.source = source
        self.is_local_run = is_local_run


    def create_pyarrow_schema(self, cursor, table, log_rdbms, log_extra):
        """
        Create PyArrow schema object by fetching list of columns from SQL Server source
        :param cursor: PyODBC cursor object
        :param table: Table for which schema needs to be created
        :return schema: PyArrow schema object
        """
        # TODO put in try except block and correct SQL query case
        try:
            schema_query = f"""
                DECLARE @object_id INT

                SELECT @object_id = object_id 
                FROM sys.tables 
                WHERE name = '{table}' 
                    AND schema_id = SCHEMA_ID('{self.src_schema}')

                SELECT col + ', '
                FROM 
                (
                    SELECT  
                    'pa.field(''' + REPLACE(REPLACE(REPLACE(temp.name, ' ', '_'), '-', ''), '__', '_') + ''', '
                    + CASE WHEN temp.dtype IN ('bigint') THEN 'pa.int64()'
                    WHEN temp.dtype IN ('int') THEN 'pa.int32()'
                    WHEN temp.dtype IN ('smallint', 'tinyint', 'bit', 'boolean') THEN 'pa.int16()'
                    WHEN temp.dtype IN ('decimal', 'numeric') THEN 'pa.decimal128' + '( ' + CAST(temp.precision AS varchar) + ',' + CAST(temp.scale AS varchar) + ' )'
                    WHEN temp.dtype IN ('float', 'real') THEN 'pa.float32()'
                    WHEN temp.dtype IN ('money') THEN 'pa.decimal128(19, 4)'
                    WHEN temp.dtype IN ('text', 'char', 'nchar', 'varchar', 'nvarchar', 'uniqueidentifier', 'timestamp') THEN 'pa.string()'
                    WHEN temp.dtype IN ('date') THEN 'pa.date32()'
                    WHEN temp.dtype IN ('datetime', 'smalldatetime', 'time') THEN 'pa.timestamp("ms")'
                    END + ', '
                    + CASE WHEN temp.is_nullable = 1 THEN 'True' ELSE 'False' END + ')' AS col
                    FROM 
                    (
                        SELECT TOP 1000 tt.name AS dtype, c.is_identity, c.is_nullable, c.precision, c.scale, c.max_length, c.name, c.column_id 
                        FROM sys.tables t 
                        JOIN sys.all_columns c 
                            ON t.object_id = c.object_id 
                        JOIN sys.types tt 
                            ON c.user_type_id = tt.user_type_id
                        LEFT JOIN sys.key_constraints kc 
                            ON t.object_id = kc.parent_object_id
                        WHERE t.object_id = @object_id
                        ORDER BY c.column_id 
                    ) temp 
                ) AS qry FOR XML PATH('')
            """

            # Create PyArrow schema
            res = cursor.execute(schema_query)
            res = cursor.fetchall()
            final_res = ""
            completed_lines_hash = set()
            for s in res:
                final_res += s[0]
            final_res_arr = final_res.split("pa.field")

            pre_schema_str = ""
            for row in final_res_arr:
                if row != "":
                    row = "pa.field" + row
                    hashValue = hashlib.md5(row.rstrip().encode('utf-8')).hexdigest()
                    if hashValue not in completed_lines_hash:
                        pre_schema_str += row
                        completed_lines_hash.add(hashValue)
            
            # pre_schema_str = pre_schema_str[:-2]
            pre_schema_str += """pa.field('row_hash_code', pa.string(), True), pa.field('updatedby', pa.string(), True), pa.field('updated_utc_ts', pa.timestamp("ms"), True), pa.field('runid', pa.int32(), True)"""

            schema_str = 'pa.schema([' + pre_schema_str + '])'
            schema = pa.schema(eval(schema_str))
            return schema
        except Exception as exc:
            log_rdbms.error(f"Exception {str(exc)} while creating pyarrow schema from RDBMS source", extra=log_extra)
            raise exc


    def create_sql_server_connection(self, log_rdbms, log_extra, is_local_run=True):
        """
        Create PyODBC connection object
        :return cnxn: PyODBC connection object
        """
        try:
            if is_local_run:
                import pyodbc
                conn_str = (
                        r'Driver=ODBC Driver 17 for SQL Server;'
                        f'Server={self.src_db_host};'
                        r'Trusted_Connection=yes;' # FOR WINDOWS AUTHENTICATION
                        f'Database={self.src_database};'
                        )
                cnxn = pyodbc.connect(conn_str)
            else:
                cnxn = pymssql.connect(
                    server=f'{self.src_db_host}', # syntax => {host}:{port}\\{db_instance}
                    user=f'{self.src_db_username}', # syntax => {domain}\\{username}}
                    password=f'{self.src_db_password}',
                    database=f'{self.src_database}'
                )  
            
            log_rdbms.info("MS SQL Server - PyODBC/PyMSSQL connection created successfully")
            return cnxn
        except Exception as exc:
            log_rdbms.error(f"Exception {str(exc)} while creating RDBMS connection", extra=log_extra)
            raise exc


    def get_cols_with_datatype(self, cursor, table, dtype, log_rdbms, log_extra):
        """
        Get columns which have specified datatype in source table
        :param cursor: PyODBC cursor object
        :param table: Table from which columns need to be fetched
        :param dtype: String which has related dtypes separated by comma "('bit', 'tinyint', 'boolean')"
        """
        try:
            query = f"""
            DECLARE @object_id INT;

            SELECT @object_id = object_id 
            FROM sys.tables 
            WHERE name = '{table}' 
                AND schema_id = SCHEMA_ID('{self.src_schema}');

            SELECT  
            c.name + ', '
            FROM sys.tables t
            JOIN sys.all_columns c
                ON t.object_id = c.object_id
            JOIN sys.types tt
                ON c.user_type_id = tt.user_type_id
            WHERE t.object_id = @object_id  
                AND tt.name IN {dtype}  
            FOR XML PATH('')
            """

            res = cursor.execute(query)
            res = cursor.fetchall()
            dtype_col_list = []
            if len(res) != 0:
                dtype_col_str = res[0][0]
                dtype_col_list = dtype_col_str.split(",")
                dtype_col_list = [x.strip() for x in dtype_col_list]
                if '' in dtype_col_list:
                    dtype_col_list.remove('')
            return dtype_col_list
        except Exception as exc:
            log_rdbms.error(f"Exception in get_cols_with_datatype: {str(exc)}", extra=log_extra)


    def get_chunks(self, tablename, log_rdbms, log_extra, redshift_obj=None, red_schema=False):
        """
        Function to read from RDBMS source in chunks
        :param tablename: Table which needs to be read
        :yield bytes_obj: yield bytes_obj which needs to be written to s3
        """
        cnxn = self.create_sql_server_connection(log_rdbms, log_extra, False)
        cnxn1 = self.create_sql_server_connection(log_rdbms, log_extra, False)
        cursor = cnxn1.cursor()

        # create pyarrow schema using source DDL
        parquet_schema = self.create_pyarrow_schema(cursor, tablename)
        
        if red_schema:
            # create pyarrow schema using target DDL
            parquet_schema = redshift_obj.get_pyarrow_schema(tablename, log_rdbms, log_extra)
        
        log_rdbms.info(f"Parquet schema created successfully for {tablename}")
        
        bit_col_list = self.get_cols_with_datatype(cursor, tablename, "('bit', 'boolean')")
        decimal_col_list = self.get_cols_with_datatype(cursor, tablename, "('decimal', 'numeric', 'money')")
        date_col_list = self.get_cols_with_datatype(cursor, tablename, "('date')")
        tinyint_col_list = self.get_cols_with_datatype(cursor, tablename, "('tinyint')")

        log_rdbms.info(f"Fetched cols which need to be casted for table: {tablename}")

        # Tweak this query to load a specific set of data
        query = f"SELECT * FROM {self.src_schema}.{tablename}"
        run_id = -1

        for chunk_dataframe in pd.read_sql(query, cnxn, chunksize=1000000):
            
            # These datatype castings are required because pyarrow throws an error while schema enforcement
            chunk_dataframe = DataframeOperations.castColumns(bit_col_list, 'bit', chunk_dataframe, log_rdbms, log_extra)  
            chunk_dataframe = DataframeOperations.castColumns(decimal_col_list, 'decimal', chunk_dataframe, log_rdbms, log_extra) 
            chunk_dataframe = DataframeOperations.castColumns(date_col_list, 'date', chunk_dataframe, log_rdbms, log_extra)
            chunk_dataframe = DataframeOperations.castColumns(tinyint_col_list, 'tinyint', chunk_dataframe, log_rdbms, log_extra)
            log_rdbms.info(f"Casting completed for 1 chunk of {tablename}")

            # chunk_dataframe = DataframeOperations.add_row_hash_column(chunk_dataframe, df_cols)
            chunk_dataframe = DataframeOperations.addAuditColumns(chunk_dataframe, log_rdbms, log_extra, run_id=run_id)

            if red_schema:
                # standardise column names by making them lowercase, replacing spaces with underscores
                chunk_dataframe.columns = chunk_dataframe.columns.str.lower().str.replace(" ", "_")
                # If column name is like "content length - kb" THEN this step is required
                chunk_dataframe.columns = chunk_dataframe.columns.str.replace("-", "").str.replace("__", "_")

            bytes_obj = DataframeOperations.get_parquet_bytes(chunk_dataframe, parquet_schema)
            run_id -= 1

            yield bytes_obj
        