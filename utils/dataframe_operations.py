import decimal
import hashlib
from datetime import timezone
import datetime as dt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class DataframeOperations:
    """
    Class which contains Pandas Dataframe related operations
    """

    @staticmethod
    def castColumns(col_list, dtype, dataframe, log_df, log_extra):
        """
        Function to cast columns to parquet-Redshift compatible types
        :param col_list: list of column names which need to be casted
        :param dtype: original column datatype
        :param dataframe: dataframe which needs to be updated
        :return dataframe: return the updated dataframe
        """
        try:
            if col_list and len(col_list) != 0:
                if dtype == 'bit':
                    for col in col_list:
                        dataframe[f'{col}'] = dataframe[f'{col}'].astype('bool').astype('Int16')
                    # log_df.info(f'Bit columns casted to Int16 for table: {table} chunk{chunk_no}')
                if dtype == 'tinyint':
                    for col in col_list:
                        dataframe[f'{col}'] = dataframe[f'{col}'].astype('Int16')
                    # log_df.info(f'Bit columns casted to Int16 for table: {table} chunk{chunk_no}')
                elif dtype == 'decimal':
                    for col in col_list:
                        dataframe[f'{col}'] = dataframe[f'{col}'].astype('str')
                        dataframe.loc[dataframe[f'{col}'] == 'None', f'{col}'] = 'NaN'
                        dataframe[f'{col}'] = dataframe[f'{col}'].map(decimal.Decimal)
                    # log_df.info(f'Decimal columns casted for table: {table} chunk{chunk_no}')
                elif dtype == 'date':
                    for col in col_list:
                        # log_df.info(col)
                        # This will throw an error if date in the column is smaller than '1677-09-21' or greater than '2262-04-11'
                        dataframe[f'{col}'] = dataframe[f'{col}'].astype('datetime64[ns]', errors='ignore').dt.date
                    # log_df.info(f'Date columns converted to datetime64[ns] for table: {table} chunk{chunk_no}') , errors='ignore'
            else:
                log_df.info(f"No columnss to cast")
            return dataframe
        except Exception as exc:
            log_df.error(f"Exception while casting columns: {str(exc)}", extra=log_extra)
    

    @staticmethod
    def addAuditColumns(dataframe, log_df, log_extra, updatedby='redshiftadmin', updated_utc_ts=dt.datetime.now(timezone.utc).replace(tzinfo=timezone.utc).timestamp(), runid=-1):
        """
        Function to add audit columns to incoming dataframe
        :param dataframe: dataframe to which audit columns need to be added
        :param updatedby: value for this column
        :param updated_utc_ts: value for this column
        :param runid: value for this column
        :return dataframe: return the dataframe with added columns
        """
        try:
            dataframe['updatedby'] = updatedby
            dataframe['updated_utc_ts'] = updated_utc_ts
            dataframe['updated_utc_ts'] = pd.to_datetime(dataframe['updated_utc_ts'], unit='s').dt.ceil(freq='ms')
            dataframe['runid'] = runid
            return dataframe
        except Exception as exc:
            log_df.error(f"Exception while adding audit columns: {str(exc)}", extra=log_extra)


    @staticmethod
    def add_row_hash_column(df, col_list, log_df, log_extra):
        """
        Function to add row_hash_code column to incoming dataframe
        :param dataframe: dataframe to which row_hash_code column needs to be added
        :return dataframe: return the dataframe with added column
        """
        # df = df.fillna(value = '')
        try: 
            df = df[col_list]
            df["concat"] = pd.Series(df[col_list].fillna('').values.tolist()).str.join(',')
            df["concat"] = '(' + df["concat"] + ')'
            df["row_hash_code"] = df["concat"].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
            df.drop(['concat'], axis=1)
            # df[""] = 
            return df
        except Exception as exc:
            log_df.error(f"Exception while adding row_hash_code column: {str(exc)}", extra=log_extra)


    @staticmethod
    def get_parquet_bytes(dataframe, parquet_schema, log_df, log_extra):
        """
        Function to create bytes object out of dataframe
        :param dataframe: dataframe which needs to be converted to bytes
        :param parquet_schema: parquet schema which needs to be enforced
        :return body: return the bytes object
        """
        try:
            # Create PyArrow table from pandas Dataframe and enforce schema
            pa_table = pa.Table.from_pandas(dataframe, schema=parquet_schema, preserve_index=False)

            # Create parquet buffer which can be written to S3
            writer = pa.BufferOutputStream()
            pq.write_table(pa_table, writer)
            body = bytes(writer.getvalue())
            log_df.info(f"Bytes object created")
            return body
        except Exception as exc:
            log_df.error(f"Exception while creating bytes object from df: {str(exc)}", extra=log_extra)