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
    def castColumns(col_list, dtype, dataframe):
        """
        Function to cast columns to parquet-Redshift compatible types
        :param col_list: list of column names which need to be casted
        :param dtype: original column datatype
        :param dataframe: dataframe which needs to be updated
        :return dataframe: return the updated dataframe
        """
        if dtype == 'bit':
            if len(col_list) != 0:
                for col in col_list:
                    dataframe[f'{col}'] = dataframe[f'{col}'].astype('bool').astype('Int16')
            # print(f'Bit columns casted to Int16 for table: {table} chunk{chunk_no}')
        if dtype == 'tinyint':
            if len(col_list) != 0:
                for col in col_list:
                    dataframe[f'{col}'] = dataframe[f'{col}'].astype('Int16')
            # print(f'Bit columns casted to Int16 for table: {table} chunk{chunk_no}')
        elif dtype == 'decimal':
            if len(col_list) != 0:
                for col in col_list:
                    dataframe[f'{col}'] = dataframe[f'{col}'].astype('str')
                    dataframe.loc[dataframe[f'{col}'] == 'None', f'{col}'] = 'NaN'
                    dataframe[f'{col}'] = dataframe[f'{col}'].map(decimal.Decimal)
            # print(f'Decimal columns casted for table: {table} chunk{chunk_no}')
        elif dtype == 'date':
            if len(col_list) != 0:
                for col in col_list:
                    print(col)
                    if col != 'LastDayofWork' and col != 'LastDayofLeaveActual' and col != 'FirstDayBackatWork':
                        dataframe[f'{col}'] = dataframe[f'{col}'].astype('datetime64[ns]', errors='ignore').dt.date
            # print(f'Date columns converted to datetime64[ns] for table: {table} chunk{chunk_no}') , errors='ignore'
        return dataframe
    

    @staticmethod
    def addAuditColumns(dataframe, updatedby='redshiftadmin', updated_utc_ts=dt.datetime.now(timezone.utc).replace(tzinfo=timezone.utc).timestamp(), runid=-1):
        """
        Function to add audit columns to incoming dataframe
        :param dataframe: dataframe to which audit columns need to be added
        :param updatedby: value for this column
        :param updated_utc_ts: value for this column
        :param runid: value for this column
        :return dataframe: return the dataframe with added columns
        """
        dataframe['updatedby'] = updatedby
        dataframe['updated_utc_ts'] = updated_utc_ts
        dataframe['updated_utc_ts'] = pd.to_datetime(dataframe['updated_utc_ts'], unit='s').dt.ceil(freq='ms')
        dataframe['runid'] = runid
        return dataframe


    @staticmethod
    def add_row_hash_column(df, col_list):
        """
        Function to add row_hash_code column to incoming dataframe
        :param dataframe: dataframe to which row_hash_code column needs to be added
        :return dataframe: return the dataframe with added column
        """
        # df = df.fillna(value = '')
        df = df[col_list]
        df["concat"] = pd.Series(df[col_list].fillna('').values.tolist()).str.join(',')
        df["concat"] = '(' + df["concat"] + ')'
        df["row_hash_code"] = df["concat"].apply(lambda x: hashlib.md5(x.encode()).hexdigest())
        df.drop(['concat'], axis = 1)
        # df[""] = 
        return df


    @staticmethod
    def get_parquet_bytes(dataframe, parquet_schema):
        """
        Function to create bytes object out of dataframe
        :param dataframe: dataframe which needs to be converted to bytes
        :param parquet_schema: parquet schema which needs to be enforced
        :return body: return the bytes object
        """
        # Create PyArrow table from pandas Dataframe and enforce schema
        pa_table = pa.Table.from_pandas(dataframe, schema = parquet_schema, preserve_index = False)

        # Create parquet buffer which can be written to S3
        writer = pa.BufferOutputStream()
        pq.write_table(pa_table, writer)
        body = bytes(writer.getvalue())
        return body