import datetime
from utils.config_gen import ConfigGen
from utils.log_support import setup_logger


log = setup_logger()

class HistoryLoad:
    """
    Main Class for History load
    """

    def __init__(self, app_settings) -> None:
        """
        Constructor
        """
        self.job_name = app_settings.job_name
        self.source_name = app_settings.source_name
        self.source_id = app_settings.source_id
        self.rdbms_obj = app_settings.rdbms_obj
        self.s3_obj = app_settings.s3_obj
        self.redshift_obj = app_settings.redshift_obj
        self.tables = app_settings.tables
        self.is_local_run = app_settings.is_local_run
        self.extra_logging = None
        self.extra_logging = {
            "custom_logging": {
                "JobName": self.job_name,
                "AffectedPipeline": self.source_name + "-HistoryLoad",
                "ServiceName": "Glue",
            }
        }


    def process(self):
        successful_count = 0
        failed_tables = []
        error_dict = {}
        current_date = datetime.datetime.utcnow()
        process_start_time = current_date.strftime("%Y/%m/%d/%H/%M")
        with open("fsilure_logs.txt", "a") as f:
            f.write(f"\n----------{process_start_time}----------\n")
        for tablename, details in self.tables.items():
            try:
                if details["active_flag"] == "T":
                    log.info(f"Processing started for table: {tablename}")
                    chunk_no = 1

                    red_schema = details.get("red_schema", "T")

                    # Get date_time which will be used for S3 partitioning
                    current_date = datetime.datetime.utcnow()
                    formatted_date_time = current_date.strftime("%Y/%m/%d/%H")

                    # get_chunks yields a chunk, so we'll iterate over it and write each chunk to S3
                    for bytes_obj in self.rdbms_obj.get_chunks(tablename, log, self.extra_logging, self.redshift_obj, red_schema):
                        log.info(f"Table {tablename}: chunk{chunk_no} write_to_s3 in progress")
                        key = f"{tablename}/{formatted_date_time}/{tablename}_{chunk_no}.parquet"
                        
                        # write chunk to s3 with key=key
                        self.s3_obj.write_to_s3(bytes_obj, key, self.is_local_run, log, self.extra_logging)
                        log.info(f"Table {tablename}: chunk{chunk_no} written to S3")
                        chunk_no += 1
                    
                    # After writing the chunks to S3, we'll run Redshift COPY command 
                    load_path = f"s3://{self.s3_obj.bucket_name}/{self.s3_obj.landing_prefix}{tablename}/{formatted_date_time}/"
                    redshift_load_status, affected_rows_count = self.redshift_obj.load_data(load_path, tablename, log, self.extra_logging)

                    log.info(f"Table {tablename} processing completed")
                    successful_count += 1
                else:
                    log.info(f"Table {tablename} is not set active, hence skipped")
            except Exception as exc:
                log.error(f"Exception for {tablename}: {str(exc)} in process-main")
                failed_tables.append(tablename)
                with open("fsilure_logs.txt", "a") as f:
                    f.write(f"{tablename}: {str(exc)}\n")
        log.info(f"Successful tables: {successful_count}")
        log.info(f"Failed tables: {str(failed_tables)}")
        with open("fsilure_logs.txt", "a") as f:
            f.write("No failures in this run\n")


if __name__ == "__main__":
    # read secrets, config file, create helper class objects
    app_settings = ConfigGen.load_config(log)
    obj = HistoryLoad(app_settings)
    obj.process()