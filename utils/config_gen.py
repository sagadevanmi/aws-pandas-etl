from utils.rdbms_operations import RDBMSOperations
from utils.s3_operations import S3Operations
from utils.secret_manager_operations import SecretManagerOperations
from utils.redshift_operations import RedshiftOperations
import yaml

class ConfigGen:
    """
    Class which handles configurations and creates Helper objects
    """

    def __init__(
        self,
        job_name,
        source_name,
        source_id,
        rdbms_obj,
        s3_obj,
        redshift_obj,
        tables,
        is_local_run
    ):
        """
        Constructor
        """
        self.job_name = job_name
        self.source_name = source_name
        self.source_id = source_id
        self.rdbms_obj = rdbms_obj
        self.s3_obj = s3_obj
        self.redshift_obj = redshift_obj
        self.tables = tables
        self.is_local_run = is_local_run

    @classmethod
    def load_config(cls, logger):
        """

        :return app_settings: a ConfigGen object
        """
        try:
            args = {}
            job_name = args["JOB_NAME"]
            source_id = args["source_id"]
            source_name = args["source_name"]
            config_file = args["config_file_path"]

            config_dict = S3Operations.read_config_file(config_file, logger)
        except Exception as exc:
            print(f"Exception has occured while reading job parameters: {str(exc)}")
            yaml_data = open("history_load_config.yaml", "r")
            config_dict = yaml.safe_load(yaml_data)
            job_name = "HistoryLoad"
            source_id = config_dict["Local_Run_Config"]["src_id"]
            source_name = config_dict["Local_Run_Config"]["src_name"]

        is_local_run = False
        aws_env = config_dict["Local_Run_Config"].get("aws_env", "DEV")

        if config_dict["Local_Run_Config"]["is_local_run"] == "T":
            is_local_run = True
        connect_info = config_dict["Source_ID"][source_id]
        tables = config_dict["Source_ID"][source_id]["Tables"]

        secret_manager_details = config_dict["Source_ID"][source_id].get("Secrets_Manager")
        region = secret_manager_details.get("region_name")
        source_secret_name = secret_manager_details.get("source_secret_name")
        dest_secret_name = secret_manager_details.get("destination_secret_name")

        
        if not is_local_run:
            try:
                source_secret_manager_response = (
                    SecretManagerOperations.load_secret_manager_details(source_secret_name, region)
                )
                dest_secret_manager_response = (
                    SecretManagerOperations.load_secret_manager_details(dest_secret_name, region)
                )
            except Exception as exc:
                raise
        else:
            source_secret_manager_response = {
                "src_db_engine": "sqlserver",
                "src_db_host": config_dict["Local_Run_Config"]["src_host"],
                "src_db_port": "1433",
                "src_database": config_dict["Local_Run_Config"]["src_database"],
                "src_db_username": "",
                "src_db_password": "",
            }
            if aws_env == 'DEV':
                config_dict["Bucket_Name"] = 'eds-dev-s3-bucket'
                dest_secret_manager_response = {
                    "username": "redshiftadmin",
                    "password": "",
                    "cluster": "dev-redshift",
                    "iam_role": "arn:aws:iam:::role/dev-redshift-service-role",
                    "host": "dev-redshift.mqskjd.us-west-1.redshift.amazonaws.com",
                    "port": "5439"
                }
            elif aws_env == 'STG':
                config_dict["Bucket_Name"] = 'stage-s3-bucket'
                dest_secret_manager_response = {
                    "username": "redshiftadmin",
                    "password": "",
                    "cluster": "stage-redshift",
                    "iam_role": "arn:aws:iam:::role/stage-redshift-service-role",
                    "host": "stage-redshift.mqskjd.us-west-1.redshift.amazonaws.com",
                    "port": "5439"
                }
            elif aws_env == 'PROD':
                config_dict["Bucket_Name"] = 'prod-s3-bucket'
                dest_secret_manager_response = {
                    "username": "redshiftadmin",
                    "password": "",
                    "cluster": "prod-redshift",
                    "iam_role": "arn:aws:iam:::role/prod-redshift-service-role",
                    "host": "prod-redshift.mqskjd.us-west-1.redshift.amazonaws.com",
                    "port": "5439"
                }

        src_details = {
                "src_db_engine": source_secret_manager_response.get(
                    "src_db_engine"
                ),
                "src_db_host": source_secret_manager_response.get("src_db_host"),
                "src_db_port": source_secret_manager_response.get("src_db_port"),
                "src_database": connect_info["DB_Info"]["source_database"],
                "src_db_username": source_secret_manager_response.get(
                    "src_db_username"
                ),
                "src_db_password": source_secret_manager_response.get(
                    "src_db_password"
                ),
                "src_schema": connect_info["DB_Info"]["source_schema"],
                "source": source_name,
                "is_local_run": is_local_run
            }
        
        rdbms_obj = RDBMSOperations(**src_details) if src_details else None


        s3_paths = None
        s3_paths = connect_info["S3_Paths"]
        s3_paths["bucket_name"] = config_dict["Bucket_Name"]
        s3_obj = S3Operations(**s3_paths) if s3_paths else None

        
        dest_details = {
            "username": dest_secret_manager_response.get("username"),
            "password": dest_secret_manager_response.get("password"),
            "cluster": dest_secret_manager_response.get("cluster"),
            "iam_role": dest_secret_manager_response.get("iam_role"),
            "host": dest_secret_manager_response.get("host"),
            "port": int(dest_secret_manager_response.get("port")),
            "database": connect_info["DB_Info"]["destination_database"],
            "schema": connect_info["DB_Info"]["destination_schema"],
        }
        del config_dict["Source_ID"][source_id]["Secrets_Manager"]
        redshift_obj = RedshiftOperations(**dest_details) if dest_details else None
    

        if not rdbms_obj or not redshift_obj or not s3_obj or not tables:
            raise ValueError("Unable to locate required arguments.")


        return cls(
            job_name,
            source_name,
            source_id,
            rdbms_obj,
            s3_obj,
            redshift_obj,
            tables,
            is_local_run
        )
