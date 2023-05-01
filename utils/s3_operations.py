import re
from utils.aws_temp_keys import *
import boto3
import yaml

class S3Operations:
    """
    Class which contains S3 related operations
    """

    def __init__(self, landing_prefix, processed_prefix, bucket_name):
        """
        :param landing_prefix: 
        :param processed_prefix: 
        :param bucket_name: s3 bucket name
        """
        self.landing_prefix = landing_prefix
        self.processed_prefix = processed_prefix
        self.bucket_name = bucket_name


    @staticmethod
    def read_config_file(s3_path, log_s3):
        """
        Function to read config file from S3 and return a dictionary object
        :param s3_path: config file path
        :return dict_data: returns config file contents as a dictionary
        """
        # load the file from s3 using boto3 client
        pattern = re.compile(r"\/\/(?P<bucket>[a-z0-9][a-z0-9-]{1,61})\/?(?P<key>.*)")
        match = pattern.search(s3_path)

        # Extract bucketname and object key from S3 path 
        bucket = match.group("bucket")
        key = match.group("key")

        s3_client = boto3.client("s3")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        yaml_data = obj["Body"].read().decode("utf-8")

        try:
            dict_data = yaml.safe_load(yaml_data)
            return dict_data
        except yaml.YAMLError as exc:
            log_s3.error(f"Exception: {str(exc)} while trying to read config file")
            raise exc
    

    def write_to_s3(self, body, key, is_local_run, log_s3, log_extra):
        """
        Function to write bytes to s3 in specified path
        :param s3_client: boto3 s3 client
        :param body: bytes to be written
        :param key: key of file which needs to be written
        """
        try:
            s3_client = self.create_boto3_client(log_s3, log_extra, is_local_run)
            s3_client.put_object(Body=body, Bucket=self.bucket_name, Key=self.landing_prefix+key)
        except Exception as exc:
            log_s3.error(f"Exception while writing to S3: {str(exc)}", extra=log_extra)
            raise exc


    def create_boto3_client(self, log_s3, log_extra, is_local_run=True):
        """
        Function to create boto3 s3 client
        :param is_local_run: Flag which indicates whether script is run locally or through glue
        :return s3_client: boto3 s3 client object
        """
        try:
            if is_local_run:
                # If trying to run from non-AWS environment, we can pass keys in /utils/creds.py
                session = boto3.Session(
                    aws_access_key_id = aws_access_key_id,
                    aws_secret_access_key = aws_secret_access_key,
                    aws_session_token = aws_session_token
                )
                s3_client = session.client('s3', region_name = 'us-west-2')
            else:
                s3_client = boto3.client('s3', region_name = 'us-west-2')
            log_s3.info(f"s3_client created successfully")
            return s3_client
        except Exception as exc:
            log_s3.error(f"Exception {str(exc)} while trying to create s3_client", extra=log_extra)
            raise exc