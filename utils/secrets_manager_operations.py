import json
from utils.log_support import setup_logger
import boto3
from botocore.exceptions import ClientError


class SecretsManagerOperations:
    """Class for handling secrets manager operations"""

    @staticmethod
    def load_secrets_manager_details(secret_name, region_name):
        """

        :param secret_name: name of the secret which need to be fetched
        :param region_name: region where the service is running
        :return: Secret
        """
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        log = setup_logger()
        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
            if get_secret_value_response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                secret_string = get_secret_value_response["SecretString"]
                secret_manager_details = json.loads(secret_string)
                return secret_manager_details

        except ClientError as exception:
            log.error(str(exception))
            raise exception
