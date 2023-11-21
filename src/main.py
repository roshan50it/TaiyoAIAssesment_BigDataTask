from pyspark.sql import SparkSession
from transformations.transformation import group_by_transform, join_transform
from connectors.s3.s3_connector import read_from_s3, write_to_s3
from common.base_logger import logger
import common.Constants as const
import util.ConfigReader as reader

import boto3
from botocore.exceptions import ClientError


#  read the values of
#  secretKey from aws secrets manager
def get_secret_access_key():
    secret_name = "interview_account_user_key"
    region_name = "us-east-1"
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    try:
        secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    return secret_value_response


# this can be enhanced for
# parameters based on env types
def create_request_param():
    dict_params = {}
    secret_value_response = get_secret_access_key()
    config_reader = reader.ConfigReader()
    env_params = config_reader.get_config_data('./config/connect.yml')
    dict_params = {
        const.AWS_ACCESS_KEY_ID: env_params[const.DEVELOPMENT][const.ACCESS_KEY],
        const.AWS_SECRET_ACCESS_KEY: secret_value_response['SecretString']
    }
    return dict_params


def main():
    spark = SparkSession.builder.appName("Assessment").getOrCreate()
    # input_data = spark.read.parquet("data/mt_cars.parquet")
    input_data = read_from_s3(spark, "s3://interview/input/data")
    logger.info(f'input data of mt_cars {input_data.show()}')
    grouped_data = group_by_transform(input_data)
    logger.info(f'group_by_transform data of mt_cars {grouped_data.show()}')
    reference_data = read_from_s3(spark, "s3://interview/input/data")
    joined_data = join_transform(grouped_data, reference_data)
    request_param = create_request_param()
    write_to_s3(joined_data, "s3://interview/output/path", request_param)
    spark.stop()


if __name__ == "__main__":
    logger.info(' started main- --------')
    try:
        main()
    except Exception as e:
        logger.error(f' exception found {e}')
        raise e
    logger.info(' ended main- --------')
