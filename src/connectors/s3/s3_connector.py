import boto3
import src.common.Constants as const


def read_from_s3(spark, s3_path):
    return spark.read.parquet(s3_path)


def write_to_s3(data, s3_path, dict_params):
    s3_resource, s3_client = get_s3_resource_client(dict_params)
    data.write.mode("overwrite").parquet(s3_path)


def get_s3_resource_client(dict_params):
    session = boto3.Session(
        aws_access_key_id=dict_params[const.AWS_ACCESS_KEY_ID],
        aws_secret_access_key=dict_params[const.AWS_SECRET_ACCESS_KEY])
    s3_resource = session.resource('s3')
    s3_client = session.client('s3')
    return s3_resource, s3_client
