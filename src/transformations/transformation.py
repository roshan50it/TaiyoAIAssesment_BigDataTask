# group_by_transform.py
from pyspark.sql import SparkSession


def group_by_transform(data):
    spark = SparkSession.builder.getOrCreate()
    grouped_data = data.groupBy("model").agg({"model": "count"})
    return grouped_data


# join_transform.py
def join_transform(data1, data2):
    joined_data = data1.join(data2, on="model", how="inner")
    return joined_data
