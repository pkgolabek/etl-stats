from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = spark.read.csv("/data/data.csv")
df.show()
logger.info("Great success! Very nice!")

