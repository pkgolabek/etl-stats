from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger

# SparkSession is an entrance point for all spark operations
# Spark is case insensitive by default. This will sometimes cause 
# problems while reading data, as some attributes are poorly named
# and contain the same names but lower and upper case. this is especially
# problematic while handling json files
spark = SparkSession \
    .builder \
    .appName("ETL inestion") \
    .config("spark.sql.caseSensitive", True) \
    .getOrCreate()

# spark logging is set to INFO by default. This makes for a cumbersom log reading.
# Warn is enoug for debugging, but in normal operations we only really care 
# for errors
spark.sparkContext.setLogLevel("ERROR")
input_path = "/data/output/warehouse/"
output_path = "/data/output/reporting/"

input_paths = {
        "customers": input_path+"customers",
        "orders": input_path+"orders",
        "products": input_path+"products",
        "invoices": input_path+"invoices"
        }
input_dfs = {name: spark.read.parquet(path) for (name, path) in input_paths.items()}

output_paths = {
    "top10_country_num_customers": output_path+"top10_country_num_customers",
    "revenue_by_country": output_path+"revenue_by_country",
    "price_volume_relationship": output_path+"price_volume_relationship",
    "top3_price_drop": output_path+"top3_price_drop"
}

output_dfs = {
    "top10_country_num_customers": (
        input_dfs["orders"]
        .join(input_dfs["customers"], ["CustomerID"], "left")
        ),
    "revenue_by_country": (
        input_dfs["orders"]
        .join(input_dfs["customers"], ["CustomerID"], "left")
        ),
    "price_volume_relationship": (
        input_dfs["orders"]
        .join(input_dfs["products"])
    ),
    "top3_price_drop": (
        input_dfs["orders"]
        .join(input_dfs["invoices"], ["InvoiceNo"], "left")
        )
}

for name,df in output_dfs.items():
    logger.info(name)
    df.show(5,0)
    # reduce number of partitions (chunks of files) to 1. nicer for testing and small stuff.
    df.coalesce(1).write.mode("overwrite").parquet(output_paths[name])

logger.info("Great success! Very nice!")
