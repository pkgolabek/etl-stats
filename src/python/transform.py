from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger
import pandas

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
        input_dfs["customers"]
        .select("Country", "CustomerID")
        .groupBy("Country")
        .agg(F.count("CustomerID").alias("Customers_Number"))
        .orderBy(F.col("Customers_Number").desc())
        .limit(10)
        ),
    "revenue_by_country": (
        input_dfs["orders"]
        # for revenue we need orders and customers
        .join(input_dfs["customers"], ["CustomerID"], "left")
        .select("Country", "Value")
        .groupBy("Country").agg(F.sum("Value").alias("Summed_Value"))
        .orderBy(F.col("Summed_Value").desc())
        ),
    "price_volume_relationship": (
        # this should be plotted on a scatter plot
        input_dfs["orders"]
        .join(
            input_dfs["products"].select("StockCode", "AvgPrice").distinct(), 
            "StockCode", 
            "inner")
        .select("StockCode", "AvgPrice", "Quantity")
        .groupBy("StockCode")
        .agg(F.sum("Quantity").alias("Quantity"), F.first("AvgPrice").alias("AvgPrice"))
        .orderBy("AvgPrice")
    )
    # I think there is not enough information to calculate month to month drop in product price.
    # The only mention of price is in the product table, which contains no dates. We can try deriving
    # order of the prices by the order of rows, or the fact that for each productId with 2 prices,
    # one has description and the other doesnt, so the one without would probably be later, but this is
    # not the case for all rows and there is still no indication of date. In Orders a single product 
    # is registered as bougth over multiple months, so this is not a lead either.
}

for name,df in output_dfs.items():
    logger.info(name)
    df.show(5,0)
    # reduce number of partitions (chunks of files) to 1. nicer for testing and small stuff.
    df.coalesce(1).write.mode("overwrite").parquet(output_paths[name])
    # this line below is just for ease of checking the results without having to inspect parquets
    df.toPandas().to_csv(output_paths[name]+".csv", header=True, index=False)

logger.info("Great success! Very nice!")
