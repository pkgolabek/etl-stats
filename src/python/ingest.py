from pyspark.sql import SparkSession
from pyspark.sql import functions as F, Window as W
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
input_path = "/data/input/"
output_path = "/data/output/warehouse/"

raw_paths = {
        "customers": input_path+"customers.csv",
        "orders": input_path+"orders.csv",
        "products": input_path+"products.csv"
        }
output_paths = {
    "customers": output_path+"customers",
    "orders": output_path+"orders",
    "products": output_path+"products",
    "invoices": output_path+"invoices"
}

# the files we are provided contain a header row with names of the source columns
# we will use them to name columns in our datasets using header=true
load = spark.read.option("header", "true").csv

# having raw datasets in dictionary makes for easier iteration over all of them
raw_dfs = {name: load(raw_path) for (name, raw_path) in raw_paths.items()}

output_dfs = {
    "customers": raw_dfs["customers"],
    "products": (
        raw_dfs["products"]
        # some fields appear to be incorret, those that do are correct have description in all caps or null.
        # so we filter for those.
        .filter(F.col("Description").isNull() | F.col("Description").rlike("^[A-Z ]+$"))
        # we have no information of when the price was changed, so we ballpark average for future processing.
        .withColumn("AvgPrice", F.mean("UnitPrice").over(W.partitionBy("StockCode")))
    ),
    "orders": (
        raw_dfs["orders"]
        .select("InvoiceNo", "StockCode", "CustomerID", "Quantity")
        # quantity of an item on an invoice can only be 0 or more
        .filter(F.col("Quantity") >= 0)
        .distinct()
    ),
    "invoices": raw_dfs["orders"].select("InvoiceNo", "InvoiceDate").distinct()
}
output_dfs["orders"] = (
    output_dfs["orders"]
    # for our facts table it would be nice to have a price of specific item and quantity
    .join(output_dfs["products"].select("StockCode", "AvgPrice").distinct(), ["StockCode"], "left")
    .withColumn("Value", F.col("AvgPrice")*F.col("Quantity"))
    .drop("AvgPrice")
)

for name,df in output_dfs.items():
    logger.info(name)
    df.show(5,0)
    # coalesce 1 will ensure there is only one file with data, only a good idea for small datasets.
    df.coalesce(1).write.mode("overwrite").parquet(output_paths[name])
    # this is for ease of looking at the data, should not be in the production env
    df.toPandas().to_csv(output_paths[name]+".csv", header=True, index=False)

logger.info("Great success! Very nice!")


