from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from loguru import logger

'''
* Implement data validation, cleaning procedures, error handling
* Target data models using your preferred dimensional modelling technique
* Create data model diagram
* Ingest files into target tables?

* Write code for:
	top 10 countries with the most number of customers
	Revenue distribution by country
	Relationship between average unit price of products and their sales volume
	Top 3 products with the maximum unit price drop in the last month
'''

# SparkSession is an entrance point for all spark operations
# Spark is case insensitive by default. This will sometimes cause 
# problems while reading data, as some attributes are poorly named
# and contain the same names but lower and upper case. this is especially
# problematic while handling json files
spark = SparkSession \
    .builder \
    .appName("ETL Stats") \
    .config("spark.sql.caseSensitive", True) \
    .getOrCreate()

# spark logging is set to INFO by default. This makes for a cumbersom log reading.
# Warn is enoug for debugging, but in normal operations we only really care 
# for errors
spark.sparkContext.setLogLevel("ERROR")
data_path = "/data/input/"

raw_paths = {
        "customers": data_path+"customers.csv",
        "orders": data_path+"orders.csv",
        "products": data_path+"products.csv"
        }

# the files we are provided contain a header row with names of the source columns
# we will use them to name columns in our datasets using header=true
load = spark.read.option("header", "true").csv

# having raw datasets in dictionary makes for easier iteration over all of them
dfs = {name: load(raw_path) for (name, raw_path) in raw_paths.items()}

for name,df in dfs.items():
    logger.info(name)
    df.show(5,0)



logger.info("Great success! Very nice!")


