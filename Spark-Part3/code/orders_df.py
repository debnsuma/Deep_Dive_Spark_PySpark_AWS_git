from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark
import sys

spark = (SparkSession
         .builder
         .appName("My Application")
         .config("spark.ui.port", "4050")
         .getOrCreate()
         )

dataset_path = "s3://data-engg-suman/dataset/orders.csv"

# Load the data (Read is an ACTION)
ordersdf = (spark
            .read
            .option("header", True)
            .option("inferSchema", True)
            .csv(dataset_path)
            )

# Print the first 10 lines 
ordersdf.show(10)

# Print the schema 
ordersdf.printSchema()
