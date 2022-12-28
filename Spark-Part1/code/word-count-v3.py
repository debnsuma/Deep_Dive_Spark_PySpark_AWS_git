from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark
import sys

spark = (SparkSession
         .builder
         .appName("Problem 1")
         .config("spark.ui.port", "4050")
         .getOrCreate()
         )


# Setting the log level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

dataset_path = "s3://data-engg-suman/dataset/search_data.txt"

rdd1 = sc.textFile(dataset_path)
rdd2 = rdd1.flatMap(lambda x: x.split())
rdd3 = rdd2.map(lambda x: (x.lower(),1))
result = rdd3.countByKey()   # this is an action and returns a python dict object


print(result)