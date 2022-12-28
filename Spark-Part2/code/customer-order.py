from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark import StorageLevel

def lineparser(line):
    each_line = line.split(",")
    return each_line[0], float(each_line[2])

spark = (SparkSession
         .builder
         .appName("Customer Order")
         .config("spark.ui.port", "4050")
         .getOrCreate()
         )


# Setting the log level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

dataset_path = "s3://data-engg-suman/dataset/customerorders.csv"

rdd1 = sc.textFile(dataset_path)
rdd2 = rdd1.map(lineparser)
rdd3 = rdd2.reduceByKey(lambda x, y: x + y)

# Primum Customers who has spent more than 5000 and persist the output 
rdd4 = rdd3.filter(lambda x: x[1] > 5000).persist(storageLevel = StorageLevel.MEMORY_AND_DISK)

# Doubbling the amount for all customer 
rdd5 = rdd4.map(lambda x : (x[0], round(x[1]*2)))
rdd6 = rdd5.sortBy(lambda x: x[1], False)

result = rdd6.collect()

rdd6.foreach(print)

# print(result)

import sys

sys.stdin.readline()
