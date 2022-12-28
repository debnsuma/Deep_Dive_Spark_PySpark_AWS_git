from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark
import sys

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
rdd4 = rdd3.sortBy(lambda x: x[1], False)

result = rdd4.collect()

print(result)