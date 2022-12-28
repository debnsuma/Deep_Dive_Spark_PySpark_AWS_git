from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark
import sys

def lineparser(line):
    each_line = line.split("\t")
    return each_line[2], 1

spark = (SparkSession
         .builder
         .appName("Movie Rating")
         .config("spark.ui.port", "4050")
         .getOrCreate()
         )


# Setting the log level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

dataset_path = "s3://data-engg-suman/dataset/moviedata.data"

rdd1 = sc.textFile(dataset_path)
rdd2 = rdd1.map(lineparser)
rdd3 = rdd2.reduceByKey(lambda x, y: x + y).sortByKey(False)

result = rdd3.collect()

print(result)

