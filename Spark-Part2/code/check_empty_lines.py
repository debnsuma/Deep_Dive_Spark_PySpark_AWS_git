from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark
import sys

spark = (SparkSession
         .builder
         .appName("Customer Order")
         .config("spark.ui.port", "4050")
         .getOrCreate()
         )


def line_checker(line):
    if len(line) == 0:
        my_accumulator.add(1)

# Setting the log level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

dataset_path = "s3://data-engg-suman/dataset/samplefile"
my_accumulator = sc.accumulator(0)

rdd1 = sc.textFile(dataset_path)
rdd1.foreach(line_checker)

print(my_accumulator.value)

