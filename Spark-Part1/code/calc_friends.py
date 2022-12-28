from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark
import sys

def lineparser(line):
    each_line = line.split("::")
    return each_line[2], (int(each_line[3]), 1)

spark = (SparkSession
         .builder
         .appName("Movie Rating")
         .config("spark.ui.port", "4050")
         .getOrCreate()
         )


# Setting the log level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

dataset_path = "s3://data-engg-suman/dataset/friendsdata.csv"

rdd1 = sc.textFile(dataset_path)
rdd2 = rdd1.map(lineparser)
rdd3 = rdd2.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
rdd4 = rdd3.mapValues(lambda x : (x[0]/x[1]))

result = rdd3.collect()

print(result)

