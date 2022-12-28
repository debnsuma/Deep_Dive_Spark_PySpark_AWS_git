from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys 
def line_parser(line):
    return line.split(":")[0], 1

spark = (SparkSession
         .builder
         .appName("Log Analysis")
         .config("spark.ui.port", "4050")
         .getOrCreate()
         )


# Setting the log level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

dataset_path = "s3://data-engg-suman/dataset/bigLog.txt"

rdd1 = sc.textFile(dataset_path)
rdd2 = rdd1.map(line_parser)

result = rdd2.reduceByKey(lambda x, y : x + y)
print(result.collect())

# result = rdd2.countByValue()
# print(result, type(result))

# result = rdd2.groupByKey().mapValues(len).collect()
# print(result)

# sys.stdin.read()


