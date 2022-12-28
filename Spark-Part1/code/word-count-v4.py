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
rdd4 = rdd3.reduceByKey(lambda x, y : x + y)
rdd5 = rdd4.map(lambda x: (x[1], x[0])) 

# using sortByKey 
rdd6 = rdd5.sortByKey(False)

rdd6.saveAsTextFile("word-count-result-v4")


#sys.stdin.readline()