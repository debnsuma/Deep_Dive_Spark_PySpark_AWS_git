from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = (SparkSession
         .builder
         .appName("Problem 1")
         .getOrCreate()
         )

# Setting the log level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Path of the dataset for all text files
data_path = "s3://data-engg-suman/dataset/tempdata.csv"

# starting the rdd operations
rdd1 = sc.textFile(data_path)
rdd2 = rdd1.map(lambda x: x.split(','))
rdd3 = rdd2.filter(lambda x: x[2] == 'TMIN')
rdd4 = rdd3.map(lambda x: (x[0], int(x[3])))
rdd5 = rdd4.reduceByKey(min)

rdd5.foreach(print)


