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
data_path = "s3://data-engg-suman/dataset/dataset1"

# defining the condition function which will be used with the MAP() function
def add_flag(x):
    y = x[:]
    if int(y[1]) > 18:
        y.append('Y')
    else:
        y.append('N')
    return y

# starting the rdd operations
rdd1 = sc.textFile(data_path)
rdd2 = rdd1.map(lambda x: x.split(','))
rdd3 = rdd2.map(add_flag)
result = rdd3.collect()

for r in result:
    print(r)
