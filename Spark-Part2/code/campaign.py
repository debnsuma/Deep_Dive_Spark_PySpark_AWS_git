from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark
import sys

spark = (SparkSession
         .builder
         .appName("Movie Rating")
         .config("spark.ui.port", "4050")
         .getOrCreate()
         )


# Setting the log level
sc = spark.sparkContext
sc.setLogLevel("ERROR")

dataset_path = "s3://data-engg-suman/dataset/bigdatacampaigndata.csv"

rdd1 = sc.textFile(dataset_path)
rdd2 = rdd1.map(lambda x: (float(x.split(",")[10]), x.split(",")[0]))
words = rdd2.flatMapValues(lambda x: x.split(" "))
final_words = words.map(lambda x: (x[1], x[0]))
result = final_words.reduceByKey(lambda x, y: x + y)

sorted_result = result.sortBy(lambda x: x[1], False)

# final_result = sorted_result.collect()

sorted_result.take(5)
# Print the top 10 searched word 
print(sorted_result.take(5))