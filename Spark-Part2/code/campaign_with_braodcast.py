from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def load_boring_words():
    file_path = "Spark-Part2/dataset/boringwords"
    with open(file_path, "r") as f:
        boaring_words = f.readlines()
    boaring_words = { i.split("\n")[0] for i in boaring_words }
    return boaring_words

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

# Broadcast the varible to all executor 
broadcasting_var = sc.broadcast(load_boring_words())

rdd2 = rdd1.map(lambda x: (float(x.split(",")[10]), x.split(",")[0].lower()))
words = rdd2.flatMapValues(lambda x: x.split(" "))
final_words = words.map(lambda x: (x[1], x[0]))

# Adding the filter here and eliminating the boaring words 
filtered_words = final_words.filter(lambda x: x[0] not in broadcasting_var.value)

result = filtered_words.reduceByKey(lambda x, y: x + y)
sorted_result = result.sortBy(lambda x: x[1], False)

# final_result = sorted_result.collect()
sorted_result.take(5)

# Print the top 10 searched word 
print(sorted_result.take(5))