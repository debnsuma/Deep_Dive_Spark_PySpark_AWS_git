from pyspark.sql import SparkSession 
from pyspark.sql import functions as F 

spark = (SparkSession
            .builder
            .appName("Demo App")
            .config("spark.ui.port", "4050")
            .getOrCreate()
            )

# Setting the log level to ERROR 
spark.sparkContext.setLogLevel('ERROR')


# Dataset 
DATASET_PATH = 's3://data-engg-suman/dataset/book-1.txt' 

book = spark.read.text(DATASET_PATH)
lines = book.select(F.split(book.value, ' ').alias('line'))
words = lines.select(F.explode(F.col('line')).alias('word'))
words_lower = words.select(F.lower(F.col("word")).alias("word_lower"))
words_clean = words_lower.select(F.regexp_extract(F.col('word_lower'), '[a-z]*', 0).alias('word'))
words_nonull = words_clean.filter(F.col('word') != '')

groups = words_nonull.groupby(F.col('word'))
results = groups.count()
results_2 = results.orderBy(F.col('count').desc())


# Show
results_2.show()

