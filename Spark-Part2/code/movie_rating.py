'''
Q1: Find the top movies based on the following criteria:
    - At least 100 people should have rated the movie 
    - Avg Rating > 4.5 
'''
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
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

dataset_path_movie = "s3://data-engg-suman/dataset/movies.dat"
dataset_path_rating = "s3://data-engg-suman/dataset/ratings.dat"

ratings_rdd = sc.textFile(dataset_path_rating)
movie_rdd = sc.textFile(dataset_path_movie)

'''
1::1193::5::978300760
1::661::3::978302109
'''

# Get the movie id, userid and rating
def line_parser_movie(line):
    each_line = [ int(i) for i in line.split("::")]
    return each_line[1], each_line[2]

rdd1 = ratings_rdd.map(line_parser_movie) # Output : [(1193, 5), (661, 3)]
rdd2 = rdd1.map(lambda x: (x[0], (x[1], 1))) # Output : [(1193, (5, 1)), (661, (3, 1))]                                                  
rdd3 = rdd2.reduceByKey(lambda x, y : (x[0]+y[0], x[1]+y[1]))  # Output : [(914, (2642, 636)), (3408, (5081, 1315))]                                      
rdd4 = rdd3.map(lambda x: (x[0], (x[1][0], round(x[1][0]/x[1][1], 3)))) # Output : [(914, (2642, 4.154)), (3408, (5081, 3.864))]

def best_movie(movie):
    movie_id, no_people, avg_rating = movie[0], movie[1][0], movie[1][1] 
    if no_people >= 1000 and avg_rating >= 4.5:
        return True
    else:
        return False 

movie_id_rating_more_5 = rdd4.filter(best_movie)  # Output : [(318, 10143, 4.555), (50, 8054, 4.517)]


def line_parser_movie_name(line):
    each_line = line.split("::")
    return int(each_line[0]), each_line[1]

movie_names_rdd_1 = movie_rdd.map(line_parser_movie_name)

# Lets join this movie_names_rdd_1 and movie_id_rating_more_5 
movie_names_rdd_2 = movie_id_rating_more_5.join(movie_names_rdd_1)

print("*******************************************")
print("Top Movies based on the criteria mentioned")
print("*******************************************")
result = movie_names_rdd_2.collect()

for i in result:
    print(f"{i[1][1]}, {i[1][0][1]}")

print("*******************************************")

