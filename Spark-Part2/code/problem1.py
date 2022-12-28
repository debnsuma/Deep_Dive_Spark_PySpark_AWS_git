import os
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

# Data PATH
DATASET_PATH = 's3://data-engg-suman/dataset/'

# Loading the CHAPTER dataset (chapterId,courseId)
chapter_path = os.path.join(DATASET_PATH, 'chapters.csv')
chapter_details = sc.textFile(chapter_path)
chapter_details = chapter_details.map(lambda x: [int(i) for i in x.split(',')])

# Loading the VIEW dataset (userId,chapterId,dateAndTime)
view_path = os.path.join(DATASET_PATH, 'views?.csv')
view_details = sc.textFile(view_path)
view_details_2 = view_details.map(lambda x: tuple(x.split(',')[:2]))

# Loading the TITLE dataset (chapterId,Title)
title_path = os.path.join(DATASET_PATH, 'titles.csv')
title_details = sc.textFile(title_path)
title_details = title_details.map(lambda x: x.split(','))


# Exr 1 Solution
chapter_details_count = chapter_details.map(lambda x: (int(x[1]), 1))
chapter_details_summary = chapter_details_count.reduceByKey(lambda x,y: x+y).sortByKey()
print(chapter_details_summary.collect())

# Exr 2 Solution

view_details_swap = view_details_2.map(lambda x: (int(x[1]), int(x[0])))
view_details_distinct = view_details_swap.distinct()
view_chapID_userID_courseID = view_details_distinct.join(chapter_details).sortByKey()

# Drop the ChapterId
view_userID_courseID_1 = view_chapID_userID_courseID.map(lambda x: (x[1], 1))

# Count Views for User/Course
views_count_per_course = view_userID_courseID_1.reduceByKey(lambda x,y: x+y).sortByKey()

# Drop the userId
views_count_per_course_2 = views_count_per_course.map(lambda x: (x[0][1], x[1]))

# Join the chapterCountRDD with courseViewsCountRDD  to integrate total chapters in a course
newJoinedRDD = views_count_per_course_2.join(chapter_details_count)

# Calculating Percentage of course completion
course_completionpercentRDD = newJoinedRDD.mapValues(lambda x : float(x[0])/x[1])

# Map Percentages to Scores
def score_check(value):
    if value >= 0.9:
        result = 10
    elif value >= .5 and x < .9:
        result = 4
    elif value >= .25 and value > .5:
        result = 2
    else:
        result = 0
    return result

scoresRDD = course_completionpercentRDD.mapValues(score_check)

# Adding up the total scores for a course 
total_score = scoresRDD.reduceByKey(lambda x, y: x+y)

# Associate Titles with Courses and getting rid of courseIDs
total_score_2 = total_score.map(lambda x: (str(x[0]), x[1]))

title_score_joined_rdd = total_score_2.join(title_details)

result = title_score_joined_rdd.map(lambda x: (x[1][0], x[1][1], int(x[0]))).sortBy(lambda x: x[0])