from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MoviePipeline") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

df_spark = spark.read.csv("top_popular_movies.csv", header=True, inferSchema=True)
df_spark.show(5)





