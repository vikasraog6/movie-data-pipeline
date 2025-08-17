from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder \
    .appName("MoviePipeline") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Read the CSVs
df_top = spark.read.csv("top_popular_movies.csv", header=True, inferSchema=True)
df_avg = spark.read.csv("avg_rating_per_year.csv", header=True, inferSchema=True)
df_high = spark.read.csv("high_rating_movies.csv", header=True, inferSchema=True)

# Example: join top movies with average rating per year
df_top.createOrReplaceTempView("top_movies")
df_avg.createOrReplaceTempView("avg_ratings")

final_df = spark.sql("""
SELECT t.title, t.release_year, a.`avg(vote_average)` as avg_rating
FROM top_movies t
JOIN avg_ratings a
ON t.release_year = a.release_year
ORDER BY avg_rating DESC
""")

# Save output as single CSV
final_df.coalesce(1).write.csv("final_output.csv", header=True, mode="overwrite")

# Show top 5 rows
final_df.show(5)

# Stop Spark
spark.stop()
