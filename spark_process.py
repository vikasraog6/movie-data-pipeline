from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt

# Fix for macOS bind exception
spark = SparkSession.builder \
    .appName("MovieDataPipeline") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

# Load cleaned CSV
df_spark = spark.read.csv("hihg_rating_movies.csv", header=True, inferSchema=True)
df_spark.show(5)

# Top 10 highest-rated movies
top20 = df_spark.orderBy(df_spark.vote_average.desc()).limit(10)
top20.show()

# Average rating per year
avg_rating_per_year = df_spark.groupBy("release_year").avg("vote_average").orderBy("release_year")
avg_rating_per_year.show()

# Save results locally
df_spark.write.parquet("movies_cleaned.parquet", mode="overwrite")
top20.toPandas().to_csv("top20_movies.csv", index=False)
avg_rating_per_year.toPandas().to_csv("avg_rating_per_year.csv", index=False)

# Plotting
avg_df = pd.read_csv("avg_rating_per_year.csv")
plt.figure(figsize=(12,12))
plt.plot(avg_df['release_year'], avg_df['avg(vote_average)'], marker='o')
plt.title("Average Movie Ratings by Year")
plt.xlabel("Year")
plt.ylabel("Average Rating")
plt.grid(True)
plt.show()

top20_df = pd.read_csv("top_popular_movies.csv")
plt.figure(figsize=(12,12))
plt.barh(top20_df['title'], top20_df['vote_average'], color='skyblue')
plt.xlabel("Rating")
plt.title("Top 20 Highest-Rated Movies")
plt.gca().invert_yaxis()
plt.show()
