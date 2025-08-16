import requests
import pandas as pd
from config import API_KEY

# TMDB API URL
url = f"https://api.themoviedb.org/3/movie/popular?api_key={API_KEY}&language=en-US&page=1"
response = requests.get(url)
data = response.json()

# Save JSON locally
with open("movies.json", "w") as f:
    f.write(str(data))

# Convert to CSV
movies = data['results']
df = pd.DataFrame(movies)
df.to_csv("movies.csv", index=False)

print("Data saved locally as movies.json and movies.csv")
