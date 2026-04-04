import pandas as pd
import sqlite3

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)

conn = sqlite3.connect("spotify.db")

query = """
SELECT 
    popularity,
    danceability,
    energy,
    acousticness,
    valence,
    tempo,
    loudness,
    speechiness,
    liveness
FROM tracks
WHERE popularity IS NOT NULL
"""

df = pd.read_sql(query, conn)

df["pop_group"] = df["popularity"].apply(
    lambda x: "high_popularity" if x >= 70 else "low_popularity"
)

features = [
    'danceability', 'energy', 'acousticness', 'valence',
    'tempo', 'loudness', 'speechiness', 'liveness'
]

popularity_comparison = df.groupby("pop_group")[features].mean()

print(popularity_comparison)

popularity_comparison.to_csv("popularity_feature_comparison.csv")

popularity_comparison.to_sql(
    "question1_popularity_comparison",
    conn,
    if_exists="replace"
)