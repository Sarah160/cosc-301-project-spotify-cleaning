import pandas as pd
import sqlite3

conn = sqlite3.connect("spotify.db")

query = """
SELECT 
    decade,
    danceability,
    energy,
    acousticness,
    valence,
    tempo,
    loudness,
    speechiness,
    liveness
FROM tracks
WHERE decade IS NOT NULL
"""

df = pd.read_sql(query, conn)

features = [
    'danceability', 'energy', 'acousticness', 'valence',
    'tempo', 'loudness', 'speechiness', 'liveness'
]

decade_means = df.groupby("decade")[features].mean()

print(decade_means)

decade_means.to_csv("feature_trends_by_decade.csv")

decade_means.to_sql(
    "question1_decade_trends",
    conn,
    if_exists="replace"
)