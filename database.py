import pandas as pd 
import sqlite3

conn = sqlite3.connect("spotify.db")

df = pd.read_csv('tracks_cleaned.csv')

df.to_sql('tracks', conn, if_exists='replace', index=False)

print("Database created and data loaded successfully")

query = """
SELECT 
    decade,
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

print(df.head())