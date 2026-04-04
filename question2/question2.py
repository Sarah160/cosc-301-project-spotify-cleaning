
import pandas as pd
import sqlite3

conn = sqlite3.connect("spotify.db")

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

features = [
    'danceability', 'energy', 'acousticness', 'valence', 'tempo',
    'loudness', 'speechiness', 'liveness'
]

results = {}
for decade in sorted(df['decade'].dropna().unique()):
    subset = df[df['decade'] == decade]
    corr = subset[features + ['popularity']].corr()['popularity'].drop('popularity')
    results[decade] = corr

corr_df = pd.DataFrame(results)
print(corr_df)

corr_df.to_csv("correlation_results.csv", index=True)

conn.close()
