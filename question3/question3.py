# Question 3 Analysis:
# This script examines whether an artist's follower count is related to how many tracks they release per year.
# Tracks are grouped by artist and year to calculate annual productivity.
# The data is then merged with artist follow counts.
# A correlation analysis is performed to evaluate between productivity and popularity.

import pandas as pd
import sqlite3

conn = sqlite3.connect("spotify.db")

tracks = pd.read_csv("tracks_cleaned.csv")
artists = pd.read_csv("artists_cleaned.csv")

track_counts = (
    tracks.groupby(["id_artists", "year"])
    .size()
    .reset_index(name="tracks_released")
)

track_counts["id_artists"] = track_counts["id_artists"].str.replace("[", "", regex=False)
track_counts["id_artists"] = track_counts["id_artists"].str.replace("]", "", regex=False)
track_counts["id_artists"] = track_counts["id_artists"].str.replace("'", "", regex=False)


merged = pd.merge(
    track_counts,
    artists[["id", "followers", "name"]],
    left_on="id_artists",
    right_on="id",
    how="inner"
)

correlation = merged["tracks_released"].corr(merged["followers"])
print("Correlation between tracks released and followers:", correlation)

print(merged[["name", "year", "tracks_released", "followers"]].head(10))
  #create the final merge table
merged.to_sql("question3_analysis", conn, if_exists="replace", index=False)
merged.to_csv("question3_output.csv", index=False)


conn.close()
