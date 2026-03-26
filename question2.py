import pandas as pd 

df = pd.read_csv('tracks_cleaned.csv')

print(df.columns)

features = [
    'danceability', 'energy', 'acousticness', 'valence', 'tempo', 
    'loudness', 'speechiness', 'liveness'
]

results = {}
for decade in sorted(df['decade'].unique()):
    subset = df[df['decade'] == decade]

    corr = subset[features + ['popularity']].corr()['popularity'].drop('popularity')
    results[decade] = corr

corr_df = pd.DataFrame(results)
print(corr_df)