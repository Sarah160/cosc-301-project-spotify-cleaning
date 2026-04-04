# cosc-301-project-spotify-cleaning

Hits Through the Decades: Unpacking What Makes Music Popular
This project analyzes how the audio characteristics of popular music have evolved across decades and investigates the factors that contribute to track and artist success. Using the Spotify dataset (1920–2020) sourced from Kaggle, we examine key audio features — including danceability, energy, acousticness, valence, tempo, loudness, speechiness, and liveness — to identify long-term trends and their relationship with track popularity. The analysis also explores whether artist productivity, measured by annual track releases, is associated with greater audience reach as proxied by follower count.

The dataset can be downloaded from Kaggle using the following command:

kaggle datasets download yamaerenay/spotify-dataset-19212020-600k-tracks

Or download the data here: https://www.kaggle.com/datasets/yamaerenay/spotify-dataset-19212020-600k-tracks?select=tracks.csv


Raw files (tracks.csv and artists.csv) are preserved in the raw/ folder, with all cleaning and transformations applied to separate copies saved in the cleaned/ folder. The cleaned data is stored in a SQLite database (spotify.db) for querying and reuse across all three analyses. Visualizations were built using Tableau, and all Python scripts used for cleaning, analysis, and database management are included in this repository.


Authors: Sarah Adelaja, Chantal Bonvie, Sofia Atilano Larios
Course: COSC 301 — Introduction to Data Analytics, The University of British Columbia Okanagan
