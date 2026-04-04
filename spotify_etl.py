import pandas as pd
import numpy as np
import sqlite3
import shutil
import os
import json
from datetime import datetime


INPUT_DIR   = "."                  
RAW_DIR     = "raw"
CLEANED_DIR = "cleaned"
DB_PATH     = os.path.join(CLEANED_DIR, "spotify.db")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CLEANED_DIR, exist_ok=True)

print("=" * 60)
print("STEP 1 — Preserving raw files...")
for fname in ["tracks.csv", "artists.csv"]:
    src = os.path.join(INPUT_DIR, fname)
    dst = os.path.join(RAW_DIR, fname)
    if os.path.exists(src) and not os.path.exists(dst):
        shutil.copy2(src, dst)
        print(f"  ✓ Copied {fname} → raw/")
    elif os.path.exists(dst):
        print(f"  ✓ raw/{fname} already exists, skipping copy.")
    else:
        print(f"  ✗ WARNING: {fname} not found in {INPUT_DIR}")


print("\nSTEP 2 — Loading CSVs...")
tracks  = pd.read_csv(os.path.join(INPUT_DIR, "tracks.csv"), low_memory=False)
artists = pd.read_csv(os.path.join(INPUT_DIR, "artists.csv"), low_memory=False)

print(f"  tracks  shape: {tracks.shape}")
print(f"  artists shape: {artists.shape}")


print("\nSTEP 3 — Cleaning tracks.csv...")
report = {}  


n_before = len(tracks)
tracks.drop_duplicates(subset="id", keep="first", inplace=True)
n_dupes = n_before - len(tracks)
print(f"  Duplicates removed (by id): {n_dupes}")
report["tracks_duplicates_removed"] = n_dupes

missing_before = tracks.isnull().sum()
print(f"\n  Missing values (before):\n{missing_before[missing_before > 0].to_string()}")

audio_features = [
    "danceability", "energy", "loudness", "speechiness",
    "acousticness", "instrumentalness", "liveness", "valence",
    "tempo", "key", "mode", "time_signature"
]
imputed_cols = []
for col in audio_features:
    if col in tracks.columns and tracks[col].isnull().any():
        med = tracks[col].median()
        tracks[col].fillna(med, inplace=True)
        imputed_cols.append((col, med))
        print(f"  Imputed '{col}' with median={med:.4f}")

if tracks["popularity"].isnull().any():
    med = tracks["popularity"].median()
    tracks["popularity"].fillna(med, inplace=True)
    print(f"  Imputed 'popularity' with median={med}")

if tracks["duration_ms"].isnull().any():
    med = tracks["duration_ms"].median()
    tracks["duration_ms"].fillna(med, inplace=True)

n_before = len(tracks)
tracks.dropna(subset=["name", "release_date"], inplace=True)
n_dropped = n_before - len(tracks)
print(f"  Rows dropped (missing name or release_date): {n_dropped}")
report["tracks_rows_dropped_missing_text"] = n_dropped


def extract_year(val):
    try:
        return int(str(val)[:4])
    except:
        return np.nan

tracks["year"]   = tracks["release_date"].apply(extract_year).astype("Int64")
tracks["decade"] = (tracks["year"] // 10 * 10).astype("Int64")

print(f"  Year range: {tracks['year'].min()} – {tracks['year'].max()}")

tracks["duration_min"] = (tracks["duration_ms"] / 60000).round(3)

tracks["explicit"] = tracks["explicit"].astype(bool)

tracks["id"]          = tracks["id"].astype(str)
tracks["name"]        = tracks["name"].astype(str)
tracks["popularity"]  = pd.to_numeric(tracks["popularity"], errors="coerce").fillna(0).astype(int)
tracks["key"]         = pd.to_numeric(tracks["key"],        errors="coerce").fillna(-1).astype(int)
tracks["mode"]        = pd.to_numeric(tracks["mode"],       errors="coerce").fillna(0).astype(int)
tracks["time_signature"] = pd.to_numeric(tracks["time_signature"], errors="coerce").fillna(4).astype(int)


print("\nSTEP 4 — Flagging outliers (IQR method)...")

def iqr_flag(df, col):
    Q1  = df[col].quantile(0.25)
    Q3  = df[col].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    flag  = ~df[col].between(lower, upper)
    pct   = flag.sum() / len(df) * 100
    print(f"  {col}: Q1={Q1:.2f}, Q3={Q3:.2f}, IQR={IQR:.2f} → "
          f"bounds=[{lower:.2f}, {upper:.2f}] → {flag.sum()} outliers ({pct:.1f}%)")
    return flag, lower, upper

outlier_meta = {}
for col in ["tempo", "loudness", "duration_ms"]:
    flag, lo, hi = iqr_flag(tracks, col)
    tracks[f"outlier_{col}"] = flag
    outlier_meta[col] = {"lower_bound": round(lo, 3), "upper_bound": round(hi, 3),
                          "n_outliers": int(flag.sum())}

report["outlier_meta"] = outlier_meta


print("\nSTEP 5 — Cleaning artists.csv...")

n_before = len(artists)
artists.drop_duplicates(subset="id", keep="first", inplace=True)
print(f"  Duplicates removed (by id): {n_before - len(artists)}")

artists["followers"] = pd.to_numeric(artists["followers"], errors="coerce").fillna(0).astype("Int64")

artists["popularity"] = pd.to_numeric(artists["popularity"], errors="coerce").fillna(0).astype(int)

def parse_genres(val):
    """Convierte string de lista de géneros a lista Python o []"""
    if pd.isna(val) or val in ("[]", ""):
        return []
    try:
        cleaned = val.replace("'", '"')
        return json.loads(cleaned)
    except:
        return []

artists["genres_parsed"] = artists["genres"].apply(parse_genres)
artists["genres_count"]  = artists["genres_parsed"].apply(len)

print(f"  artists final shape: {artists.shape}")


print("\nSTEP 6 — Saving cleaned CSVs...")

tracks_out  = os.path.join(CLEANED_DIR, "tracks_cleaned.csv")
artists_out = os.path.join(CLEANED_DIR, "artists_cleaned.csv")

tracks.to_csv(tracks_out,  index=False)
artists.to_csv(artists_out, index=False)

print(f"  ✓ {tracks_out}  ({len(tracks):,} rows)")
print(f"  ✓ {artists_out} ({len(artists):,} rows)")


print(f"\nSTEP 7 — Loading into SQLite: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)

tracks_sql = tracks.drop(columns=["artists", "id_artists"], errors="ignore")
tracks_sql.to_sql("tracks", conn, if_exists="replace", index=False)

artists_sql = artists.drop(columns=["genres_parsed"], errors="ignore")
artists_sql.to_sql("artists", conn, if_exists="replace", index=False)

cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM tracks")
print(f"  ✓ tracks table: {cursor.fetchone()[0]:,} rows")
cursor.execute("SELECT COUNT(*) FROM artists")
print(f"  ✓ artists table: {cursor.fetchone()[0]:,} rows")

cursor.execute("CREATE INDEX IF NOT EXISTS idx_tracks_year ON tracks(year)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_tracks_popularity ON tracks(popularity)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_tracks_decade ON tracks(decade)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_artists_popularity ON artists(popularity)")
conn.commit()
conn.close()
print("  ✓ Indexes created")

print("\nSTEP 8 — Generating data dictionary...")

tracks_stats  = tracks[[
    "danceability","energy","loudness","speechiness","acousticness",
    "instrumentalness","liveness","valence","tempo","duration_ms",
    "duration_min","popularity","year"
]].describe().round(4)

artists_stats = artists[["followers","popularity","genres_count"]].describe().round(4)

now = datetime.now().strftime("%Y-%m-%d %H:%M")

dict_md = f"""# Spotify Dataset — Data Dictionary
*Generated: {now}*

---

## tracks_cleaned.csv

| Field | Type | Range / Values | Transformation Applied |
|-------|------|---------------|----------------------|
| id | str | Spotify URI (22 chars) | None; used as dedup key |
| name | str | — | Rows with missing name dropped |
| popularity | int | 0–100 | Median-imputed if null |
| duration_ms | int | ms | Median-imputed if null; outliers flagged |
| duration_min | float | minutes | Derived: duration_ms / 60000 |
| explicit | bool | True/False | Cast from int (0/1) |
| artists | str | Python list as string | Kept as-is (excluded from SQLite) |
| id_artists | str | Python list as string | Kept as-is (excluded from SQLite) |
| release_date | str | YYYY or YYYY-MM-DD | Original preserved |
| year | Int64 | {int(tracks['year'].min())}–{int(tracks['year'].max())} | Extracted from release_date[:4] |
| decade | Int64 | {int(tracks['decade'].min())}–{int(tracks['decade'].max())} | Derived: year // 10 * 10 |
| danceability | float | 0.0–1.0 | Median-imputed if null |
| energy | float | 0.0–1.0 | Median-imputed if null |
| key | int | 0–11 (pitch class) | Cast to int; -1 if missing |
| loudness | float | dB (typically -60–0) | Median-imputed; outliers flagged |
| mode | int | 0=minor, 1=major | Cast to int |
| speechiness | float | 0.0–1.0 | Median-imputed if null |
| acousticness | float | 0.0–1.0 | Median-imputed if null |
| instrumentalness | float | 0.0–1.0 | Median-imputed if null |
| liveness | float | 0.0–1.0 | Median-imputed if null |
| valence | float | 0.0–1.0 (sad→happy) | Median-imputed if null |
| tempo | float | BPM | Median-imputed; outliers flagged |
| time_signature | int | 1–5 | Cast to int; 4 if missing |
| outlier_tempo | bool | True=outlier | IQR method: bounds [{outlier_meta['tempo']['lower_bound']}, {outlier_meta['tempo']['upper_bound']}] — {outlier_meta['tempo']['n_outliers']:,} flagged |
| outlier_loudness | bool | True=outlier | IQR method: bounds [{outlier_meta['loudness']['lower_bound']}, {outlier_meta['loudness']['upper_bound']}] — {outlier_meta['loudness']['n_outliers']:,} flagged |
| outlier_duration_ms | bool | True=outlier | IQR method: bounds [{outlier_meta['duration_ms']['lower_bound']}, {outlier_meta['duration_ms']['upper_bound']}] — {outlier_meta['duration_ms']['n_outliers']:,} flagged |

### tracks numeric summary
```
{tracks_stats.to_string()}
```

---

## artists_cleaned.csv

| Field | Type | Range / Values | Transformation Applied |
|-------|------|---------------|----------------------|
| id | str | Spotify URI | Used as dedup key |
| name | str | — | None |
| followers | Int64 | 0–{int(artists['followers'].max()):,} | Cast from float; 0 if missing |
| genres | str | Python list string | Original preserved |
| genres_parsed | list | List of genre strings | Parsed from string (not in SQLite) |
| genres_count | int | 0–N | Derived: len(genres_parsed) |
| popularity | int | 0–100 | Cast to int; 0 if missing |

### artists numeric summary
```
{artists_stats.to_string()}
```

---

## ETL Summary

| Metric | Value |
|--------|-------|
| tracks duplicates removed | {report['tracks_duplicates_removed']:,} |
| tracks rows dropped (missing name/date) | {report['tracks_rows_dropped_missing_text']:,} |
| tracks final row count | {len(tracks):,} |
| artists final row count | {len(artists):,} |
| outlier_tempo flagged | {outlier_meta['tempo']['n_outliers']:,} |
| outlier_loudness flagged | {outlier_meta['loudness']['n_outliers']:,} |
| outlier_duration_ms flagged | {outlier_meta['duration_ms']['n_outliers']:,} |
| SQLite DB | cleaned/spotify.db |

---

## Outlier Methodology
Outliers detected using the **IQR method** (Tukey fences): a value is flagged when it falls
outside [Q1 − 1.5×IQR, Q3 + 1.5×IQR]. Outliers are **flagged but NOT removed** — the boolean
columns `outlier_tempo`, `outlier_loudness`, and `outlier_duration_ms` let downstream
analysis filter or study them separately.

## Imputation Rationale
**Median imputation** (not mean) was chosen for all numeric audio features and popularity
because these distributions are often skewed (e.g. instrumentalness is heavily right-skewed).
Median is more robust to those extremes.

## Folder Structure
```
project/
├── raw/
│   ├── tracks.csv          
│   └── artists.csv         
├── cleaned/
│   ├── tracks_cleaned.csv
│   ├── artists_cleaned.csv
│   └── spotify.db          
├── data_dictionary.md      
└── spotify_etl.py          
```
"""

dict_path = "data_dictionary.md"
with open(dict_path, "w", encoding="utf-8") as f:
    f.write(dict_md)

print(f"  ✓ {dict_path} written")


print("\n" + "=" * 60)
print("ETL COMPLETE ✓")
print(f"  cleaned/tracks_cleaned.csv  — {len(tracks):,} tracks")
print(f"  cleaned/artists_cleaned.csv — {len(artists):,} artists")
print(f"  cleaned/spotify.db")
print(f"  data_dictionary.md")
print("=" * 60)
