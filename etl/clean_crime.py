import pandas as pd

INP = "data/raw/Crime_Data_from_2020_to_Present__A.csv"
OUT_EVENTS = "data/processed/crime_events_clean.csv"
OUT_LOC = "data/processed/dim_location_from_crime.csv"

df = pd.read_csv(INP)

# Parse date
df["DATE OCC"] = pd.to_datetime(
    df["DATE OCC"],
    format="%m/%d/%Y %I:%M:%S %p",
    errors="coerce"
)
df["year"] = df["DATE OCC"].dt.year

# Keep 2020+ only (should already be)
df = df[df["year"] >= 2020].copy()

events = df[[
    "DR_NO", "year", "AREA", "AREA NAME", "Crm Cd", "Crm Cd Desc", "LAT", "LON"
]].copy()

events = events.rename(columns={
    "DR_NO": "crime_id",
    "AREA": "area_code",
    "AREA NAME": "area_name",
    "Crm Cd": "crime_code",
    "Crm Cd Desc": "crime_desc",
    "LAT": "lat",
    "LON": "lon",
})

# Location dimension extracted from crime
loc = events[["area_code", "area_name"]].drop_duplicates().sort_values("area_code")
loc.to_csv(OUT_LOC, index=False)

events.to_csv(OUT_EVENTS, index=False)
print(f"Saved events → {OUT_EVENTS} ({len(events)} rows)")
print(f"Saved locations → {OUT_LOC} ({len(loc)} rows)")
print(loc.head(10))
