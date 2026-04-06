import pandas as pd

XLSX = "data/raw/SUB-IP-EST2024-POP-06__B.xlsx"
OUT_CLEAN = "data/processed/population_ca_clean.csv"
OUT_LONG  = "data/processed/population_ca_long.csv"

# Read multi-row header (2 rows), skip title lines above
df = pd.read_excel(XLSX, header=[0, 1], skiprows=3)

# Drop fully empty columns
df = df.dropna(axis=1, how="all")

# Flatten multiindex columns
def flatten(col):
    a, b = col
    a = "" if pd.isna(a) else str(a).strip()
    b = "" if pd.isna(b) else str(b).strip()
    # If second level is a year -> use year
    if b.isdigit():
        return b
    # Otherwise prefer a (top header); fallback to b
    return a if a else b

df.columns = [flatten(c) for c in df.columns]

# --- NEW: detect geography column automatically ---
cols_lower = {c.lower(): c for c in df.columns}
geo_candidates = [c for c in df.columns if any(k in c.lower() for k in ["geographic", "area", "name", "place"])]
if not geo_candidates:
    raise RuntimeError(f"Could not find geography column. Columns found: {list(df.columns)}")

geo_col = geo_candidates[0]  # first candidate is usually correct
df = df.rename(columns={geo_col: "place"})

# Detect year columns
year_cols = [c for c in df.columns if str(c).isdigit()]
if not year_cols:
    raise RuntimeError(f"Could not find year columns. Columns found: {list(df.columns)}")

# Keep only place + year columns
df = df[["place"] + year_cols]

# Clean values
df["place"] = df["place"].astype(str).str.strip()
for c in year_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# Save clean wide
df.to_csv(OUT_CLEAN, index=False)
print(f"Saved clean wide → {OUT_CLEAN}")
print("Columns:", df.columns.tolist()[:10])

# Convert to long format
long_df = df.melt(id_vars=["place"], var_name="year", value_name="population")
long_df["year"] = long_df["year"].astype(int)
long_df["population"] = long_df["population"].round(0).astype("Int64")

long_df.to_csv(OUT_LONG, index=False)
print(f"Saved long format → {OUT_LONG} ({len(long_df)} rows)")
