import pandas as pd

XLSX = "data/raw/SUB-IP-EST2024-POP-06__B.xlsx"
OUT_CLEAN = "data/processed/population_ca_clean.csv"
OUT_LONG  = "data/processed/population_ca_long.csv"

# Read raw grid
raw = pd.read_excel(XLSX, header=None)

# Find row that contains the year headers (2020..2024)
target_years = {"2020", "2021", "2022", "2023", "2024"}
year_row = None

for i in range(min(80, len(raw))):
    row = ["" if pd.isna(x) else str(x).strip() for x in raw.iloc[i].tolist()]
    if target_years.issubset(set(row)):
        year_row = i
        break

if year_row is None:
    raise RuntimeError("Could not find the header row containing years 2020-2024.")

# The row above usually contains 'Geographic Area'
geo_row = year_row - 1

# Build column names:
# col0 = "place"
# col1 = "est_base" (optional)
# then the years
years = ["" if pd.isna(x) else str(x).strip() for x in raw.iloc[year_row].tolist()]

# Make sure first column exists
years[0] = "place"

# If second column is not a year, keep it as "base"
if len(years) > 1 and not str(years[1]).isdigit():
    years[1] = "base"

# Slice data starting after the year header row
df = raw.iloc[year_row + 1 :].copy()
df.columns = years

# Drop fully empty columns
df = df.dropna(axis=1, how="all")

# Keep only place + year columns
year_cols = [c for c in df.columns if str(c).isdigit()]
df = df[["place"] + year_cols]

# Clean
df["place"] = df["place"].astype(str).str.strip()
for c in year_cols:
    df[c] = pd.to_numeric(df[c], errors="coerce")

# Drop rows without place (end-of-table garbage)
df = df[df["place"].notna() & (df["place"] != "nan")]

# Save clean wide
df.to_csv(OUT_CLEAN, index=False)
print(f"Saved clean wide → {OUT_CLEAN}")
print("Columns:", df.columns.tolist())

# Wide -> long
long_df = df.melt(id_vars=["place"], var_name="year", value_name="population")
long_df["year"] = long_df["year"].astype(int)
long_df["population"] = long_df["population"].round(0).astype("Int64")

long_df.to_csv(OUT_LONG, index=False)
print(f"Saved long format → {OUT_LONG} ({len(long_df)} rows)")
