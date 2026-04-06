import pandas as pd

XLSX = "data/raw/SUB-IP-EST2024-POP-06__B.xlsx"
OUT_CLEAN = "data/processed/population_ca_clean.csv"
OUT_LONG  = "data/processed/population_ca_long.csv"

# Step 1: detect the header row containing "Geographic Area"
raw = pd.read_excel(XLSX, header=None)

header_row = None
for i in range(min(60, len(raw))):
    row = ["" if pd.isna(x) else str(x).strip().lower() for x in raw.iloc[i].tolist()]
    if any("geographic area" in cell for cell in row):
        header_row = i
        break

if header_row is None:
    raise RuntimeError("Could not find 'Geographic Area' header row.")

# Step 2: read from that header row
df = pd.read_excel(XLSX, header=header_row)

# Drop fully empty columns
df = df.dropna(axis=1, how="all")

# Clean column names
df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]

# Step 3: identify the geography column (first one usually)
geo_col = None
for c in df.columns:
    if "geographic" in c.lower() and "area" in c.lower():
        geo_col = c
        break
if geo_col is None:
    geo_col = df.columns[0]

df = df.rename(columns={geo_col: "place"})

# Step 4: keep only the first 7 columns (place + base + 2020..2024)
# This dataset should have exactly these columns in order.
df = df.iloc[:, :7].copy()

# Rename columns by position (most reliable)
df.columns = ["place", "base", "2020", "2021", "2022", "2023", "2024"]

# Clean rows
df["place"] = df["place"].astype(str).str.strip()
for y in ["base", "2020", "2021", "2022", "2023", "2024"]:
    df[y] = pd.to_numeric(df[y], errors="coerce")

df = df.dropna(subset=["place"])

# Save clean wide
df.to_csv(OUT_CLEAN, index=False)
print(f"Saved clean wide → {OUT_CLEAN}")
print(df.head(3))

# Wide -> long
long_df = df.melt(id_vars=["place"], value_vars=["2020","2021","2022","2023","2024"],
                  var_name="year", value_name="population")
long_df["year"] = long_df["year"].astype(int)
long_df["population"] = long_df["population"].round(0).astype("Int64")

long_df.to_csv(OUT_LONG, index=False)
print(f"Saved long format → {OUT_LONG} ({len(long_df)} rows)")

