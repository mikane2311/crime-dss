import pandas as pd

INP = "data/raw/laborforceandunemployment_monthly_2026213__C.csv"
OUT = "data/processed/unemployment_ca_yearly.csv"

df = pd.read_csv(INP)

# Keep California state, not seasonally adjusted
df = df[
    (df["Area Name"] == "California") &
    (df["Area Type"] == "State") &
    (df['Seasonally Adjusted(Y/N)'] == "N")
].copy()

df["Year"] = pd.to_numeric(df["Year"], errors="coerce")

# Convert unemployment rate to numeric
df["Unemployment Rate"] = pd.to_numeric(df["Unemployment Rate"], errors="coerce")

# Annual average
yearly = (
    df.groupby("Year", as_index=False)["Unemployment Rate"]
      .mean()
      .rename(columns={"Year": "year", "Unemployment Rate": "unemployment_rate"})
)

# Keep only years relevant to crime dataset (2020+)
yearly = yearly[yearly["year"] >= 2020].copy()

yearly.to_csv(OUT, index=False)
print(f"Saved → {OUT}")
print(yearly.head(10))
