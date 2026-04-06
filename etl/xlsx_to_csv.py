import os
import pandas as pd

RAW_DIR = "data/raw"

def convert_one_xlsx(path: str):
    # Read sheet without headers (raw grid)
    preview = pd.read_excel(path, header=None)

    # Find the row that contains "Geographic Area" (robust to NaN)
    header_row = None
    for i in range(min(50, len(preview))):
        row = preview.iloc[i].tolist()
        row_str = ["" if pd.isna(x) else str(x).strip().lower() for x in row]
        if any("geographic area" in cell for cell in row_str):
            header_row = i
            break

    if header_row is None:
        # helpful debug
        raise RuntimeError("Could not find a header row containing 'Geographic Area' in the first 50 rows.")

    # Read again using the detected header row
    df = pd.read_excel(path, header=header_row)

    # Drop fully empty columns
    df = df.dropna(axis=1, how="all")

    # Clean column names
    df.columns = [str(c).replace("\n", " ").strip() for c in df.columns]

    # Remove empty rows (where first column is missing)
    first_col = df.columns[0]
    df = df.dropna(subset=[first_col])

    out_path = path.replace(".xlsx", "_clean.csv")
    df.to_csv(out_path, index=False)
    print(f"Detected header row: {header_row}")
    print(f"Saved clean file → {out_path}")

def main():
    for file in os.listdir(RAW_DIR):
        if file.endswith(".xlsx"):
            path = os.path.join(RAW_DIR, file)
            print(f"Processing {file}")
            convert_one_xlsx(path)

if __name__ == "__main__":
    main()
