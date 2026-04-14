

import zipfile
import os
import io
import pandas as pd
from pathlib import Path

#inputs
data_folder   = Path("data")               
output_file   = Path("cabi_combined_data.parquet")  

excel_types = {".xlsx", ".xls", ".xlsm", ".xlsb"}
csv_types   = {".csv"}


def normalize_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce every object-dtype column to a uniform type so pyarrow can
    serialize it without type conflicts.
    """
    for col in df.select_dtypes(include=["object", "str"]).columns:
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.notna().sum() >= df[col].notna().sum():
            df[col] = numeric
        else:
            df[col] = df[col].where(df[col].isna(), df[col].astype(str))
    return df


def read_file_from_zip(zf: zipfile.ZipFile, name: str) -> pd.DataFrame | None:
    #read a csv from the zip
    suffix = Path(name).suffix.lower()
    try:
        with zf.open(name) as f:
            data = io.BytesIO(f.read())
            if suffix in csv_types:
                return pd.read_csv(data, low_memory=False)
            elif suffix in excel_types:
                return pd.read_excel(data)
    except Exception as e:
        print(f"Skipped '{name}': {e}")
    return None

# start main
def main():
    zip_paths = sorted(data_folder.glob("*.zip"))

    frames = []
    total_rows = 0

    for zip_path in zip_paths:
        print(f"Processing {zip_path.name}")
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                entries = [
                    n for n in zf.namelist()
                    if Path(n).suffix.lower() in csv_types | excel_types
                    and not Path(n).name.startswith(".")   # skip macOS metadata
                    and not Path(n).name.startswith("__")
                ]
                if not entries:
                    print("No file found")
                    continue
                for name in entries:
                    df = read_file_from_zip(zf, name)
                    if df is not None and not df.empty:
                        df["_source_zip"]  = zip_path.name
                        df["_source_file"] = Path(name).name
                        frames.append(df)
                        total_rows += len(df)
        except zipfile.BadZipFile:
            print(f"'{zip_path.name}' not a zip.")


    print("combining")
    combined = pd.concat(frames, ignore_index=True)

    print("normalizing")
    combined = normalize_dtypes(combined)

    print("writing")
    combined.to_parquet(output_file, index=False, engine="pyarrow", compression="snappy")



if __name__ == "__main__":
    main()
