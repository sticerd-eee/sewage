import pandas as pd
import os
import re

# Base path (adjust as needed)
base_dir = "/Users/s.dhingra/Dropbox/sewage"

# Files to process: name → description
parquet_files = {
    "matched_events_annual_data": "Final merged dataset",
    "events_unmatched": "Unmatched individual events",
    "annual_unmatched": "Unmatched annual records",
    "site_metadata": "Site-level metadata"
}

# Cleaning function with logging
def clean_and_log(text, colname, rownum, log_list):
    if pd.isnull(text):
        return text
    original = str(text)
    cleaned = re.sub(r'[\U00010000-\U0010ffff]', '', original)  # emoji
    cleaned = re.sub(r'[\u2000-\u2BFF]', '', cleaned)           # symbols
    cleaned = re.sub(r'[\u0000-\u001F]', '', cleaned)           # control chars
    try:
        cleaned_encoded = cleaned.encode("latin-1", errors="ignore").decode("latin-1")
    except:
        cleaned_encoded = ""
    if cleaned_encoded != original:
        log_list.append(f"[{colname}][row {rownum}]: {original} → {cleaned_encoded}")
    return cleaned_encoded

# Loop through each file
for key in parquet_files:
    input_path = os.path.join(base_dir, f"data/processed/{key}.parquet")
    output_path = os.path.join(base_dir, f"raw_to_final/{key}.csv")
    log_path = os.path.join(base_dir, f"raw_to_final/{key}_cleaning_log.txt")

    if not os.path.exists(input_path):
        print(f"❌ File not found: {input_path}. Skipping.")
        continue

    print(f"🔄 Processing {key}.parquet")

    # Read parquet
    try:
        df = pd.read_parquet(input_path)
    except Exception as e:
        print(f"❌ Error reading {input_path}: {e}")
        continue

    # Remove timezone info from datetime columns
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    # Clean object columns
    log_entries = []
    for col in df.columns:
        if df[col].dtype == 'object':
            if df[col].isnull().all():
                df = df.drop(columns=[col])
            else:
                df[col] = df[col].astype(str)
                df[col] = [
                    clean_and_log(val, col, idx, log_entries)
                    for idx, val in enumerate(df[col])
                ]

    # Drop or fix 'tie' column if present
    if 'tie' in df.columns:
        try:
            df['tie'] = df['tie'].where(df['tie'].notnull(), None)
        except:
            df = df.drop(columns=['tie'])

    # Ensure output folder exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write CSV
    try:
        df.to_csv(output_path, index=False, encoding="utf-8")
        print(f"✅ Exported CSV to {output_path}")
    except Exception as e:
        print(f"❌ Failed to write CSV for {key}: {e}")
        continue

    # Write log
    with open(log_path, "w", encoding="utf-8") as log_file:
        log_file.write("\n".join(log_entries))
    print(f"📝 Cleaning log written to {log_path}")
