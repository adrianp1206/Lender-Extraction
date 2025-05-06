#!/usr/bin/env python3
import pandas as pd
import glob
import os

def dedupe_validated(file_path):
    """
    Read the Excel file at file_path, remove duplicate lender names
    in the 'lender_name_validated' column (within each cell),
    and overwrite the file with the cleaned DataFrame.
    """
    df = pd.read_excel(file_path, engine="openpyxl")

    def _dedupe_cell(cell):
        if pd.isna(cell) or not isinstance(cell, str):
            return cell
        items = [item.strip() for item in cell.split(';') if item.strip()]
        seen = set()
        unique_items = []
        for item in items:
            if item not in seen:
                seen.add(item)
                unique_items.append(item)
        return '; '.join(unique_items)

    df['lender_name_validated'] = df['lender_name_validated'].apply(_dedupe_cell)
    df.to_excel(file_path, index=False, engine="openpyxl")
    print(f"✔ Processed: {os.path.basename(file_path)}")

def main():
    pattern = "extracted_lenders/extracted_lenders_*_updated.xlsx"
    files = sorted(glob.glob(pattern))
    if not files:
        print("No files found matching pattern:", pattern)
        return

    for file_path in files:
        dedupe_validated(file_path)

if __name__ == "__main__":
    main()
