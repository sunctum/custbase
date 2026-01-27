#stepx_pipeline_on_1_file.py

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent / "pipeline"))

from pipeline.step1_preprocess import process_source, normalize_company_columns, COLUMNS_MAPS
from pipeline.utils.io import save_to_excel_file

def run_single_file(xlsx_path: str):
    df = process_source("single_file", xlsx_path, COLUMNS_MAPS["rf_world"], 0)
    df = normalize_company_columns(df)
    Path("data/st1_cleaned").mkdir(parents=True, exist_ok=True)
    save_to_excel_file(df, "data/st1_cleaned/st1.xlsx")


if __name__ == "__main__":
    run_single_file("data/raw/7307910000_2024_2025.xlsx")
