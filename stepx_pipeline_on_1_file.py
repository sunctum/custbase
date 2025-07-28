from pathlib import Path
from pipeline.step1_preprocess import process_source, normalize_company_columns, COLUMNS_MAPS
from utils.io import save_to_excel_file

def run_single_file(xlsx_path: str):
    df = process_source("single_file", xlsx_path, COLUMNS_MAPS["rf_world"])
    df = normalize_company_columns(df)
    Path("data/st1_cleaned").mkdir(parents=True, exist_ok=True)
    save_to_excel_file(df, "data/st1_cleaned/st1.xlsx")


if __name__ == "__main__":
    run_single_file("data/raw/Импорт 7307910000_2024.xlsx")
