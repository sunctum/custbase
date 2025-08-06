import sys
from pathlib import Path

# pipeline в PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parent / "pipeline"))

from step1_preprocess import process_source, normalize_company_columns, COLUMNS_MAPS
from utils.io import save_to_excel_file

def run_single_file(xlsx_path: str):
    df = process_source("single_file", xlsx_path, COLUMNS_MAPS["rf_world"])
    df = normalize_company_columns(df)
    Path("data/st1_cleaned").mkdir(parents=True, exist_ok=True)
    save_to_excel_file(df, "data/st1_cleaned/st1.xlsx")


if __name__ == "__main__":
    run_single_file("data/raw/Импорт 8481808199, 7412200000, 8481808508, 8481806390, 8481806100, 8421210009_2024.xlsx")
