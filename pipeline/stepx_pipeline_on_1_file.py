# single_file_run.py
from pipeline.step1_preprocess import process_rf_world
from pipeline import step2_tagging, step3_enrichment, step4_brand_extraction, step5_datamart
import pandas as pd
from pathlib import Path

def run_single_file(path_to_excel: str):
    df = process_rf_world(path_to_excel)
    df['exporter_name_orig'] = df['exporter_name']
    df['importer_name_orig'] = df['importer_name']

    # Нормализация как в step1
    from utils.normalization_utils import normalize_company
    for col in ['exporter_name', 'importer_name']:
        normalized = df[col].apply(normalize_company)
        df[f'{col}_opf'] = normalized['ОПФ']
        df[col] = normalized['Нормализованное_название']

    Path("data/st1_cleaned").mkdir(parents=True, exist_ok=True)
    df.to_excel("data/st1_cleaned/st1.xlsx", index=False)

    # Запуск последующих шагов
    step2_tagging.main()
    step3_enrichment.main()
    step4_brand_extraction.main()
    step5_datamart.main()

if __name__ == "__main__":
    run_single_file("data/raw/Импорт 7307910000_2024.xlsx")
