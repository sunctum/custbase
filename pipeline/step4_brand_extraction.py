# steps/step4_branding.py

import pandas as pd
import re
from datetime import datetime
from thefuzz import process as fuzz_process
from tqdm import tqdm
import logging
import traceback
import psutil

from utils.io import read_excel_file, save_to_excel_file
from utils.logging_utils import setup_logger

# --- Логгер ---
logger = setup_logger()
start_time = datetime.now()
logger.info('--- Step 4: Определение брендов ---')

# --- Пути и параметры ---
INPUT_PATH = "data/st3_enriched/st3.xlsx"
OUTPUT_PATH = "data/st4_branded/st4.xlsx"
BRAND_DICT_PATH = "data/utilities/dict_brand.xlsx"
FUZZY_MIN_ALIAS_LEN = 3

# ---------------------------- ФУНКЦИИ ---------------------------- #

def load_brand_aliases(excel_path: str) -> dict:
    df = pd.read_excel(excel_path)
    df = df.dropna(subset=['brand', 'aliases'])
    df['brand'] = df['brand'].str.strip().str.lower()

    alias_to_brand = {}
    for _, row in df.iterrows():
        brand = row['brand']
        aliases = [alias.strip().lower() for alias in str(row['aliases']).split(',')]
        for alias in aliases:
            if alias:
                alias_to_brand[alias] = brand
    return alias_to_brand

def get_adaptive_threshold(token: str) -> int:
    length = len(token)
    if length <= 3:
        return 100
    elif length <= 5:
        return 97
    elif length <= 7:
        return 95
    else:
        return 90

def extract_brand_from_row(row: pd.Series, alias_to_brand: dict, fuzzy_keys: list[str]) -> tuple[str, list[str], str]:
    try:
        search_fields = ['prod_brand', 'prod_man', 'exporter_name', 'prod_details']
        found = set()
        column_reasons = []

        # --- Быстрый проход ---
        for field in search_fields:
            val = row.get(field)
            if pd.isna(val):
                continue
            val = str(val).lower()[:1000]
            for alias, brand in alias_to_brand.items():
                alias_words = alias.split()
                if all(re.search(r'\b' + re.escape(word) + r'\b', val) for word in alias_words):
                    found.add(brand)
                    column_reasons.append(field)

        if found:
            return (
                found.pop() if len(found) == 1 else "смешанный",
                sorted(found),
                ', '.join(column_reasons)
            )

        # --- Медленный fuzzy matching ---
        for field in ['prod_brand', 'prod_man']:
            val = row.get(field)
            if pd.isna(val):
                continue
            tokens = re.findall(r'\b\w{3,}\b', str(val).lower())
            for token in tokens:
                threshold = get_adaptive_threshold(token)
                result = fuzz_process.extractOne(token, fuzzy_keys, score_cutoff=threshold)
                if result:
                    match, _ = result
                    found.add(alias_to_brand[match])
                    column_reasons.append(field)

        if not found:
            return 'не определено', [], ', '.join(column_reasons)

        return (
            found.pop() if len(found) == 1 else "смешанный",
            sorted(found),
            ', '.join(column_reasons)
        )

    except Exception as e:
        logger.error(f"Ошибка при обработке строки: {e}")
        return 'не определено', [], ''

def assign_brands(df: pd.DataFrame, alias_to_brand: dict) -> pd.DataFrame:
    df = df.copy()
    fuzzy_keys = [alias for alias in alias_to_brand.keys() if len(alias) >= FUZZY_MIN_ALIAS_LEN]
    tqdm.pandas(desc="🔍 Поиск брендов")

    logger.info("▶️ Обработка строк...")
    results = df.progress_apply(lambda row: extract_brand_from_row(row, alias_to_brand, fuzzy_keys), axis=1)
    logger.info("✅ Обработка завершена")

    df['brand_extracted'] = results.apply(lambda x: x[0])
    df['brand_candidates'] = results.apply(lambda x: ', '.join(x[1]) if x[1] else '')
    df['brand_mixed'] = df['brand_extracted'].apply(lambda x: x == 'смешанный')
    df['brand_column_reason'] = results.apply(lambda x: x[2])

    return df

# ------------------------- ОСНОВНОЙ БЛОК -------------------------- #

def main():
    try:
        alias_to_brand = load_brand_aliases(BRAND_DICT_PATH)
        df = read_excel_file(INPUT_PATH)
        logger.info(f"📥 Прочитано: {INPUT_PATH} — {df.shape}")

        df_with_brands = assign_brands(df, alias_to_brand)

        mem_used = psutil.Process().memory_info().rss / 1024 / 1024
        logger.info(f"📊 Использовано памяти: {mem_used:.1f} MB")

        save_to_excel_file(df_with_brands, OUTPUT_PATH)
        logger.info(f"📁 Сохранено: {OUTPUT_PATH}")

        end_time = datetime.now()
        logger.info(f'🕒 Продолжительность: {end_time - start_time}')

    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        logger.error(traceback.format_exc())

if __name__ == '__main__':
    main()
