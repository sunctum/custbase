import pandas as pd
from datetime import datetime
from thefuzz import process as fuzz_process
import logging
import re
from tqdm import tqdm
import psutil
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

start_time = datetime.now()
logger.info('Начало работы Step 4')

input_path = "data/st3_enriched/st3.xlsx"
output_path = "data/st4_branded/st4.xlsx"
brand_dict_path = "data/utilities/dict_brand.xlsx"
FUZZY_MIN_ALIAS_LEN = 3

def load_brand_aliases(excel_path: str) -> dict:
    df = pd.read_excel(excel_path)
    df = df.dropna(subset=['brand', 'aliases'])
    df['brand'] = df['brand'].str.strip().str.lower()

    alias_to_brand = {}

    for _, row in df.iterrows():
        brand = row['brand']
        aliases = [alias.strip().lower() for alias in str(row['aliases']).split(',')]
        for alias in aliases:
            if alias:  # Пропускаем пустые строки
                alias_to_brand[alias] = brand

    return alias_to_brand

def get_adaptive_threshold(token: str) -> int:
    length = len(token)
    if length <= 3:
        return 100  # Только точное совпадение
    elif length <= 5:
        return 97
    elif length <= 7:
        return 95
    else:
        return 90  # длина ≥ 8

def extract_brand_from_row(row: pd.Series, alias_to_brand: dict, fuzzy_keys: list[str]) -> tuple[str, list[str]]:
    search_fields = ['prod_brand', 'prod_man', 'exporter_name', 'prod_details']
    found = set()
    # 1. Быстрый проход, full match
    for field in search_fields:
        val = row.get(field)
        if pd.isna(val):
            continue
        val = str(val).lower()[:1000]  # ограничиваем длину строки
        for alias, brand in alias_to_brand.items():
            # Проверяем, что весь псевдоним совпадает с фразой целиком
            if alias in val:  # Исключаем частичные совпадения
                found.add(brand)

    if found:
        return (found.pop(), list(found)) if len(found) == 1 else ("смешанный", sorted(found))
    
    # 2. Медленный проход — только по prod_brand и prod_man
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

    if not found:
        return 'не определено'
    return (found.pop(), list(found)) if len(found) == 1 else ("смешанный", sorted(found))
    


def assign_brands(df: pd.DataFrame, alias_to_brand: dict) -> pd.DataFrame:
    df = df.copy()
    fuzzy_keys = [alias for alias in alias_to_brand.keys() if len(alias) >= FUZZY_MIN_ALIAS_LEN]
    tqdm.pandas(desc="🔍 Обработка строк")

    results = df.progress_apply(lambda row: extract_brand_from_row(row, alias_to_brand, fuzzy_keys), axis=1)
    df['brand_extracted'] = results.apply(lambda x: x[0])
    df['brand_candidates'] = results.apply(lambda x: ', '.join(x[1]) if x[1] else '')
    df['brand_mixed'] = df['brand_extracted'].apply(lambda x: x == 'смешанный')

    return df

try:
    alias_to_brand = load_brand_aliases(brand_dict_path)
    df = pd.read_excel(input_path)
    logger.info(f"📥 Данные загружены: {df.shape}")

    logger.info("⚙️  Начинается присвоение брендов...")
    process = psutil.Process()
    df_with_brands = assign_brands(df, alias_to_brand)

    mem_used = process.memory_info().rss / 1024 / 1024
    logger.info(f"📊 Использовано памяти: {mem_used:.1f} MB")

    df_with_brands.to_excel(output_path, index=False)
    logger.info(f"📁 Файл сохранён: {output_path}")

    end_time = datetime.now()
    logger.info(f'🕒 Продолжительность: {end_time - start_time}')
except Exception as e:
    logger.error(f"❌ Ошибка: {e}")
    logger.error(traceback.format_exc())