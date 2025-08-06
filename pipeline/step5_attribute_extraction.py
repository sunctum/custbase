import re
from datetime import datetime
import logging
import pandas as pd
from utils.io import read_excel_file, save_to_excel_file
from utils.logging_utils import setup_logger


# --- Пути ---
INPUT_PATH = 'data/st4_branded/st4.xlsx'
OUTPUT_PATH = 'data/st5_attributes/st5.xlsx'

# --- Логгер ---
logger = setup_logger()
start_time = datetime.now()
logger.info('--- Step 5: Парсинг свойств ---')

DN_KEYS = ["ду", "dn", "дн", "диаметр", "внешний диаметр", "diameter", "du", "диаметром", "dn="]
MAT_KEYS = ["GGG40", "GG40", "нержавеющая сталь", "aisi 304", "aisi 316", "12х18н10т", "12x18h10t", "SS316", "сталь 20", "09г2с"]
PN_KEYS = ['ру', 'pn', 'давление', 'условное давление', 'pressure', 'nominal pressure', 'давлением', 'pn=']
PRODUCT_TYPE_KEYS = []

def parse_dn(text) -> str | None:
    if not isinstance(text, str):
        try:
            text = str(text)
        except:
            return None

    text = text.lower()
    for key in DN_KEYS:
        pattern = rf"{key}\s*[:\-]?\s*(\d{{1,3}})"
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def main():
    try:
        df = read_excel_file(INPUT_PATH)
        logger.info(f"📥 Прочитано: {INPUT_PATH} — {df.shape}")

        df["attribute_dn"] = df['prod_details'].apply(parse_dn)

        save_to_excel_file(df, OUTPUT_PATH)
        logger.info(f"📁 Сохранено: {OUTPUT_PATH}")

        end_time = datetime.now()
        logger.info(f'🕒 Продолжительность: {end_time - start_time}')

    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")

if __name__ == '__main__':
    main()
