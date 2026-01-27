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

DN_KEYS = ["ду", "dn", "дн", "диаметр", "внешний диаметр", "diameter", "du", "диаметром", "dn=", "Ø", 'ду=', 'дн=']
PN_KEYS = ['ру', 'pn', 'давление', 'условное давление', 'pressure', 'nominal pressure', 'давлением', 'pn=', 'ру=']
MATERIAL_PATTERNS = {
    "Нержавеющая сталь": ["нержавеющая сталь", 'нержавеющей стали', 
                          "aisi 304", "aisi 316", 'aisi304', 'aisi316',
                          "12х18н10т", "12x18h10t",
                          '08Х18Н10', 
                          "ss316", 'ss304', 
                          'корозионностойкий', 'корозионностойкая', 'корозионностойкой',
                          'cf8m'],
    "Углеродистая сталь": ["сталь 20", 
                           "углеродистая сталь", "углеродистой стали"],
    "Легированная сталь": ["09г2с", 
                           "легированная сталь", "легированной стали"],
    "Чугун": ["чугун", "чугунный", "чугуна", 'чугунным', 'чуг',
              "ggg40", "gg40", 'gjs400', 'gs-400', 'ggg50', 'gg50', 'gg25'
              "32ч24р",
              'ci/cd']
}

PRODUCT_TYPE_PATTERNS = {
    "Трехэксцентриковый": ['трехэксцентриковый','треxэксцентриковые', 'треxэксцентриковый', 
                           'трехэксцентриковые', 'трехэкс', 'тройным эксцентриком', 'тройным эксцентриситетом', 
                           'тройным экcцентриситетом', 'тройным смещением диска', '3х-эксц', '3-Х ЭКСЦЕНТРИКОВЫЙ', 
                           'triple offset', 'triple eccentric', '3/эксц'],
    "Двухэксцентриковый": ['двухэксцентриковый', 'двухэксцентриковые', 'двухэкс', 'двойным эксцентриком', 
                           'двойным эксцентриситетом', 'двойным смещением диска', '2х-эксц', 'double offset', 
                           'double eccentric', '2/эксц'],
    "Безэксцентриковый": ['безэксцентриковый', 'безэкс', 'осевой', 'бабочка', 'без смещения диска', 'no offset', 'без эксцентриситета', 'центрический'],
    "Межфланцевый": ['межфланцевый', 'wafer', 'lug', '32ч24р', 'межфланцевые', 'межфланцевой', 'межфланц'],
}
SEAL_PATTERNS = {
    "Металл-Металл": ['металл по металлу', 'металл-металл', 'metal to metal'],
    "EPDM": ['epdm'],
    "ТРГ": ['трг', 'терморасширенный графит', 'teg'],
    "Резина": ['резина'],
    "NBR": ['nbr', 'каучук']
}

def parse_numeric_attribute(text: str, keys: list[str]) -> str | None:
    if not isinstance(text, str):
        try:
            text = str(text)
        except:
            return None
    text = text.lower()
    for key in keys:
        pattern = rf"{key}\s*[:\-]?\s*(\d{{1,4}})"
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None

def parse_from_patterns(text: str, patterns_dict: dict) -> str | None:
    """Поиск нормализованного значения по словарю синонимов."""
    if not isinstance(text, str):
        return None
    text = text.lower()
    for normalized_value, synonyms in patterns_dict.items():
        for synonym in synonyms:
            if synonym.lower() in text:
                return normalized_value
    return None

def main():
    try:
        df = read_excel_file(INPUT_PATH)
        logger.info(f"📥 Прочитано: {INPUT_PATH} — {df.shape}")

        df["attribute_dn"] = df['prod_details'].apply(lambda x: parse_numeric_attribute(x, DN_KEYS))
        df["attribute_pn"] = df['prod_details'].apply(lambda x: parse_numeric_attribute(x, PN_KEYS))
        df["attribute_material"] = df['prod_details'].apply(lambda x: parse_from_patterns(x, MATERIAL_PATTERNS))
        df["attribute_prodtype"] = df['prod_details'].apply(lambda x: parse_from_patterns(x, PRODUCT_TYPE_PATTERNS))
        df["attribute_sealing"] = df['prod_details'].apply(lambda x: parse_from_patterns(x, SEAL_PATTERNS))

        save_to_excel_file(df, OUTPUT_PATH)
        logger.info(f"📁 Сохранено: {OUTPUT_PATH}")

        end_time = datetime.now()
        logger.info(f'🕒 Продолжительность: {end_time - start_time}')

    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")

if __name__ == '__main__':
    main()
