import pandas as pd
from datetime import datetime
import os
import logging
from utils.normalization_utils import normalize_company

# === Logging Setup ===
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

logger = setup_logging()
start_time = datetime.now()
logger.info('Начало работы Step 1')

# === Constants ===
INPUT_PATHS = {
    "eau": "data/raw/EAU.xlsx",
    "atlas": "data/raw/atlas.xlsx",
    "rf_world": "data/raw/rf_world_exp_2025.xlsx"
}
OUTPUT_PATH = "data/st1_cleaned/st1.xlsx"

# === Utility Functions ===
def read_excel_file(path, sheet_name='Sheet1', parse_dates=None):
    return pd.read_excel(path, sheet_name=sheet_name, parse_dates=parse_dates)

def insert_source_column(df, source_name):
    df.insert(0, 'source', source_name)
    return df

def rename_and_filter(df, columns_map):
    return df[list(columns_map.keys())].rename(columns=columns_map)

def normalize_dates(df, column, fmt='%d.%m.%Y'):
    df[column] = pd.to_datetime(df[column], format=fmt, errors='coerce')
    return df

# === Step 1: Read and Clean DataFrames ===
def process_rf_world(path):
    logger.info('Читается РФ-Мир')
    df = read_excel_file(path)
    df = normalize_dates(df, 'G072 (Дата регистрации)')
    columns_map = {
        'ND (Номер декларации)': 'decl_number',
        'G072 (Дата регистрации)': 'decl_date',
        'G021 (ИНН отправителя)': 'exporter_tin',
        'G022 (Наименование отправителя)': 'exporter_name',
        'G023 (Адрес отправителя)': 'exporter_address',
        'G0231 (Код страны отправителя)': 'exporter_country',
        'G081 (ИНН получателя)': 'importer_tin',
        'G082 (Наименование получателя)': 'importer_name',
        'G0831 (Код страны получателя)': 'importer_country',
        'G083 (Адрес получателя)': 'importer_address',
        'G31_1 (Наименование и характеристики товаров)': 'prod_details',
        'G31_11 (Фирма-изготовитель)': 'prod_man',
        'G31_12 (Товарный знак, патент)': 'prod_brand',
        'G31_13 (Страна происхождения)': 'prod_coo',
        'G33 (Код товара по ТН ВЭД)': 'prod_hsc',
        'G38 (Вес нетто, кг)': 'prod_netw',
        'G46 (Статистическая стоимость, USD.)': 'prod_price_statFOB',
        'G06 (Кол-во мест)': 'prod_quant',
        'G202 (Условие поставки)': 'decl_inc'
    }
    df = rename_and_filter(df, columns_map)
    df = insert_source_column(df, os.path.basename(path))
    logger.info(f'Прочитан РФ-Мир, размер: {df.shape}')
    return df

def process_eau(path):
    logger.info('Читается ЕАЭС')
    df = read_excel_file(path)
    df = normalize_dates(df, 'Дата подачи статформы')
    columns_map = {
        'Регистрационный №': 'decl_number',
        'Дата подачи статформы': 'decl_date',
        'Наименование получателя': 'importer_name',
        'ИНН получателя': 'importer_tin',
        'Код страны получателя': 'importer_country',
        'Адрес получателя': 'importer_address',
        'Наименование отправителя': 'exporter_name',
        'ИНН отправителя': 'exporter_tin',
        'Адрес отправителя': 'exporter_address',
        'Код страны отправителя': 'exporter_country',
        'Код товара по ТН ВЭД ТС': 'prod_hsc',
        'Наименование товара по ТН ВЭД ТС': 'prod_details',
        'Вес нетто кг': 'prod_netw',
        'Статистическая стоимость $': 'prod_price_statFOB',
        'Код страны происхождения': 'prod_coo'
    }
    df = rename_and_filter(df, columns_map)
    df = insert_source_column(df, os.path.basename(path))
    logger.info(f'Прочитан ЕАЭС, размер: {df.shape}')
    return df

def process_atlas(path):
    logger.info('Читается Атлас')
    df = read_excel_file(path, sheet_name=0, parse_dates=[1])
    columns_map = {
        'DECLARATION NUMBER': 'decl_number',
        'ARRIVAL DATE': 'decl_date',
        'INCOTERMS': 'decl_inc',
        'IMPORTER NAME': 'importer_name',
        'IMPORTER ID': 'importer_tin',
        'IMPORTER COUNTRY': 'importer_country',
        'IMPORTER ADDRESS': 'importer_address',
        'EXPORTER NAME': 'exporter_name',
        'EXPORTER ID': 'exporter_tin',
        'EXPORTER ADDRESS': 'exporter_address',
        'EXPORTER COUNTRY': 'exporter_country',
        'HS CODE': 'prod_hsc',
        'PRODUCT DETAILS': 'prod_details',
        'BRAND NAME': 'prod_brand',
        'MANUFACTURING COMPANY': 'prod_man',
        'NET WEIGHT': 'prod_netw',
        'QUANTITY': 'prod_quant',
        'USD FOB': 'prod_price_statFOB',
        'COUNTRY OF ORIGIN': 'prod_coo'
    }
    df = rename_and_filter(df, columns_map)
    df = insert_source_column(df, os.path.basename(path))
    logger.info(f'Прочитан Атлас, размер: {df.shape}')
    return df

# === Main Aggregation ===
def load_all_data():
    df_rf = process_rf_world(INPUT_PATHS["rf_world"])
    df_eau = process_eau(INPUT_PATHS["eau"])
    df_atlas = process_atlas(INPUT_PATHS["atlas"])
    return pd.concat([df_eau, df_atlas, df_rf], ignore_index=True)

# === Main Execution ===
merged_df = load_all_data()
merged_df['decl_date'] = pd.to_datetime(merged_df['decl_date'], errors='coerce')
merged_df.insert(0, 'decl_id', range(len(merged_df)))

logger.info(f"Размер итогового датафрейма: {merged_df.shape}")
logger.info(f"Колонки: {merged_df.columns.tolist()}")
logger.info('Источники прочитаны')

# === Normalize Company Names ===
merged_df['exporter_name_orig'] = merged_df['exporter_name']
merged_df['importer_name_orig'] = merged_df['importer_name']

for col in ['exporter_name', 'importer_name']:
    normalized = merged_df[col].apply(normalize_company)
    merged_df[f'{col}_opf'] = normalized['ОПФ']
    merged_df[col] = normalized['Нормализованное_название']

# === Save to Excel ===
merged_df.to_excel(OUTPUT_PATH, index=False)
end_time = datetime.now()
logger.info(f'Время начала: {start_time}')
logger.info(f'Время окончания: {end_time}')
logger.info(f'Продолжительность: {end_time - start_time}')
logger.info('Готово')