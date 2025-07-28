# steps/step1_preprocess.py

from datetime import datetime
import os
import pandas as pd

from utils.io import read_excel_file, save_to_excel_file
from utils.logging_utils import setup_logger
from utils.normalization_utils import normalize_company

logger = setup_logger()

INPUT_PATHS = {
    "eau": "data/raw/EAU.xlsx",
    "atlas": "data/raw/atlas.xlsx",
    "rf_world": "data/raw/rf_world_exp_2025.xlsx"
}
OUTPUT_PATH = "data/st1_cleaned/st1.xlsx"

COLUMNS_MAPS = {
    "rf_world": {
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
    },
    "eau": {
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
    },
    "atlas": {
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
}

def process_source(name, path, columns_map, sheet_name='Sheet1', parse_dates=None):
    logger.info(f'Читается источник: {name}')
    df = read_excel_file(path, sheet_name=sheet_name, parse_dates=parse_dates)
    if 'decl_date' in columns_map.values():
        date_column = next(k for k, v in columns_map.items() if v == 'decl_date')
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce', dayfirst=True)
    df = df[list(columns_map.keys())].rename(columns=columns_map)
    df.insert(0, 'source', os.path.basename(path))
    logger.info(f'{name}: {df.shape}')
    return df

def normalize_company_columns(df):
    for col in ['exporter_name', 'importer_name']:
        df[f'{col}_orig'] = df[col]
        normalized = df[col].apply(normalize_company)
        df[f'{col}_opf'] = normalized['ОПФ']
        df[col] = normalized['Нормализованное_название']
    return df

def main():
    start_time = datetime.now()
    logger.info('--- Step 1: Загрузка и нормализация ---')

    df_rf = process_source('РФ-Мир', INPUT_PATHS['rf_world'], COLUMNS_MAPS['rf_world'])
    df_eau = process_source('ЕАЭС', INPUT_PATHS['eau'], COLUMNS_MAPS['eau'])
    df_atlas = process_source('Атлас', INPUT_PATHS['atlas'], COLUMNS_MAPS['atlas'], sheet_name=0, parse_dates=[1])

    merged_df = pd.concat([df_eau, df_atlas, df_rf], ignore_index=True)
    merged_df['decl_date'] = pd.to_datetime(merged_df['decl_date'], errors='coerce')
    merged_df.insert(0, 'decl_id', range(len(merged_df)))

    logger.info(f'Итоговая форма: {merged_df.shape}')
    logger.info(f'Колонки: {merged_df.columns.tolist()}')

    merged_df = normalize_company_columns(merged_df)

    save_to_excel_file(merged_df, OUTPUT_PATH)
    end_time = datetime.now()
    logger.info(f'Время начала: {start_time}')
    logger.info(f'Время окончания: {end_time}')
    logger.info(f'Продолжительность: {end_time - start_time}')
    logger.info('--- Готово ---')

if __name__ == '__main__':
    main()
