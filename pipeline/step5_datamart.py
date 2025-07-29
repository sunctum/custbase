# steps/step5_finalize.py

from datetime import datetime

from utils.io import read_excel_file, save_to_excel_file
from utils.logging_utils import setup_logger

logger = setup_logger()

INPUT_PATH = 'data/st4_branded/st4.xlsx'
OUTPUT_PATH = 'data/st5_datamart/st5.xlsx'
COLUMNS_TO_DROP = [
    'brand_candidates', 'brand_mixed', 'brand_column_reason',
    'was_adjusted', 'is_valid',
    'is_bad_importer', 'is_bad_exporter',
    'is_blacklisted_manual', 'blacklist_reason',
    'classification', 'reason', 'matched_approved', 'matched_rejected'
]


def add_is_relevant_column(df):
    df['any_black_flag'] = ~(
        (df['is_valid'] == 'ИСТИНА') |
        (df['is_bad_importer'] == 'ИСТИНА') |
        (df['is_bad_exporter'] == 'ИСТИНА') |
        (df['is_blacklisted_manual'] == 'ИСТИНА') |
        (df['classification'] == 'исключено')
    )
    df['any_black_flag'] = df['any_black_flag'].map({True: 'ИСТИНА', False: 'ЛОЖЬ'})
    return df


def main():
    start_time = datetime.now()
    logger.info('--- Начало Step 5 ---')

    df = read_excel_file(INPUT_PATH)
    df = add_is_relevant_column(df)
    df_cleaned = df.drop(columns=COLUMNS_TO_DROP, errors='ignore')
    save_to_excel_file(df_cleaned, OUTPUT_PATH)

    end_time = datetime.now()
    logger.info(f'Время начала: {start_time}')
    logger.info(f'Время окончания: {end_time}')
    logger.info(f'Продолжительность: {end_time - start_time}')
    logger.info('--- Готово ---')


if __name__ == '__main__':
    main()
