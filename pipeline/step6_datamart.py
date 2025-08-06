# steps/step5_finalize.py

from datetime import datetime

from utils.io import read_excel_file, save_to_excel_file
from utils.logging_utils import setup_logger

logger = setup_logger()

INPUT_PATH = 'data/st5_attributes/st5.xlsx'
OUTPUT_PATH = 'data/st6_datamart/st6.xlsx'
COLUMNS_TO_DROP = [
    'brand_candidates', 'brand_mixed', 'brand_column_reason',
    'was_adjusted', '__same_price','__same_netw',
    'is_valid', 'is_valid_reason',
    'is_bad_importer', 'is_bad_exporter',
    'is_blacklisted_manual', 'blacklist_reason',
    'classification', 'reason', 'matched_approved', 'matched_rejected',
    'brand_candidates', 'brand_mixed'
]


def add_is_relevant_column(df):
    df = df.copy()

    reasons = []

    conditions = {
        'is_valid': lambda row: not row['is_valid'],
        'is_bad_importer': lambda row: row['is_bad_importer'],
        'is_bad_exporter': lambda row: row['is_bad_exporter'],
        'is_blacklisted_manual': lambda row: row['is_blacklisted_manual'],
        'classification': lambda row: row['classification'] == 'исключено',
    }

    def get_reasons(row):
        row_reasons = []
        for reason, condition in conditions.items():
            if condition(row):
                row_reasons.append(reason)
        return ', '.join(row_reasons)

    df['is_relevant'] = ~(
        (~df['is_valid']) |
        (df['is_bad_importer']) |
        (df['is_bad_exporter']) |
        (df['is_blacklisted_manual']) |
        (df['classification'] == 'исключено')
    )
    df['is_relevant'] = df['is_relevant'].map({True: 'ИСТИНА', False: 'ЛОЖЬ'})

    df['is_relevant_reason'] = df.apply(
        lambda row: get_reasons(row) if row['is_relevant'] == 'ЛОЖЬ' else '', axis=1
    )

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
