# steps/step3_enrichment.py

import pandas as pd
from datetime import datetime
import logging

from utils.io import read_excel_file, save_to_excel_file
from utils.logging_utils import setup_logger

# --- Логгер ---
logger = setup_logger()
start_time = datetime.now()
logger.info('--- Step 3: Обогащение и валидация ---')

# --- Пути ---
INPUT_PATH = 'data/st2_tagged/st2.xlsx'
OUTPUT_PATH = 'data/st3_enriched/st3.xlsx'
BLACKLIST_PATH = 'data/utilities/blacklist_companies.xlsx'

# ---------------------------- ФУНКЦИИ ---------------------------- #

# --- Функция для унификации стран ---
def unify_country_names(df: pd.DataFrame, columns_to_process: list) -> pd.DataFrame:
    """
    Унифицирует страны в указанных столбцах по словарю с вариантами написаний.
    Поддержаны ISO‑коды alpha‑2/alpha‑3 (UZ, UZB, CN, CHN, MN, MNG и т.д.) и специфичные (таможенные) варианты.
    """

    groups = {
        # СНГ/ЕАЭС и соседи
        ('AM', 'ARM', 'Armenia', 'AM - АРМЕНИЯ'): 'Армения',
        ('OS', 'South Ossetia'): 'Южная Осетия',
        ('AB', 'ABH', 'Abkhazia'): 'Абхазия',
        ('AZ', 'AZE', 'Azerbaijan'): 'Азербайджан',
        ('BY', 'BLR', 'Belarus', 'BY - БЕЛАРУСЬ'): 'Беларусь',
        ('GE', 'GEO', 'Georgia'): 'Грузия',
        ('KZ', 'KAZ', 'Kazakhstan', 'KZ - КАЗАХСТАН'): 'Казахстан',
        ('KG', 'KGZ', 'Kyrgyzstan', 'KG - КИРГИЗИЯ', 'KG - КЫРГЫЗСТАН', 'Киргизия', 'Кыргызстан'): 'Кыргызстан',
        ('MN', 'MNG', 'Mongolia'): 'Монголия',
        ('RU', 'RUS', 'Russian Federation', 'RU - РОССИЯ', 'Российская Федерация'): 'Россия',
        ('TJ', 'TJK', 'Tajikistan', 'TJ - ТАДЖИКИСТАН'): 'Таджикистан',
        ('TM', 'TKM', 'Turkmenistan'): 'Туркменистан',
        ('UA', 'UKR', 'Ukraine', 'UA - УКРАИНА'): 'Украина',
        ('UZ', 'UZB', 'Uzbekistan'): 'Узбекистан',

        # Европа
        ('DE', 'DEU', 'Germany', 'DE - ГЕРМАНИЯ'): 'Германия',
        ('FO', 'FRO', 'Faroe Islands'): 'Фарерские острова',
        ('IT', 'ITA', 'Italy', 'IT - ИТАЛИЯ'): 'Италия',
        ('FR', 'FRA', 'France', 'FR - ФРАНЦИЯ'): 'Франция',
        ('ES', 'ESP', 'Spain', 'ES - ИСПАНИЯ'): 'Испания',
        ('PT', 'PRT', 'Portugal', 'PT - ПОРТУГАЛИЯ'): 'Португалия',
        ('PL', 'POL', 'Poland', 'PL - ПОЛЬША'): 'Польша',
        ('CZ', 'CZE', 'Czech Republic', 'CZ - ЧЕХИЯ'): 'Чехия',
        ('SK', 'SVK', 'Slovakia', 'SK - СЛОВАКИЯ'): 'Словакия',
        ('SI', 'SVN', 'Slovenia', 'SI - СЛОВЕНИЯ'): 'Словения',
        ('HU', 'HUN', 'Hungary', 'HU - ВЕНГРИЯ'): 'Венгрия',
        ('RO', 'ROU', 'Romania', 'RO - РУМЫНИЯ'): 'Румыния',
        ('BG', 'BGR', 'Bulgaria', 'BG - БОЛГАРИЯ'): 'Болгария',
        ('GR', 'GRC', 'Greece', 'GR - ГРЕЦИЯ'): 'Греция',
        ('NL', 'NLD', 'Netherlands', 'NL - НИДЕРЛАНДЫ', 'НИДЕРЛАНДЫ, КОРОЛЕВСТВО'): 'Нидерланды',
        ('BE', 'BEL', 'Belgium', 'BE - БЕЛЬГИЯ'): 'Бельгия',
        ('LU', 'LUX', 'Luxembourg'): 'Люксембург',
        ('IE', 'IRL', 'Ireland', 'IE - ИРЛАНДИЯ'): 'Ирландия',
        ('GB', 'GBR', 'United Kingdom', 'GB - СОЕДИНЕННОЕ КОРОЛЕВСТВО'): 'Великобритания',
        ('EE', 'EST', 'Estonia', 'EE - ЭСТОНИЯ'): 'Эстония',
        ('LV', 'LVA', 'Latvia', 'LV - ЛАТВИЯ'): 'Латвия',
        ('LT', 'LTU', 'Lithuania', 'LT - ЛИТВА'): 'Литва',
        ('FI', 'FIN', 'Finland', 'FI - ФИНЛЯНДИЯ'): 'Финляндия',
        ('SE', 'SWE', 'Sweden', 'SE - ШВЕЦИЯ'): 'Швеция',
        ('NO', 'NOR', 'Norway', 'NO - НОРВЕГИЯ'): 'Норвегия',
        ('DK', 'DNK', 'Denmark', 'DK - ДАНИЯ'): 'Дания',
        ('CH', 'CHE', 'Switzerland', 'CH - ШВЕЙЦЕРИЯ'): 'Швейцария',
        ('AD', 'AND', 'Andorra'): 'Андорра',
        ('HR', 'HRV', 'Croatia', 'HR - ХОРВАТИЯ'): 'Хорватия',
        ('SJ', 'SJM', 'Svalbard and Jan Mayen'): 'Шпицберген и Ян-Майен',
        ('RS', 'SRB', 'Serbia', 'RS - СЕРБИЯ'): 'Сербия',
        ('MK', 'MKD', 'Macedonia'): 'Северная Македония',
        ('CY', 'CYP', 'Cyprus'): 'Кипр',
        ('MD', 'MDA', 'Moldova'): 'Молдова',

        # Азия
        ('AF', 'AFG', 'Afghanistan'): 'Афганистан',
        ('BO', 'BOL', 'Bolivia'): 'Боливия',
        ('BD', 'BGD', 'Bangladesh'): 'Бангладеш',
        ('AO', 'AGO', 'Angola'): 'Ангола',
        ('CN', 'CHN', 'China', 'CN - КИТАЙ'): 'Китай',
        ('HK', 'HKG', 'Hong Kong'): 'Гонконг',
        ('MO', 'MAC', 'MO - МАКАО', 'Macao', 'Макао'): 'Макао',
        ('JP', 'JPN', 'Japan', 'JP - ЯПОНИЯ'): 'Япония',
        ('KR', 'KOR', 'South Korea', 'KR - КОРЕЯ, РЕСПУБЛИКА'): 'Южная Корея',
        ('TW', 'TWN', 'Taiwan', 'TW - КИТАЙСКАЯ ПРОВИНЦИЯ ТАЙВАНЬ', 'TW - ТАЙВАНЬ (КИТАЙ)'): 'Тайвань',
        ('VN', 'VNM', 'Viet Nam', 'VN - ВЬЕТНАМ'): 'Вьетнам',
        ('TH', 'THA', 'Thailand', 'TH - ТАИЛАНД'): 'Таиланд',
        ('SG', 'SGP', 'Singapore', 'SG - СИНГАПУР'): 'Сингапур',
        ('MY', 'MYS', 'Malaysia', 'MY - МАЛАЙЗИЯ'): 'Малайзия',
        ('ID', 'IDN', 'Indonesia', 'ID - ИНДОНЕЗИЯ'): 'Индонезия',
        ('PH', 'PHL', 'Philippines', 'PH - ФИЛИППИНЫ'): 'Филиппины',
        ('IN', 'IND', 'India', 'IN - ИНДИЯ'): 'Индия',
        ('IR', 'IRN', 'Iran', 'IR - ИРАН (ИСЛАМСКАЯ РЕСПУБЛИКА)'): 'Иран',
        ('IL', 'ISR', 'Israel', 'IL - ИЗРАИЛЬ'): 'Израиль',
        ('IQ', 'IRQ', 'Iraq'): 'Ирак',
        ('QA', 'QAT', 'Qatar'): 'Катар',
        ('AE', 'ARE', 'United Arab Emirates'): 'Объединенные Арабские Эмираты',
        ('TR', 'TUR', 'Turkiye', 'TR - ТУРЦИЯ'): 'Турция',
        ('LB', 'LBN', 'Lebanon'): 'Ливан',
        ('LK', 'LKA', 'Sri Lanka'): 'Шри-Ланка',
        ('SA', 'SAU', 'Saudi Arabia'): 'Саудовская Аравия',
        ('LA', 'LAO', 'Laos'): 'Лаос',
        ('LY', 'LBY', 'Libya'): 'Ливия',
        ('OM', 'OMN', 'Oman'): 'Оман',

        # Африка
        ('EG', 'EGY', 'Egypt'): 'Египет',
        ('GN', 'GIN', 'Guinea'): 'Гвинея',
        ('CD', 'COD', 'Congo (the Democratic Republic of the)'): 'Конго (ДРК)',
        ('CG', 'COG', 'Congo (the)'): 'Конго',
        ('MA', 'MAR', 'Morocco', 'MA - МАРОККО'): 'Марокко',
        ('TN', 'TUN', 'Tunisia'): 'Тунис',
        ('ZA', 'ZAF', 'South African Republic', 'ZA - ЮЖНАЯ АФРИКА', 'ZA - ЮЖНАЯАФРИКА'): 'Южно-Африканская Республика',
        ('RW', 'RWA', 'RW - РУАНДА'): 'Руанда',
        ('GA', 'GAB', 'Gabon'): 'Габон',
        ('DZ', 'DZA', 'DZ - АЛЖИР'): 'Алжир',
        ('SL', 'SLE', 'Sierra Leone'): 'Сьерра-Леоне',
        ('SN', 'SEN', 'Senegal'): 'Сенегал',
        ('UG', 'UGA', 'Uganda'): 'Уганда',

        # Америка и Океания
        ('US', 'USA', 'United States', 'US - СОЕДИНЕННЫЕ ШТАТЫ'): 'США',
        ('EC', 'ECU', 'Ecuador'): 'Эквадор',
        ('HN', 'HND', 'Honduras'): 'Гондурас',
        ('BB', 'BRB', 'Barbados'): 'Барбадос',
        ('CA', 'CAN', 'Canada'): 'Канада',
        ('MX', 'MEX', 'Mexico', 'MX - МЕКСИКА'): 'Мексика',
        ('PA', 'PAN', 'Panama', 'PA - ПАНАМА', 'Panama'): 'Панама',
        ('BR', 'BRA', 'BR - БРАЗИЛИЯ', 'Brazil'): 'Бразилия',
        ('AR', 'ARG', 'AR - АРГЕНТИНА', 'Argentina'): 'Аргентина',
        ('CO', 'COL', 'Colombia'): 'Колумбия',
        ('PE', 'PER', 'Peru'): 'Перу',
        ('CL', 'CHL', 'Chile'): 'Чили',
        ('AU', 'AUS', 'Australia'): 'Австралия',
        ('SC', 'SYC', 'Seychelles'): 'Сейшельские Острова',
        ('NZ', 'NZL', 'New Zealand'): 'Новая Зеландия',

        # Прочее
        ('EU', 'EU - СТРАНЫ ЕВРОСОЮЗА'): 'Страны Евросоюза',
        ('Hong Kong',): 'Гонконг',
    }

    flat = {}
    for variants, ru in groups.items():
        for v in variants:
            flat[str(v).strip().casefold()] = ru

    def _map_val(x):
        if isinstance(x, str):
            return flat.get(x.strip().casefold(), x.strip())
        return x

    for col in columns_to_process:
        if col in df.columns:
            df[col] = df[col].map(_map_val)

    return df
# --- Функция перераспределения стоимости и веса ---
def enrich_decl_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    group_keys = ['decl_number', 'decl_date', 'importer_name', 'exporter_name', 'source']
    extended_keys = group_keys + ['prod_price_statFOB', 'prod_netw']

    df['prod_quant'] = df['prod_quant'].fillna(0)
    df['prod_price_statFOB'] = df['prod_price_statFOB'].fillna(0).round(2)
    df['prod_netw'] = df['prod_netw'].fillna(0).round(3)

    # Найдём строки, которые дублируются по extended_keys
    df['__needs_adjustment'] = df.duplicated(subset=extended_keys, keep=False)
    df_adj = df[df['__needs_adjustment']].copy()

    # Группируем по extended_keys
    agg = df_adj.groupby(extended_keys).agg(
        total_quant=('prod_quant', 'sum'),
        total_price=('prod_price_statFOB', 'first'),
        total_netw=('prod_netw', 'first'),
        num_rows=('prod_quant', 'count')
    ).reset_index()

    df_adj = df_adj.merge(agg, on=extended_keys, how='left')

    # Пропорциональное распределение
    df_adj['adj_price'] = df_adj.apply(
        lambda row: (row['prod_quant'] / row['total_quant']) * row['total_price']
        if row['total_quant'] > 0 else row['total_price'] / row['num_rows'],
        axis=1
    )
    df_adj['adj_netw'] = df_adj.apply(
        lambda row: (row['prod_quant'] / row['total_quant']) * row['total_netw']
        if row['total_quant'] > 0 else row['total_netw'] / row['num_rows'],
        axis=1
    )
    df_adj['was_adjusted'] = True
    df_adj.drop(columns=['total_quant', 'total_price', 'total_netw', 'num_rows'], inplace=True)

    # Остальные строки — без перерасчета
    df_rest = df[~df['__needs_adjustment']].copy()
    df_rest['adj_price'] = df_rest['prod_price_statFOB']
    df_rest['adj_netw'] = df_rest['prod_netw']
    df_rest['was_adjusted'] = False

    # Объединение и финальные шаги
    df_final = pd.concat([df_adj, df_rest], ignore_index=True)
    df_final.drop(columns='__needs_adjustment', inplace=True)
    df_final['adj_price'] = df_final['adj_price'].fillna(df_final['prod_price_statFOB'])
    df_final['adj_netw'] = df_final['adj_netw'].fillna(df_final['prod_netw'])

    return df_final
# --- Функция тегирования аномалий ЦКГ ---
def flag_unit_price_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Вычисляет цену за кг и помечает записи с аномально низкой/высокой/нулевой ценой.
    
    Добавляет столбцы:
        - unit_price_kg: цена за кг
        - is_valid: True/False — признак валидности
        - is_valid_reason: причина (если строка невалидна)
    
    Args:
        df (pd.DataFrame): DataFrame с полями 'adj_price' и 'adj_netw'.
        
    Returns:
        pd.DataFrame: DataFrame с новыми флагами и диагностикой.
    """
    df = df.copy()
    df['unit_price_kg'] = df['adj_price'] / df['adj_netw']

    too_low = df['unit_price_kg'] < 1
    too_high = df['unit_price_kg'] > 50
    zero_or_negative = df['unit_price_kg'] <= 0

    df['is_valid'] = ~(too_low | too_high | zero_or_negative)

    # Инициализация пустых причин
    df['is_valid_reason'] = ''
    df.loc[too_low, 'is_valid_reason'] = 'unit_price_kg < 1'
    df.loc[too_high, 'is_valid_reason'] = 'unit_price_kg > 50'
    df.loc[zero_or_negative, 'is_valid_reason'] = 'unit_price_kg <= 0'

    return df
# --- Функция тегирования компаний на основании классификации лемм ---
def flag_suspect_companies(df: pd.DataFrame, min_records: int = 10, exclusion_threshold: float = 0.9) -> pd.DataFrame:
    """
    Вычисляет долю записей с классификацией 'исключено' для каждой компании-импортера и экспортера.
    Добавляет флаги:
        - is_bad_importer: True, если у импортера >90% исключенных записей и >= 10 строк
        - is_bad_exporter: аналогично для экспортера

    Args:
        df (pd.DataFrame): Исходный DataFrame, содержащий колонку 'classification'
        min_records (int): Минимальное число строк для анализа
        exclusion_threshold (float): Порог доли 'исключено'

    Returns:
        pd.DataFrame: Обогащённый DataFrame с двумя новыми флагами
    """
    df = df.copy()
    for entity in ['importer_name', 'exporter_name']:
        stats = (
            df.groupby(entity)['classification']
            .value_counts(normalize=False)
            .unstack(fill_value=0)
            .rename(columns=lambda x: f"_{x}")
        )

        stats['total'] = stats.sum(axis=1)
        stats['excluded_ratio'] = stats.get('_исключено', 0) / stats['total']
        suspects = stats.query('total >= @min_records and excluded_ratio > @exclusion_threshold').index

        flag_column = f'is_bad_{entity.split("_")[0]}'
        df[flag_column] = df[entity].isin(suspects)

    return df
# --- Функция тегирования компаний из ручного блеклиста ---
def apply_manual_blacklist(df: pd.DataFrame, path: str) -> pd.DataFrame:
    df = df.copy()
    df['is_blacklisted_manual'] = False
    df['blacklist_reason'] = ""
    try:
        blacklist = pd.read_excel(path)
        blacklist = blacklist.dropna(subset=["company_name", "type"])
        for _, row in blacklist.iterrows():
            name = row["company_name"]
            type_ = row["type"].lower()
            reason = row.get("reason", "")
            if type_ == "importer":
                mask = df["importer_name"] == name
            elif type_ == "exporter":
                mask = df["exporter_name"] == name
            else:
                continue
            df.loc[mask, "is_blacklisted_manual"] = True
            df.loc[mask, "blacklist_reason"] = reason
    except Exception as e:
        logger.warning(f"⚠️ Не удалось применить ручной блеклист: {e}")
    return df
# --- Функция обрезки prod_hsc --- 
# !!! не работает, нужно переделать. скорее всего проблема в том, что prod_hsc - int. 
# по хорошему, результат нужно выкидывать в новый "prod_hsc_adj" или какой-то другой лог
def truncate_long_prod_hsc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Если длина значения в prod_hsc больше 10 символов — обрезает последнюю цифру.

    Args:
        df (pd.DataFrame): Исходный DataFrame

    Returns:
        pd.DataFrame: DataFrame с обработанным prod_hsc
    """
    df = df.copy()
    if "prod_hsc" in df.columns:
        df["prod_hsc"] = df["prod_hsc"].apply(
            lambda x: str(x)[:-1] if isinstance(x, str) and len(x) > 10 else x
        )
    else:
        logger.warning("⚠️ Столбец 'prod_hsc' не найден в DataFrame — обрезка не выполнена.")
    return df

# ------------------------- ОСНОВНОЙ БЛОК -------------------------- #

def main():
    try:
        df_raw = read_excel_file(INPUT_PATH)
        logger.info(f"✅ Прочитан файл: {INPUT_PATH} ({df_raw.shape})")
    except Exception as e:
        logger.error(f"❌ Ошибка при загрузке: {e}")
        return

    # 1. Унификация стран
    df = unify_country_names(df_raw, ["prod_coo", "exporter_country", "importer_country"])

    # 2. Обрезка prod_hsc
    df = truncate_long_prod_hsc(df)

    # 3. Обогащение по дубликатам
    df = enrich_decl_duplicates(df)

    # 4. Аномалии unit_price_kg
    df = flag_unit_price_anomalies(df)
    logger.info(f"❗ Некорректных строк: {(~df['is_valid']).sum()}")

    # 5. Подозрительные компании
    df = flag_suspect_companies(df)

    # 6. Ручной блеклист
    df = apply_manual_blacklist(df, BLACKLIST_PATH)

    try:
        save_to_excel_file(df, OUTPUT_PATH)
        end_time = datetime.now()
        logger.info(f"📁 Сохранено: {OUTPUT_PATH}")
        logger.info(f"🕒 Продолжительность: {end_time - start_time}")
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении: {e}")

if __name__ == '__main__':
    main()