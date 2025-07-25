import pandas as pd
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

start_time = datetime.now()
logger.info('Начало работы Step 3')

input_excel_path = 'data/st2_tagged/st2.xlsx'
output_excel_path = 'data/st3_enriched/st3.xlsx'

# --- Функция для унификации стран ---
def unify_country_names(df: pd.DataFrame, columns_to_process: list) -> pd.DataFrame:
    """
    Унифицирует названия стран в указанных столбцах DataFrame согласно предопределенному словарю.

    Args:
        df (pd.DataFrame): Входной DataFrame, содержащий данные о странах.
        columns_to_process (list): Список строковых названий столбцов, в которых нужно унифицировать страны.

    Returns:
        pd.DataFrame: DataFrame с унифицированными названиями стран в указанных столбцах.
    """
    country_mapping = {
        'AM - АРМЕНИЯ': 'Армения',
        'Armenia': 'Армения',
        'Azerbaijan': 'Азербайджан',
        'Belarus': 'Беларусь',
        'BY - БЕЛАРУСЬ': 'Беларусь',
        'DE - ГЕРМАНИЯ': 'Германия',
        'Germany': 'Германия',
        'Georgia': 'Грузия',
        'IT - ИТАЛИЯ': 'Италия',
        'Italy': 'Италия',
        'Kazakhstan': 'Казахстан',
        'KZ - КАЗАХСТАН': 'Казахстан',
        'KG - КИРГИЗИЯ': 'Кыргызстан',
        'KG - КЫРГЫЗСТАН': 'Кыргызстан',
        'Kyrgyzstan': 'Кыргызстан',
        'Mongolia': 'Монголия',
        'PA - ПАНАМА': 'Панама',
        'Panama': 'Панама',
        'RU - РОССИЯ': 'Россия',
        'Russian Federation': 'Россия',
        'Tajikistan': 'Таджикистан',
        'TJ - ТАДЖИКИСТАН': 'Таджикистан',
        'Uzbekistan': 'Узбекистан',
        'Argentina': 'Аргентина',
        'AR - АРГЕНТИНА': 'Аргентина',
        'Australia': 'Австралия',
        'Austria': 'Австрия',
        'AT - АВСТРИЯ': 'Австрия',
        'Bahrain': 'Бахрейн',
        'Belgium': 'Бельгия',
        'BE - БЕЛЬГИЯ': 'Бельгия',
        'Bolivia': 'Боливия',
        'Brazil': 'Бразилия',
        'BR - БРАЗИЛИЯ': 'Бразилия',
        'Bulgaria': 'Болгария',
        'BG - БОЛГАРИЯ': 'Болгария',
        'Canada': 'Канада',
        'China': 'Китай',
        'CN - КИТАЙ': 'Китай',
        'Colombia': 'Колумбия',
        'Costa Rica': 'Коста-Рика',
        'Croatia': 'Хорватия',
        'HR - ХОРВАТИЯ': 'Хорватия',
        'Cyprus': 'Кипр',
        'Czech Republic': 'Чехия',
        'CZ - ЧЕХИЯ': 'Чехия',
        'Denmark': 'Дания',
        'DK - ДАНИЯ': 'Дания',
        'Egypt': 'Египет',
        'Estonia': 'Эстония',
        'EE - ЭСТОНИЯ': 'Эстония',
        'Finland': 'Финляндия',
        'FI - ФИНЛЯНДИЯ': 'Финляндия',
        'France': 'Франция',
        'FR - ФРАНЦИЯ': 'Франция',
        'Greece': 'Греция',
        'GR - ГРЕЦИЯ': 'Греция',
        'Hong Kong': 'Гонконг',
        'Hungary': 'Венгрия',
        'HU - ВЕНГРИЯ': 'Венгрия',
        'India': 'Индия',
        'IN - ИНДИЯ': 'Индия',
        'Indonesia': 'Индонезия',
        'ID - ИНДОНЕЗИЯ': 'Индонезия',
        'Iran': 'Иран',
        'IR - ИРАН (ИСЛАМСКАЯ РЕСПУБЛИКА)': 'Иран',
        'Ireland': 'Ирландия',
        'IE - ИРЛАНДИЯ': 'Ирландия',
        'Israel': 'Израиль',
        'IL - ИЗРАИЛЬ': 'Израиль',
        'Japan': 'Япония',
        'JP - ЯПОНИЯ': 'Япония',
        'Latvia': 'Латвия',
        'LV - ЛАТВИЯ': 'Латвия',
        'Lebanon': 'Ливан',
        'Liechtenstein': 'Лихтенштейн',
        'Lithuania': 'Литва',
        'LT - ЛИТВА': 'Литва',
        'Luxembourg': 'Люксембург',
        'Macedonia': 'Северная Македония',
        'Malaysia': 'Малайзия',
        'MY - МАЛАЙЗИЯ': 'Малайзия',
        'Malta': 'Мальта',
        'Mauritius': 'Маврикий',
        'Mexico': 'Мексика',
        'MX - МЕКСИКА': 'Мексика',
        'Moldova': 'Молдова',
        'Morocco': 'Марокко',
        'MA - МАРОККО': 'Марокко',
        'Netherlands': 'Нидерланды',
        'NL - НИДЕРЛАНДЫ': 'Нидерланды',
        'НИДЕРЛАНДЫ, КОРОЛЕВСТВО': 'Нидерланды',
        'New Zealand': 'Новая Зеландия',
        'Norway': 'Норвегия',
        'NO - НОРВЕГИЯ': 'Норвегия',
        'Peru': 'Перу',
        'Philippines': 'Филиппины',
        'PH - ФИЛИППИНЫ': 'Филиппины',
        'Poland': 'Польша',
        'PL - ПОЛЬША': 'Польша',
        'Portugal': 'Португалия',
        'PT - ПОРТУГАЛИЯ': 'Португалия',
        'Qatar': 'Катар',
        'Romania': 'Румыния',
        'RO - РУМЫНИЯ': 'Румыния',
        'Serbia': 'Сербия',
        'RS - СЕРБИЯ': 'Сербия',
        'Seychelles': 'Сейшельские Острова',
        'Singapore': 'Сингапур',
        'SG - СИНГАПУР': 'Сингапур',
        'Slovakia': 'Словакия',
        'SK - СЛОВАКИЯ': 'Словакия',
        'Slovenia': 'Словения',
        'SI - СЛОВЕНИЯ': 'Словения',
        'South African Republic': 'Южно-Африканская Республика',
        'ZA - ЮЖНАЯ АФРИКА': 'Южно-Африканская Республика',
        'ZA - ЮЖНАЯАФРИКА': 'Южно-Африканская Республика',
        'South Korea': 'Южная Корея',
        'KR - КОРЕЯ, РЕСПУБЛИКА': 'Южная Корея',
        'Spain': 'Испания',
        'ES - ИСПАНИЯ': 'Испания',
        'Sweden': 'Швеция',
        'SE - ШВЕЦИЯ': 'Швеция',
        'Switzerland': 'Швейцария',
        'CH - ШВЕЙЦЕРИЯ': 'Швейцария',
        'Taiwan': 'Тайвань',
        'TW - КИТАЙСКАЯ ПРОВИНЦИЯ ТАЙВАНЬ': 'Тайвань',
        'TW - ТАЙВАНЬ (КИТАЙ)': 'Тайвань',
        'Thailand': 'Таиланд',
        'TH - ТАИЛАНД': 'Таиланд',
        'Turkiye': 'Турция',
        'TR - ТУРЦИЯ': 'Турция',
        'Turkmenistan': 'Туркменистан',
        'Ukraine': 'Украина',
        'UA - УКРАИНА': 'Украина',
        'United Arab Emirates': 'Объединенные Арабские Эмираты',
        'United Kingdom': 'Великобритания',
        'GB - СОЕДИНЕННОЕ КОРОЛЕВСТВО': 'Великобритания',
        'United States': 'США',
        'US - СОЕДИНЕННЫЕ ШТАТЫ': 'США',
        'Viet Nam': 'Вьетнам',
        'VN - ВЬЕТНАМ': 'Вьетнам',
        'Andorra': 'Андорра',
        'Bosnia And Herzegovina': 'Босния и Герцеговина',
        'Dominica': 'Доминика',
        'Dominican Republic': 'Доминиканская Республика',
        'DZ - АЛЖИР': 'Алжир',
        'EC - ЭКВАДОР': 'Эквадор',
        'EU - СТРАНЫ ЕВРОСОЮЗА': 'Страны Евросоюза',
        'Honduras': 'Гондурас',
        'MO - МАКАО': 'Макао',
        'RW - РУАНДА': 'Руанда',
        'Saudi Arabia': 'Саудовская Аравия',
        'Sierra Leone': 'Сьерра-Леоне',
        'Tunisia': 'Тунис',
    }

    for col in columns_to_process:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: country_mapping.get(x, x) if isinstance(x, str) else x)
        else:
            logger.warning(f"Предупреждение: Столбец '{col}' не найден в DataFrame. Пропускаем его.")
    return df
# --- Функция перераспределения стоимости и веса ---
def enrich_decl_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    group_keys = ['decl_number', 'decl_date', 'importer_name', 'exporter_name', 'source']

    # Защита от NaN
    df['prod_quant'] = df['prod_quant'].fillna(0)
    df['prod_price_statFOB'] = df['prod_price_statFOB'].fillna(0)
    df['prod_netw'] = df['prod_netw'].fillna(0)

    # Округляем для стабильности
    df['prod_price_statFOB'] = df['prod_price_statFOB'].round(2)
    df['prod_netw'] = df['prod_netw'].round(3)

    # Надежная проверка "все значения одинаковы"
    df['__same_price'] = df.groupby(group_keys)['prod_price_statFOB'].transform(lambda x: x.max() - x.min() < 0.01)
    df['__same_netw'] = df.groupby(group_keys)['prod_netw'].transform(lambda x: x.max() - x.min() < 0.001)

    df['__needs_adjustment'] = df['__same_price'] & df['__same_netw']

    df_adj = df[df['__needs_adjustment']].copy()
    agg = df_adj.groupby(group_keys).agg(
        total_quant=('prod_quant', 'sum'),
        total_price=('prod_price_statFOB', 'first'),
        total_netw=('prod_netw', 'first'),
        num_rows=('prod_quant', 'count')
    ).reset_index()

    df_adj = df_adj.merge(agg, on=group_keys, how='left')

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
    df_adj.drop(columns=[
        'total_quant', 'total_price', 'total_netw', 'num_rows',
        '__same_price', '__same_netw'
    ], inplace=True)

    df_rest = df[~df['__needs_adjustment']].copy()
    df_rest['adj_price'] = df_rest['prod_price_statFOB']
    df_rest['adj_netw'] = df_rest['prod_netw']
    df_rest['was_adjusted'] = False

    df_final = pd.concat([df_adj, df_rest], ignore_index=True)
    df_final.drop(columns='__needs_adjustment', inplace=True)

    # Страхуем от NaN
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
def apply_manual_blacklist(df: pd.DataFrame) -> pd.DataFrame:
    """
    Проставляет флаг is_blacklisted_manual на основании таблицы с ручным блеклистом.

    blacklist_path — путь к Excel-файлу с колонками:
        - company_name (str): название компании
        - type (str): 'importer' или 'exporter'
        - reason (str): причина внесения

    Добавляет в df:
        - is_blacklisted_manual: True/False
        - blacklist_reason: причина (если есть)

    Возвращает DataFrame с новыми колонками.
    """
    df = df.copy()
    df['is_blacklisted_manual'] = False
    df['blacklist_reason'] = ""
    blacklist_path = r'C:\Users\424\Documents\custbase\data\utilities\blacklist_companies.xlsx'

    try:
        blacklist = pd.read_excel(blacklist_path)
        blacklist = blacklist.dropna(subset=["company_name", "type"])
        for _, row in blacklist.iterrows():
            name = row["company_name"]
            type = row["type"].lower()
            reason = row.get("reason", "")

            if type == "importer":
                mask = df["importer_name"] == name
            elif type == "exporter":
                mask = df["exporter_name"] == name
            else:
                continue  # неизвестный тип, пропускаем

            df.loc[mask, "is_blacklisted_manual"] = True
            df.loc[mask, "blacklist_reason"] = reason

    except Exception as e:
        logger.warning(f"⚠️ Не удалось применить ручной блеклист: {e}")

    return df

# --- Начало основного блока обработки ---
try:
    df_raw = pd.read_excel(input_excel_path)
    logger.info(f"✅ Файл '{input_excel_path}' успешно прочитан. Размеры DataFrame: {df_raw.shape}")
except FileNotFoundError:
    logger.warning(f"⚠️ Ошибка: Файл '{input_excel_path}' не найден. Проверьте путь.")
    df_raw = pd.DataFrame()
except Exception as e:
    logger.error(f"❌ Произошла ошибка при чтении файла: {e}")
    df_raw = pd.DataFrame()

if not df_raw.empty:
    # 1. Унификация стран
    logger.info("🔁 Унификация названий стран...")
    country_cols = ["prod_coo", "exporter_country", "importer_country"]
    df_processed = unify_country_names(df_raw.copy(), country_cols)
    logger.info("✅ Завершена унификация.")
    # 2. Обогащение по дублирующимся декларациям
    logger.info("🔁 Перераспределение по дубликатам деклараций...")
    df_processed = enrich_decl_duplicates(df_processed)
    logger.info("✅ Завершено перераспределение. Добавлены adj_price, adj_netw, was_adjusted.")
    # 3. Проверка корректности unit_price_kg
    logger.info("🔁 Проверка корректности unit_price_kg...")
    df_processed = flag_unit_price_anomalies(df_processed)
    logger.info(f"✅ Аномалии помечены. Некорректных строк: {(~df_processed['is_valid']).sum()}")
    # 4. Тегирование "плохих" компаний
    logger.info("🔁 Выявление подозрительных компаний на основании классификации...")
    df_processed = flag_suspect_companies(df_processed)
    bad_importers = df_processed['is_bad_importer'].sum()
    bad_exporters = df_processed['is_bad_exporter'].sum()
    logger.info(f"✅ Подозрительные компании выявлены. Импортёров: {bad_importers}, экспортёров: {bad_exporters}")
    # 5. Тегирование по ручному блеклисту
    logger.info("🔁 Применение ручного блеклиста...")
    df_processed = apply_manual_blacklist(df_processed)
    logger.info(f"✅ Блеклист применён. Найдено: {df_processed['is_blacklisted_manual'].sum()} записей")


    # Сохранение
    try:
        df_processed.to_excel(output_excel_path, index=False)
        logger.info(f"📁 Файл сохранён: {output_excel_path}")
        end_time = datetime.now()
        logger.info(f'🕒 Продолжительность: {end_time - start_time}')
    except Exception as e:
        logger.error(f"❌ Ошибка при сохранении файла: {e}")
else:
    logger.warning("⚠️ Обработка не выполнена: исходный файл пуст.")