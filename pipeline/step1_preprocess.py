import pandas as pd
from datetime import datetime
import os
import re
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
logger.info('Начало работы Step 1')

eau_df_path = 'data/raw/EAU.xlsx'
atlas_df_path = 'data/raw/atlas.xlsx'
rf_world_path = 'data/raw/rf_world_exp_2025.xlsx'
output_excel_path = 'data/st1_cleaned/st1.xlsx'
eau_df_name = os.path.basename(eau_df_path)
atlas_df_name = os.path.basename(atlas_df_path)
rf_world_name = os.path.basename(rf_world_path)

# блок рф-мир
logger.info('Читается РФ-Мир')

rf_world_df = pd.read_excel(rf_world_path, sheet_name='Sheet1')
rf_world_df['G072 (Дата регистрации)'] = pd.to_datetime(
    rf_world_df['G072 (Дата регистрации)'], 
    format='%d.%m.%Y',
    errors='coerce'
)

rf_world_df_sel = rf_world_df[[
                               'ND (Номер декларации)',
                               'G072 (Дата регистрации)',
                               'G021 (ИНН отправителя)',
                               'G022 (Наименование отправителя)',
                               'G023 (Адрес отправителя)',
                               'G0231 (Код страны отправителя)',
                               'G081 (ИНН получателя)',
                               'G082 (Наименование получателя)',
                               'G0831 (Код страны получателя)',
                               'G083 (Адрес получателя)',
                               'G31_1 (Наименование и характеристики товаров)',
                               'G31_11 (Фирма-изготовитель)',
                               'G31_12 (Товарный знак, патент)',
                               'G31_13 (Страна происхождения)',
                               'G33 (Код товара по ТН ВЭД)',
                               'G38 (Вес нетто, кг)',
                               'G46 (Статистическая стоимость, USD.)',
                               'G06 (Кол-во мест)',
                               'G202 (Условие поставки)'
                               ]]

rf_world_df_sel = rf_world_df_sel.rename(columns= \
                                         {
                                            'ND (Номер декларации)':'decl_number',
                                            'G072 (Дата регистрации)':'decl_date',
                                            'G021 (ИНН отправителя)':'exporter_tin',
                                            'G022 (Наименование отправителя)':'exporter_name',
                                            'G023 (Адрес отправителя)':'exporter_address',
                                            'G0231 (Код страны отправителя)':'exporter_country',
                                            'G081 (ИНН получателя)':'importer_tin',
                                            'G082 (Наименование получателя)':'importer_name',
                                            'G0831 (Код страны получателя)':'importer_country',
                                            'G083 (Адрес получателя)':'importer_address',
                                            'G31_1 (Наименование и характеристики товаров)':'prod_details',
                                            'G31_11 (Фирма-изготовитель)':'prod_man',
                                            'G31_12 (Товарный знак, патент)':'prod_brand',
                                            'G31_13 (Страна происхождения)':'prod_coo',
                                            'G33 (Код товара по ТН ВЭД)':'prod_hsc',
                                            'G38 (Вес нетто, кг)':'prod_netw',
                                            'G46 (Статистическая стоимость, USD.)':'prod_price_statFOB',
                                            'G06 (Кол-во мест)':'prod_quant',
                                            'G202 (Условие поставки)':'decl_inc'    
                                         })

rf_world_df_sel.insert(0, 'source', rf_world_name)

logger.info('Прочитан РФ-Мир')
logger.info(f"Размер РФ-Мир: {rf_world_df_sel.shape}")

# блок eau
logger.info('Читается ЕАЭС')

eau_df = pd.read_excel(eau_df_path, sheet_name='Sheet1')

eau_df['Дата подачи статформы'] = pd.to_datetime(
    eau_df['Дата подачи статформы'], 
    format='%d.%m.%Y',
    errors='coerce'
)

eau_df_sel = eau_df[['Регистрационный №',
                     'Дата подачи статформы',  
                    'Наименование получателя', 
                    'Адрес получателя', 
                    'Код страны получателя', 
                    'ИНН получателя', 
                    'Наименование отправителя', 
                    'Адрес отправителя', 
                    'Код страны отправителя', 
                    'ИНН отправителя',
                    'Код товара по ТН ВЭД ТС', 
                    'Наименование товара по ТН ВЭД ТС', 
                    'Вес нетто кг', 
                    'Статистическая стоимость $', 
                    'Код страны происхождения'
                    ]]

eau_df_sel = eau_df_sel.rename(columns= \
                           {'Регистрационный №': 'decl_number', 
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
                            })
                  
eau_df_sel.insert(0, 'source', eau_df_name)

logger.info('Прочитан ЕАЭС')
logger.info(f"Размер ЕАЭС: {eau_df_sel.shape}")

# блок atlas
# Есть несоответствие - в eau мы берем стат стоимость в $, а в атлас - фоб в $. Это связано с исходными данными - в атласе CIF USD практически не заполнен, а в EAU нет близких "условий"
logger.info('Читается Атлас')
atlas_df = pd.read_excel(atlas_df_path, sheet_name=0, parse_dates=[1])

atlas_df_sel = atlas_df[['DECLARATION NUMBER',
                    'ARRIVAL DATE',
                    'INCOTERMS', 
                    'IMPORTER NAME',
                    'IMPORTER ID', 
                    'IMPORTER COUNTRY', 
                    'IMPORTER ADDRESS', 
                    'EXPORTER NAME', 
                    'EXPORTER ID',
                    'EXPORTER ADDRESS', 
                    'EXPORTER COUNTRY', 
                    'HS CODE', 
                    'PRODUCT DETAILS', 
                    'BRAND NAME', 
                    'MANUFACTURING COMPANY', 
                    'NET WEIGHT', 
                    'QUANTITY', 
                    'COUNTRY OF ORIGIN',
                    'USD FOB'
                    ]]

atlas_df_sel = atlas_df_sel.rename(columns= \
                           {'DECLARATION NUMBER': 'decl_number', 
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
                            'BRAND NAME' : 'prod_brand',
                            'MANUFACTURING COMPANY': 'prod_man',
                            'NET WEIGHT': 'prod_netw', 
                            'QUANTITY': 'prod_quant',                            
                            'USD FOB': 'prod_price_statFOB', 
                            'COUNTRY OF ORIGIN': 'prod_coo' 
                            })

atlas_df_sel.insert(0, 'source', atlas_df_name)

logger.info('Прочитан Атлас')
logger.info(f"Размер Атлас: {atlas_df_sel.shape}")

merged_df = pd.concat([eau_df_sel, atlas_df_sel, rf_world_df_sel], ignore_index=True, sort=False)

merged_df['decl_date'] = pd.to_datetime(merged_df['decl_date'], errors='coerce')
merged_df.insert(0, 'decl_id', range(len(merged_df)))

logger.info(f"Размер итогового датафрейма: {merged_df.shape}")
logger.info(f"Колонки: {merged_df.columns.tolist()}")
logger.info('Источники прочитаны')

def clean_and_extract(name):
    # Сначала удаляем кавычки и спецсимволы. Убрал точку и двоеточие, т.к. могут быть в ОПФ типа S.P.A. или Ф-Л
    # Двоеточие может быть в "ООО Компания: Филиал" - лучше оставить для более тонкой обработки если нужно
    cleaned_name = re.sub(r'[",«»()&?<>“”|/-]', '', name)
    cleaned_name = re.sub(r"'", '', cleaned_name)
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip().lower()

    keywords_after = ["по поручению", "по поруч", "по пручению", "для", "b/o", "by order", "by"]
    keywords_before = ["через"]

    for keyword in keywords_after:
        if keyword in cleaned_name:
            parts = cleaned_name.split(keyword, maxsplit=1)
            if len(parts) > 1:
                cleaned_name = parts[1].strip()
                return cleaned_name

    for keyword in keywords_before:
        if keyword in cleaned_name:
            parts = cleaned_name.split(keyword, maxsplit=1)
            cleaned_name = parts[0].strip()
            return cleaned_name
            
    return cleaned_name

OPF_MAPPING = {
    # CIS - Сначала более длинные и специфичные, затем более короткие и общие
    # Комбинированные и длинные формы
    r'\b(Иностранное\s+предприятие\s+общество\s+с\s+ограниченной\s+ответственностью|ИП\s+ООО)\b': 'ИП ООО',
    r'\b(Акционерное\s+Общество\s+Совместное\s+Предприятие|АО\s+СП)\b': 'АО СП',
    r'\b(Общество\s+с\s+Ограниченной\s+Ответственностью|Общество\s+Ограниченной\s+Ответственностью|Общество\s+с\s+ограниченной\s+отстветственностью|Общество\s+с\s+ограниченной\s+ответсвенностью|OOO)\b': 'ООО',
    r'\b(Публичное\s+Акционерное\s+Общество)\b': 'ПАО',
    r'\b(Закрытое\s+Акционерное\s+Общество|ЗАО)\b': 'ЗАО',
    r'\b(Индивидуальный\s+Предприниматель)\b': 'ИП',
    r'\b(Общество\s+с\s+Дополнительной\s+Ответственностью|ОДО)\b': 'ОДО',
    r'\b(Федеральное\s+Государственное\s+Унитарное\s+Предприятие|ФГУП)\b': 'ФГУП',
    r'\b(Государственное\s+Унитарное\s+Предприятие|ГУП)\b': 'ГУП',
    r'\b(Федеральное\s+Государственное\s+Предприятие|ФГП)\b': 'ФГП',
    r'\b(Муниципальное\s+Унитарное\s+Предприятие|МУП)\b': 'МУП',
    r'\b(Непубличное\s+Акционерное\s+Общество|НАО)\b': 'НАО',
    r'\b(Товарищество\s+с\s+Ограниченной\s+Ответственностью|Товарищество\s+с\s+Ограниченной\s+Отвественностью|ТОО|TOO)\b': 'ТОО', # TOO уже было, но унифицируем написание
    r'\b(Акционерное\s+Общество\s+с\s+Закрытым\s+Акционерным\s+Капиталом|АО\s+ЗАО)\b': 'АО ЗАО',
    r'\b(Государственное\s+Предприятие|ГП)\b': 'ГП',
    r'\b(Республиканское\s+Унитарное\s+Предприятие|РУП)\b': 'РУП',
    r'\b(Коммерческое\s+Унитарное\s+Предприятие|КУП)\b': 'КУП',
    r'\b(Коллективное\s+Предприятие|КП)\b': 'КП',
    r'\b(Частное\s+Предприятие|Частное\s+Предриятие|ЧП)\b': 'ЧП',
    r'\b(Частная\s+Компания|ЧК)\b': 'ЧК',
     r'\b(ОсОО)\b': 'ОсОО', # Явное указание для ОсОО, чтобы не путать с ООО
    r'\b(Акционерное\s+Общество|ААТ)\b': 'ААТ', # ААТ - Акционердик Ачык Типтеги Коом (Кыргызстан)
    r'\b(Частное\s+торговое\s+унитарное\s+предприятие|ЧТУП)\b': 'ЧТУП',
    r'\b(Открытое\s+акционерное\s+общество|ОАО)\b': 'ОАО',
    r'\b(ЧАСТНОЕ\s+ТОРГОВО-ПРОИЗВОДСТВЕННОЕ\s+УНИТАРНОЕ\s+ПРЕДПРИЯТИЕ|ЧТПУП)\b': 'ЧТПУП',
    r'\b(Частное\s+производственно-торговое\s+унитарное\s+предприятие|ЧПТУП)\b': 'ЧПТУП',
    r'\b(Частное\s+Унитарное\s+предприятие|ЧУП)\b': 'ЧУП',
    r'\b(Иностранное\s+унитарное\s+предприятие|ИУП)\b': 'ИУП',
    r'\b(Унитарное\s+предприятие|УП)\b': 'УП',
    r'\b(Филиал\s+компании|Филиал\s+корпорации|Филиал\s+партнерства\s+с\s+ограниченной\s+ответственностью|ФИЛИАЛ|ФЛ|Ф-Л)\b': 'ФИЛИАЛ', # Для филиалов

    # Базовые ОПФ (должны идти после более специфичных, если есть пересечения)
    r'\b(ООО|OOO)\b': 'ООО',
    r'\b(АО)\b': 'АО',
    r'\b(ИП)\b': 'ИП',
    r'\b(ЧДММ)\b': 'ЧДММ',
    r'\b(ИЧП)\b': 'ИЧП',
    
    # INTL
    # LLC / Limited Liability Company
    r'\b(limited\s+liability\s+company|l[.\s]*l[.\s]*c)\b': 'LLC',
    # LTD / Limited
    r'\b(limited|l[.\s]*t[.\s]*d)\b': 'LTD',
    # GmbH (немецкая форма)
    r'\b(gesellschaft\s+mit\s+beschränkter\s+haftung|g[.\s]*m[.\s]*b[.\s]*h)\b': 'GMBH',
    # CO / Company
    r'\b(c[.\s]*o)\b': 'CO',
    # SA / S.A. (французская/испанская форма)
    r'\b(s[.\s]*a)\b': 'SA',
    # SRL / S.R.L. (румынская/итальянская форма)
    r'\b(s[.\s]*r[.\s]*l)\b': 'SRL',
    # SPA (итальянская форма)
    r'\b(s[.\s]*p[.\s]*a)\b': 'SPA',
    # AS (норвежская/турецкая форма)
    r'\b(a[.\s]*s)\b': 'AS',
    # D.O.O. (балканская форма)
    r'\b(d[.\s]*o[.\s]*o)\b': 'D.O.O.',
    # D.D.O.O. (редкая форма)
    r'\b(d[.\s]*d[.\s]*o[.\s]*o)\b': 'D.D.O.O.',
    # SP Z O.O. (польская форма)
    r'\b(s[.\s]*p[.\s]*z[.\s]*o[.\s]*o|spolka\s+z\s+oo|spolka\s+z\s+ograniczona\s+odpowiedzialnoscia)\b': 'SP ZOO',
    # FZC (Free Zone Company, ОАЭ)
    r'\b(free\s+zone\s+company|f[.\s]*z[.\s]*c)\b': 'FZC',
    # FZE (Free Zone Establishment, ОАЭ)
    r'\b(free\s+zone\s+establishment|f[.\s]*z[.\s]*e)\b': 'FZE',
    # Sanayi ve Ticaret A.Ş. (турецкая форма)
    r'\b(sanayi\s+ve\s+ticaret\s+a\.?\s*s\.?|sanayi\s+ve\s+ticaret\s+a\.?\s*ş\.?)\b': 'Sanayi ve Ticaret A.Ş.',
    # LLP (Limited Liability Partnership)
    r'\b(limited\s+liability\s+partnership|l[.\s]*l[.\s]*p)\b': 'LLP',
    # JSC (Joint Stock Company)
    r'\b(joint\s+stock\s+company|j[.\s]*s[.\s]*c)\b': 'JSC',
    # Corp. / Corporation
    r'\b(c[.\s]*o[.\s]*r[.\s]*p|corporation)\b': 'Corp.',
    # LP (Limited Partnership)
    r'\b(limited\s+partnership|l[.\s]*p)\b': 'LP',
    # GP (General Partnership)
    r'\b(general\s+partnership|g[.\s]*p)\b': 'GP',
    # Sole Proprietorship
    r'\b(sole\s+prop\.|sole\s+proprietorship|s[.\s]*p)\b': 'Sole prop.',
    # Nonprofit Corporation
    r'\b(nonprofit\s+corp\.|nonprofit\s+corporation)\b': 'Nonprofit corp.',
    # PLC (Public Limited Company)
    r'\b(public\s+limited\s+company|p[.\s]*l[.\s]*c)\b': 'PLC',
    # Sole Trader
    r'\b(sole\s+trader)\b': 'Sole Trader',
    # OPC (One Person Company)
    r'\b(one\s+person\s+company|o[.\s]*p[.\s]*c)\b': 'OPC',
    # INC (Incorporated)
    r'\b(incorporated|i[.\s]*n[.\s]*c)\b': 'INC',
    # SIA (латвийская форма)
    r'\b(s[.\s]*i[.\s]*a)\b': 'SIA',
    # S.R.O. 
    r'\b(s[.\s]*r[.\s]*o)\b': 'SRO',
    # S.L.
    r'\b(s[.\s]*l)\b': 'SL',
    # UAB
    r'\b(u[.\s]*a[.\s]*b)\b': 'UAB',
    # AG
    r'\b(a[.\s]*g)\b': 'AG',
    # MCHJ
    r'\b(m[.\s]*c[.\s]*h[.\s]*j)\b': 'MCHJ',
    # CJSC
    r'\b(c[.\s]*j[.\s]*s[.\s]*c)\b': 'CJSC',
}


# Список ОПФ-аббревиатур, которые могут "прилипать" к названию.
# Сортируем по длине от длинных к коротким, чтобы "ИП ООО" обработалось до "ООО".
# Используем значения из OPF_MAPPING, которые являются аббревиатурами.
# Или можно составить вручную/полуавтоматически.
STICKING_OPFS_CANDIDATES = sorted(
    list(set([
        "ИП ООО", "АО СП", "ОсОО", "ООО", "АО", "ИП", "ЗАО", "ПАО", "ТОО", "TOO" "ЧП", "ОАО", "ГУП", 
        "МУП", "РУП", "ЧТУП", "ЧУП", "ФЛ", "Ф-Л", "СП", # CIS
        "LTD", "LLC", "GMBH", "CO", "SA", "SPA", "SRL", "JSC", "LLP", "LP", "GP", "PLC" # INTL
    ])), 
    key=len, 
    reverse=True
)


def normalize_company(name):
    if pd.isna(name):
        return pd.Series([None, None], index=['ОПФ', 'Нормализованное_название'])

    processed_name = str(name)

    # 1. Предварительная обработка для "слипшихся" ОПФ (Проблема 2)
    # Вставляем пробел между ОПФ и кавычкой/буквой/цифрой
    # Это делается до приведения к нижнему регистру и удаления кавычек.
    for opf_abbr in STICKING_OPFS_CANDIDATES:
        escaped_opf = re.escape(opf_abbr)
        # Паттерн: ОПФ (как слово/слова), за которым сразу идет кавычка или буква/цифра.
        # (?i) для нечувствительности к регистру ОПФ, но [A-ZА-ЯЁ0-9] для начала имени чувствителен (если имя с большой буквы)
        # Однако, т.к. clean_and_extract потом всё равно приводит к lower(), можно и здесь упростить.
        # Главное - вставить пробел.
        # ОПФ + кавычка -> ОПФ + " " + кавычка
        processed_name = re.sub(r'(\b' + escaped_opf + r')([«"“])', r'\1 \2', processed_name, flags=re.IGNORECASE)
        # ОПФ + Буква/Цифра -> ОПФ + " " + Буква/Цифра
        processed_name = re.sub(r'(\b' + escaped_opf + r')([A-Za-zА-Яа-яЁё0-9])', r'\1 \2', processed_name, flags=re.IGNORECASE)
        
    # 2. Основная очистка и извлечение ключевых слов (поручения и т.д.)
    clean_name_str = clean_and_extract(processed_name) # Эта функция также приводит к lower()

    opf = None
    normalized_name_str = clean_name_str # Начинаем с полностью очищенной строки

    # 3. Извлечение ОПФ (Проблемы 1 и 3)
    # Сортируем OPF_MAPPING по длине регулярного выражения (ключа) в обратном порядке.
    # Это гарантирует, что более длинные и специфичные паттерны (например, "ИП ООО") будут проверяться раньше более коротких (например, "ООО").
    
    # Используем длину первого элемента группы в регулярке как ключ для сортировки, либо просто длину самой регулярки как прокси для специфичности.
    # `len(item[0])` (длина строки регулярного выражения) - хороший эвристический подход.
    sorted_opf_items = sorted(OPF_MAPPING.items(), key=lambda item: len(item[0]), reverse=True)

    for pattern_regex, mapped_opf_value in sorted_opf_items:
        # Ищем паттерн в уже очищенной и приведенной к нижнему регистру строке.
        # re.IGNORECASE здесь важен, т.к. паттерны в OPF_MAPPING могут содержать заглавные буквы.
        match = re.search(pattern_regex, normalized_name_str, flags=re.IGNORECASE)
        if match:
            opf = mapped_opf_value
            # Удаляем найденный ОПФ из названия. count=1 для одного замещения (обычно в начале).
            normalized_name_str = re.sub(pattern_regex, '', normalized_name_str, count=1, flags=re.IGNORECASE).strip()
            # Дополнительно убираем возможные оставшиеся дефисы или запятые в начале имени после удаления ОПФ
            normalized_name_str = re.sub(r'^[\s,-]+', '', normalized_name_str)
            break # ОПФ найден, выходим из цикла

    # 4. Финальная очистка имени компании от лишних символов (если остались)
    # clean_and_extract уже многое удалил, но могут остаться точки от S.P.A. и т.п.
    # или если ОПФ содержал точки, а паттерн нет.
    normalized_name_str = re.sub(r'[.,]', '', normalized_name_str) # Удаляем точки и запятые внутри
    normalized_name_str = re.sub(r'\s\s+', ' ', normalized_name_str).strip() # Нормализуем пробелы

    return pd.Series([opf, normalized_name_str], index=['ОПФ', 'Нормализованное_название'])


## Для отладки нормализатора
merged_df['exporter_name_orig'] = merged_df['exporter_name']
merged_df['importer_name_orig'] = merged_df['importer_name']

# Нормализуем обе колонки
for col in ['exporter_name', 'importer_name']:
    # Применяем новую функцию нормализации
    normalized_data = merged_df[col].apply(normalize_company)
    merged_df[f'{col}_opf'] = normalized_data['ОПФ']
    merged_df[col] = normalized_data['Нормализованное_название']


merged_df.to_excel(output_excel_path, index=False)

end_time = datetime.now()

logger.info(f'Время начала: {start_time}')
logger.info(f'Время окончания: {end_time}')
logger.info(f'Продолжительность: {end_time - start_time}')
logger.info('Готово: tagging_results.xlsx')