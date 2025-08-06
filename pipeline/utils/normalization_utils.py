# utils/io.py
import re
import pandas as pd

# --- Очистка названия компании ---
def clean_and_extract(name: str) -> str:
    cleaned_name = re.sub(r'[",«»()&?<>“”|/-]', '', name)
    cleaned_name = re.sub(r"'", '', cleaned_name)
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name).strip().lower()

    keywords_after = ["по поручению", "по поруч", "по пручению", "для", "b/o", "by order", "by"]
    keywords_before = ["через"]

    for keyword in keywords_after:
        if keyword in cleaned_name:
            return cleaned_name.split(keyword, maxsplit=1)[1].strip()

    for keyword in keywords_before:
        if keyword in cleaned_name:
            return cleaned_name.split(keyword, maxsplit=1)[0].strip()

    return cleaned_name

# --- Список склеенных ОПФ ---
STICKING_OPFS_CANDIDATES = sorted(
    list(set([
        "ИП ООО", "АО СП", "ОсОО", "ООО", "АО", "ИП", "ЗАО", "ПАО", "ТОО", "TOO", "ЧП", "ОАО", "ГУП", 
        "МУП", "РУП", "ЧТУП", "ЧУП", "ФЛ", "Ф-Л", "СП",
        "LTD", "LLC", "GMBH", "CO", "SA", "SPA", "SRL", "JSC", "LLP", "LP", "GP", "PLC"
    ])),
    key=len,
    reverse=True
)

# --- Словарь паттернов ОПФ ---
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
    # STI (Şirketi)
    r'\b(s[.\s]*t[.\s]*i)\b': 'STI'
}

# --- Основная функция нормализации ---
def normalize_company(name: str) -> pd.Series:
    if pd.isna(name):
        return pd.Series([None, None], index=['ОПФ', 'Нормализованное_название'])

    processed_name = str(name)

    for opf in STICKING_OPFS_CANDIDATES:
        escaped_opf = re.escape(opf)
        processed_name = re.sub(r'(\b' + escaped_opf + r')([«"“])', r'\1 \2', processed_name, flags=re.IGNORECASE)
        processed_name = re.sub(r'(\b' + escaped_opf + r')([A-Za-zА-Яа-яЁё0-9])', r'\1 \2', processed_name, flags=re.IGNORECASE)

    clean_name_str = clean_and_extract(processed_name)
    opf = None
    normalized_name_str = clean_name_str

    sorted_opf_items = sorted(OPF_MAPPING.items(), key=lambda item: len(item[0]), reverse=True)
    for pattern_regex, mapped_opf in sorted_opf_items:
        if re.search(pattern_regex, normalized_name_str, flags=re.IGNORECASE):
            opf = mapped_opf
            normalized_name_str = re.sub(pattern_regex, '', normalized_name_str, count=1, flags=re.IGNORECASE).strip()
            normalized_name_str = re.sub(r'^[\s,-]+', '', normalized_name_str)
            break

    normalized_name_str = re.sub(r'[.,]', '', normalized_name_str)
    normalized_name_str = re.sub(r'\s\s+', ' ', normalized_name_str).strip()

    return pd.Series([opf, normalized_name_str], index=['ОПФ', 'Нормализованное_название'])
