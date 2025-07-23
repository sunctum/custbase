import pandas as pd
import re
import pymorphy2
from nltk.corpus import stopwords
from nltk import download
from collections import defaultdict
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
start_time = datetime.now()

logging.info('Начало работы Step 2')

# Загрузка ресурсов
morph = pymorphy2.MorphAnalyzer()
download('stopwords')
stop_words = set(stopwords.words("russian"))

input_excel_path = 'data/st1_cleaned/st1.xlsx'
output_excel_path = 'data/st2_tagged/st2.xlsx'
tags_csv_path = 'data/utilities/word_tagger/tagged_words.csv'

# Загрузка списка слов
tags_df = pd.read_csv(tags_csv_path)
approved = set(tags_df[tags_df["tag"] == "approved"]["word"].str.lower())
rejected = set(tags_df[tags_df["tag"] == "rejected"]["word"].str.lower())

df = pd.read_excel(input_excel_path)

# Функция препроцессинга
def extract_lemmas(text):
    tokens = re.findall(r"\b[а-яА-Яa-zA-Z]+\b", text.lower())
    lemmas = []
    for token in tokens:
        if token not in stop_words and len(token) > 1:
            lemma = morph.parse(token)[0].normal_form
            lemmas.append(lemma)
    return lemmas

# Инициализация
classifications = []
reasons = []
approved_matches = []
rejected_matches = []

for index, row in df.iterrows():
    text = row["prod_details"]
    if pd.isna(text):
        classifications.append("не определено")
        reasons.append("")
        approved_matches.append([])
        rejected_matches.append([])
        continue

    lemmas = extract_lemmas(text)
    matched_approved = [lemma for lemma in lemmas if lemma in approved]
    matched_rejected = [lemma for lemma in lemmas if lemma in rejected]

    if matched_rejected:
        classification = "исключено"
        reason = matched_rejected[0]
    elif matched_approved:
        classification = "одобрено"
        reason = matched_approved[0]
    else:
        classification = "не определено"
        reason = ""

    classifications.append(classification)
    reasons.append(reason)
    approved_matches.append(matched_approved)
    rejected_matches.append(matched_rejected)

# Объединение с исходным DataFrame
df["classification"] = classifications
df["reason"] = reasons
df["matched_approved"] = approved_matches
df["matched_rejected"] = rejected_matches

df.to_excel(output_excel_path, index=False)

end_time = datetime.now()

logger.info(f'Время начала: {start_time}')
logger.info(f'Время окончания: {end_time}')
logger.info(f'Продолжительность: {end_time - start_time}')
logger.info("Готово: tagging_results.xlsx")