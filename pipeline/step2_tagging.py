# steps/step2_tagging.py

# Для запуска из venv
# pipeline\venv\scripts\activate
# python pipeline\step2_tagging.py
from datetime import datetime
import pandas as pd
import re
import unicodedata
import pymorphy2
from nltk import download
from nltk.corpus import stopwords

from utils.io import read_excel_file, save_to_excel_file
from utils.logging_utils import setup_logger

DEBUG = True  # включи на один прогон: логируем схему и самотест

logger = setup_logger()
start_time = datetime.now()
logger.info('--- Step 2: Текстовая классификация (negation + mixed-script) ---')

# --- Пути ---
INPUT_PATH = 'data/st1_cleaned/st1.xlsx'
OUTPUT_PATH = 'data/st2_tagged/st2.xlsx'
TAGS_PATH = 'data/utilities/word_tagger/tagged_words.csv'

# --- Загрузка ресурсов ---
morph = pymorphy2.MorphAnalyzer()
download('stopwords')
stop_words = set(stopwords.words("russian"))

# --- Загрузка данных ---
df = read_excel_file(INPUT_PATH)
tags_df = pd.read_csv(TAGS_PATH)
approved = set(tags_df[tags_df["tag"] == "approved"]["word"].str.lower())
rejected = set(tags_df[tags_df["tag"] == "rejected"]["word"].str.lower())

# =========================================================
# НОРМАЛИЗАЦИЯ СМЕШАННЫХ ТОКЕНОВ (КИР/ЛАТ) — безопасная
# =========================================================

LAT_TO_CYR = {
    'A':'А','a':'а','B':'В','E':'Е','e':'е','K':'К','k':'к',
    'M':'М','m':'м','H':'Н','h':'н','O':'О','o':'о','P':'Р','p':'р',
    'C':'С','c':'с','T':'Т','t':'т','X':'Х','x':'х','Y':'У','y':'у'
}
CYR_TO_LAT = {v: k for k, v in LAT_TO_CYR.items()}

def _is_cyr(ch: str) -> bool:
    return 'CYRILLIC' in unicodedata.name(ch, '')

def _is_lat(ch: str) -> bool:
    return 'LATIN' in unicodedata.name(ch, '')

TOKEN_RE = re.compile(r"[0-9A-Za-zА-Яа-яЁё\-_/\.]+")
SKIP_PATTERNS = [
    re.compile(r"[A-Za-z]{2,}\d+"),   # DN50, PN16, G3/4, M20x1.5
    re.compile(r"[A-Z]{3,}"),         # FANUC, PLC, CNC, SIEMENS
    re.compile(r"\d+[A-Za-z\-]+"),    # LS59-1, 40Cr, 20#, 12X18H10T
    re.compile(r"https?://|www\."),   # URL
]

def _token_should_be_skipped(tok: str) -> bool:
    return any(p.search(tok) for p in SKIP_PATTERNS)

def _normalize_mixed_token(tok: str) -> str:
    lat = sum(_is_lat(ch) for ch in tok)
    cyr = sum(_is_cyr(ch) for ch in tok)
    if lat == 0 or cyr == 0 or _token_should_be_skipped(tok):
        return tok
    to_cyr = cyr >= lat
    out = []
    for ch in tok:
        if to_cyr and _is_lat(ch) and ch in LAT_TO_CYR:
            out.append(LAT_TO_CYR[ch])
        elif not to_cyr and _is_cyr(ch) and ch in CYR_TO_LAT:
            out.append(CYR_TO_LAT[ch])
        else:
            out.append(ch)
    return "".join(out)

def normalize_confusables(text: str) -> str:
    if not isinstance(text, str):
        return text
    text = unicodedata.normalize('NFKC', text)
    out, last = [], 0
    for m in TOKEN_RE.finditer(text):
        s, e = m.span()
        out.append(text[last:s])
        out.append(_normalize_mixed_token(m.group(0)))
        last = e
    out.append(text[last:])
    return "".join(out)

# =========================================================
# ЛЕММАТИЗАЦИЯ
# =========================================================

WORD_RE = re.compile(r"\b[а-яА-Яa-zA-ZЁё]+(?:-[а-яА-Яa-zA-ZЁё]+)*\b")

def extract_lemmas(text: str) -> list[str]:
    if not isinstance(text, str):
        return []
    text = normalize_confusables(text).lower()
    tokens = WORD_RE.findall(text)
    lemmas = []
    for token in tokens:
        if token not in stop_words and len(token) > 1:
            lemmas.append(morph.parse(token)[0].normal_form)
    return lemmas

# =========================================================
# ОТРИЦАНИЕ (NEGATION) ДЛЯ REJECTED-ТЕРМИНОВ
# =========================================================

SENT_SPLIT_RE = re.compile(r"[.;]|(?:\s—\s)|(?:\s-\s)")

NEGATION_CONTEXT_TEMPLATES = [
    r"\bне\s+явля\w*\s+{term}\w*\b",               # не являются клапаном
    r"\bи\s+не\s+явля\w*\s+{term}\w*\b",           # и не являются клапаном
    r"\bне\s+\w{0,3}\s*явля\w*\s+{term}\w*\b",     # универсальнее
    r"\bне\b[^,;:.]{0,80}\b{term}\w*\b",           # не ... клапан(ы)
    r"\bбез\b[^,;:.]{0,80}\b{term}\w*\b",
    r"\bне\s+содерж\w*\b[^,;:.]{0,80}\b{term}\w*\b",
    r"\bкроме\b[^,;:.]{0,80}\b{term}\w*\b",
    r"\bза\s+исключением\b[^,;:.]{0,80}\b{term}\w*\b",
    r"\bисключая\b[^,;:.]{0,80}\b{term}\w*\b",
    r"\b{term}\w*\b[^,;:.]{0,80}\bне\s+(предусмотр\w*|относ\w*|явля\w*)\b",
]

NEGATOR_TOKENS = {"не", "ни", "без"}
EXCEPTION_TOKENS = {"кроме", "исключая"}

def _spans_overlap(a, b) -> bool:
    return not (a[1] <= b[0] or b[1] <= a[0])

def _term_occurrences(text: str, term: str):
    rx = re.compile(rf"\b{re.escape(term)}\w*\b")
    return [m.span() for m in rx.finditer(text)]

def _find_negation_hits(text: str, term: str):
    hits = []
    term_esc = re.escape(term)
    for i, patt in enumerate(NEGATION_CONTEXT_TEMPLATES, start=1):
        rx = re.compile(patt.replace("{term}", term_esc))
        for m in rx.finditer(text):
            hits.append((m.start(), m.end(), f"NEG_{i}"))
    return hits

def _is_occurrence_negated(text: str, span, term: str, neg_hits):
    for s, e, name in neg_hits:
        if _spans_overlap(span, (s, e)):
            return True, name

    sent_bounds = [0]
    for m in SENT_SPLIT_RE.finditer(text):
        sent_bounds.append(m.end())
    sent_bounds.append(len(text))
    sent_bounds = sorted(set(sent_bounds))

    sent_start, sent_end = 0, len(text)
    for i in range(len(sent_bounds) - 1):
        if sent_bounds[i] <= span[0] < sent_bounds[i + 1]:
            sent_start, sent_end = sent_bounds[i], sent_bounds[i + 1]
            break

    sent = text[sent_start:sent_end]
    toks = [t.lower() for t in WORD_RE.findall(sent)]

    term_idx = None
    for i, tok in enumerate(toks):
        if tok.startswith(term):
            term_idx = i
            break
    if term_idx is None:
        return False, ""

    left = toks[max(0, term_idx - 6): term_idx]
    if any(t in NEGATOR_TOKENS for t in left):
        return True, "NEG_FALLBACK_LEFT_NEGATOR"
    if any(t in EXCEPTION_TOKENS for t in left):
        return True, "NEG_FALLBACK_EXCEPTION"
    if "не" in left:
        return True, "NEG_FALLBACK_NE_TERM"
    return False, ""

def filter_rejected_with_negation(raw_text: str, rejected_terms: list[str]):
    """
    Возвращает dict:
      - rejected_pos: запрещённые термины в ПОЛОЖИТЕЛЬНОМ контексте
      - rejected_neg: запрещённые термины, встретившиеся ТОЛЬКО в отрицании
      - triggers: список срабатываний правил (для дебага)
    """
    res = {"rejected_pos": [], "rejected_neg": [], "triggers": []}
    if not raw_text or not isinstance(raw_text, str):
        return res
    text = normalize_confusables(raw_text).lower()

    for term in sorted(set(rejected_terms)):
        occs = _term_occurrences(text, term)
        if not occs:
            continue
        neg_hits = _find_negation_hits(text, term)
        neg_count = pos_count = 0
        local_triggers = []
        for occ in occs:
            is_neg, trig = _is_occurrence_negated(text, occ, term, neg_hits)
            if is_neg:
                neg_count += 1
                if trig:
                    local_triggers.append(f"{term}:{trig}")
            else:
                pos_count += 1
        if pos_count == 0 and neg_count > 0:
            res["rejected_neg"].append(term)
            res["triggers"].extend(local_triggers)
        elif pos_count > 0:
            res["rejected_pos"].append(term)
            res["triggers"].extend(local_triggers)
    return res

# =========================================================
# КЛАССИФИКАЦИЯ
# =========================================================

def classify_text(text: str) -> dict:
    if pd.isna(text):
        return {
            "classification": "не определено",
            "reason": "",
            "matched_approved": [],
            "matched_rejected": [],
            "matched_rejected_negated": [],
            "negation_triggers": [],
        }

    raw = str(text)
    normalized = normalize_confusables(raw).lower()
    lemmas = extract_lemmas(raw)

    matched_approved = [l for l in lemmas if l in approved]

    # кандидаты в rejected = (по леммам) ∪ (по прямому сканированию нормализованного текста)
    rej_candidates = set(l for l in lemmas if l in rejected)
    rej_candidates |= {term for term in rejected if re.search(rf"\b{re.escape(term)}\w*\b", normalized)}
    rej_candidates = sorted(rej_candidates)

    neg_res = filter_rejected_with_negation(raw, rej_candidates)
    rejected_pos = neg_res["rejected_pos"]
    rejected_neg = neg_res["rejected_neg"]
    neg_triggers = neg_res["triggers"]

    if rejected_pos:
        return {
            "classification": "исключено",
            "reason": rejected_pos[0],
            "matched_approved": matched_approved,
            "matched_rejected": rejected_pos,
            "matched_rejected_negated": rejected_neg,
            "negation_triggers": neg_triggers,
        }
    if matched_approved:
        return {
            "classification": "одобрено",
            "reason": matched_approved[0],
            "matched_approved": matched_approved,
            "matched_rejected": [],
            "matched_rejected_negated": rejected_neg,
            "negation_triggers": neg_triggers,
        }
    return {
        "classification": "не определено",
        "reason": "",
        "matched_approved": matched_approved,
        "matched_rejected": [],
        "matched_rejected_negated": rejected_neg,
        "negation_triggers": neg_triggers,
    }

# =========================================================
# ПРИМЕНЕНИЕ
# =========================================================

# Разворачиваем dict в столбцы надёжно
meta = df["prod_details"].astype(str).apply(classify_text).apply(pd.Series)

# Merge (перезаписываем одноимённые колонки, если вдруг были)
df = pd.concat([df.drop(columns=[c for c in meta.columns if c in df.columns], errors="ignore"), meta], axis=1)

# Предпросмотр нормализованного текста для контроля
df["text_norm_preview"] = df["prod_details"].astype(str).apply(normalize_confusables).str[:200]

# Логируем схему
if DEBUG:
    logger.info(f"Столбцы после классификации: {list(df.columns)}")
    # Самотест на твоей фразе
    sample = "АРМАТУРА ТРУБОПРОВОДНAЯ:КРАНЫ ШАРОВЫЕ, ДЛЯ УСТАНОВКИ НА ТРУБОПРОВОДАХ ВОДЫ И ГАЗА, КОРПУСЫ ИЗГОТОВЛЕНЫ ИЗ СТАЛИ МАРКИ СТ.20; И ЛАТУНИ МАРКИ ЛС59-1 НЕ СОДЕРЖИТ УПЛОТНЕНИЙ СИЛЬФОННОГО ТИПА И НЕ ЯВЛЯЮТСЯ КЛАПАНОМ"
    test = classify_text(sample)
    logger.info(f"SAMOTEST classification={test['classification']}; reason={test['reason']}; "
                f"rej_pos={test['matched_rejected']}; rej_neg={test['matched_rejected_negated']}; "
                f"triggers={test['negation_triggers']}")

# --- Сохранение ---
save_to_excel_file(df, OUTPUT_PATH)

end_time = datetime.now()
logger.info(f'Время начала: {start_time}')
logger.info(f'Время окончания: {end_time}')
logger.info(f'Продолжительность: {end_time - start_time}')
logger.info(f'Готово: {OUTPUT_PATH}')
