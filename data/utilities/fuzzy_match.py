from rapidfuzz import process, fuzz

def fuzzy_match(value: str, choices: list[str], threshold: int = 90) -> str | None:
    """
    Выполняет нечеткое сопоставление строки с возможными вариантами.

    Args:
        value (str): строка, которую нужно сопоставить
        choices (list[str]): список кандидатов
        threshold (int): минимальное значение сходства (0-100)

    Returns:
        str | None: ближайший кандидат или None, если ничего не найдено
    """
    if not value or not choices:
        return None

    match, score, _ = process.extractOne(value, choices, scorer=fuzz.ratio)
    return match if score >= threshold else None
