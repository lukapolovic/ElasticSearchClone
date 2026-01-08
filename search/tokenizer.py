from typing import Optional, Set
import unicodedata
import regex
from nltk.stem import WordNetLemmatizer

STOP_WORDS: Optional[Set[str]] = None

lemmatizer = WordNetLemmatizer()

def _get_stop_words():
    """
    Lazy load stop words from NLTK.
    This avoids crashing at import time (in Docker)
    """
    global STOP_WORDS
    if STOP_WORDS is not None:
        return STOP_WORDS
    
    from nltk.corpus import stopwords
    STOP_WORDS = set(stopwords.words("english"))
    return STOP_WORDS

def normalize_unicode(text):
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))

def clean_punctuation(text):
    return regex.sub(r"\p{P}+", " ", text)

def basic_token_filter(token):
    if len(token) < 2:
        return False
    if token in STOP_WORDS:
        return False
    if regex.fullmatch(r"\d+", token):
        return False
    return True

def tokenize(text, use_lemmatization=True):
    if not text:
        return []
    
    text = text.lower()

    text = normalize_unicode(text)

    text = clean_punctuation(text)

    tokens = text.split()

    stop_words = _get_stop_words()
    tokens = [t for t in tokens if t not in stop_words]

    if use_lemmatization:
        tokens = [lemmatizer.lemmatize(t, pos='v') for t in tokens]
        tokens = [lemmatizer.lemmatize(t, pos='n') for t in tokens]

    return tokens