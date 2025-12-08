import unicodedata
import regex
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

nltk.download('stopwords', quiet=True)
STOP_WORDS = set(stopwords.words('english'))

nltk.download("wordnet", quiet=True)
nltk.download("omw-1.4", quiet=True)
lemmatizer = WordNetLemmatizer()

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

    tokens = [t for t in tokens if t not in STOP_WORDS]

    if use_lemmatization:
        tokens = [lemmatizer.lemmatize(t) for t in tokens]

    return tokens