import unicodedata
import regex
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords', quiet=True)
STOP_WORDS = set(stopwords.words('english'))

def tokenize(text):
    if not text:
        return []
    
    text = text.lower()

    normalized = unicodedata.normalize("NFKD", text)
    no_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))

    no_punct = regex.sub(r"\p{P}+", " ", no_accents)

    tokens = no_punct.split()

    filtered_tokens = [t for t in tokens if t not in STOP_WORDS]

    return filtered_tokens