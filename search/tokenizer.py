import string

def tokenize(text):
    if not text:
        return []
    
    lower_case_text = text.lower()

    wo_punctuation = lower_case_text.translate(str.maketrans('', '', string.punctuation))

    split_text = wo_punctuation.split()

    tokens = [t for t in split_text if t]

    return tokens

