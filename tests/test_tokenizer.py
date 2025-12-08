import pytest
from search.tokenizer import tokenize, normalize_unicode, clean_punctuation, basic_token_filter, STOP_WORDS


# -------------------------
# Helper unit tests
# -------------------------
def test_normalize_unicode():
    assert normalize_unicode("café") == "cafe"
    assert normalize_unicode("naïve") == "naive"
    assert normalize_unicode("coöperate") == "cooperate"


def test_clean_punctuation():
    assert clean_punctuation("hello, world!") == "hello  world "
    assert clean_punctuation("no.punctuation?here!") == "no punctuation here "


def test_basic_token_filter():
    assert basic_token_filter("a") is False  # too short
    assert basic_token_filter("the") is False  # stopword
    assert basic_token_filter("123") is False  # numbers
    assert basic_token_filter("hello") is True


# -------------------------
# Tokenizer main tests
# -------------------------
def test_empty_input():
    assert tokenize("") == []
    assert tokenize(None) == []


def test_lowercasing():
    assert tokenize("HELLO WORLD") == ["hello", "world"]


def test_stopwords_removal():
    assert "is" not in tokenize("this is a test")
    assert tokenize("this is a test") == ["test"]


def test_short_tokens_removed():
    assert tokenize("a i an") == []


def test_number_tokens_kept():
    assert tokenize("2025 123 hello") == ["2025", "123", "hello"]


def test_lemmatization():
    tokens = tokenize("running cars eat cats")
    # Only basic lemmatization for nouns/verbs
    # 'running' -> 'run', 'cars' -> 'car', 'eat' -> 'eat', 'cats' -> 'cat'
    expected = {"run", "car", "eat", "cat"}
    assert set(tokens) >= expected

def test_complex_sentence():
    input_text = "Café running in 2025, hello world!"
    output = tokenize(input_text)
    # Numbers are kept, accents removed, verbs/nouns lemmatized, stopwords removed
    expected = ["cafe", "run", "2025", "hello", "world"]
    assert output == expected
