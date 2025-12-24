import ast
import json
import os
import pandas as pd

CSV_PATH = os.path.join(os.path.dirname(__file__), "data", "25kMovies.csv")
OUT_PATH = os.path.join(os.path.dirname(__file__), "data", "25kMovies.cleaned.jsonl")


def parse_list(value):
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    if not isinstance(value, str):
        return []
    value = value.strip()
    if not value:
        return []
    try:
        parsed = ast.literal_eval(value)
    except Exception:
        return []
    if isinstance(parsed, list):
        return parsed
    return []


def transform():
    df = pd.read_csv(CSV_PATH)

    df = df.rename(
        columns={
            "movie title": "title",
            "Overview": "description",
            "Top 5 Casts": "cast",
            "Generes": "genres",
            "Rating": "rating",
            "Director": "director",
            "path": "path",
        }
    )

    df["year"] = pd.to_numeric(df["year"], errors="coerce").abs()
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    df["genres"] = df["genres"].apply(parse_list)
    df["cast"] = df["cast"].apply(parse_list)

    df = df.dropna(subset=["title", "year", "rating"]).reset_index(drop=True)
    df["id"] = df.index + 1
    df = df[
        [
            "id",
            "title",
            "year",
            "genres",
            "description",
            "cast",
            "director",
            "rating",
        ]
    ]
    df["year"] = df["year"].astype(int).astype(str)
    df["rating"] = df["rating"].astype(float).astype(str)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        for record in df.to_dict(orient="records"):
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    transform()
