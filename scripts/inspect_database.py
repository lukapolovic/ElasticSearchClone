import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

# Step 1: Load the CSV
df = pd.read_csv('/Users/Luka/Documents/Stvari za faks/Masters/1. Semestar/ElasticSearchClone/scripts/data/25kMovies.csv')

# Step 2: Basic Stats
print(f"Number of rows: {(len(df))}")
print(f"Column names: {df.columns.tolist()}")

# Step 3: Check for missing values
null_counts = df.isnull().sum()
empty_counts = (df == '').sum()
print(f"Missing values per column: {null_counts}")
print(f"Empty strings per column: {empty_counts}")

# Step 4: Examine specific columns
print("First 5 entires of the dataset:")
print(df.head(5))

# Step 5: Identify data issues
expected_fields = {"id", "title", "year", "genres", "description", "cast", "director", "rating"}
normalized_columns = {col: col.strip().lower() for col in df.columns}
normalized_set = set(normalized_columns.values())
normalized_to_originals = {}
for original, normalized in normalized_columns.items():
    normalized_to_originals.setdefault(normalized, []).append(original)

missing_fields = sorted(expected_fields - normalized_set)
extra_fields = sorted(normalized_set - expected_fields)

list_like_cols = []
mixed_numeric_cols = []
scaled_numeric_cols = []
year_issue_cols = []

for col in df.columns:
    series = df[col]
    if series.dtype != object:
        continue

    non_null = series.dropna().astype(str).str.strip()
    if non_null.empty:
        continue

    if non_null.str.startswith("[").any() and non_null.str.endswith("]").any():
        list_like_cols.append(col)

    if non_null.str.contains(r"(?i)^\d+(\.\d+)?[KMB]$", regex=True).any():
        scaled_numeric_cols.append(col)

    numeric = pd.to_numeric(non_null, errors="coerce")
    numeric_count = numeric.notna().sum()
    if 0 < numeric_count < len(non_null):
        mixed_numeric_cols.append(col)
    elif numeric_count == len(non_null):
        if non_null.str.contains(r"[^0-9\.\-]", regex=True).any():
            mixed_numeric_cols.append(col)

    if "year" in normalized_columns[col]:
        year_numeric = pd.to_numeric(non_null, errors="coerce")
        if (year_numeric < 0).any() or year_numeric.isna().any():
            year_issue_cols.append(col)

field_sources = {
    "id": ["id", "path"],
    "title": ["title", "movie title"],
    "year": ["year"],
    "genres": ["genres", "generes"],
    "description": ["description", "overview", "plot kyeword", "plot keyword"],
    "cast": ["cast", "top 5 casts"],
    "director": ["director"],
    "rating": ["rating", "user rating"],
}

transform_notes = []
rename_map = {}
for field, candidates in field_sources.items():
    source_norm = next((name for name in candidates if name in normalized_set), None)
    if not source_norm:
        transform_notes.append(f"Missing source for '{field}'.")
        continue

    source_originals = normalized_to_originals.get(source_norm, [])
    if not source_originals:
        transform_notes.append(f"Missing original column for '{field}'.")
        continue

    source = source_originals[0]
    if source_norm != field or source != field:
        rename_map[source] = field
        transform_notes.append(f"Rename '{source}' -> '{field}'.")

    if field == "id" and source_norm == "path":
        transform_notes.append("Extract IMDb id from 'path' (e.g., /title/tt1234567/ -> 1234567).")

    if source in list_like_cols:
        transform_notes.append(f"Parse list-like values in '{source}' into lists.")

    if source in mixed_numeric_cols:
        transform_notes.append(f"Clean and convert numeric values in '{source}'.")

    if source in scaled_numeric_cols:
        transform_notes.append(f"Scale K/M/B suffix values in '{source}' to numbers.")

    if source in year_issue_cols:
        transform_notes.append(f"Fix invalid year values in '{source}' (negative or missing).")

print("Transformations needed:", transform_notes)
print("Columns to map/rename:", sorted(normalized_columns.values()))
print("Missing model fields:", missing_fields)
print("Extra/unmapped fields:", extra_fields)
print("List-like columns (parse to lists):", list_like_cols)
print("Mixed numeric columns (clean/convert):", mixed_numeric_cols)
print("Scaled numeric columns (K/M/B suffix):", scaled_numeric_cols)
print("Year columns with invalid values:", year_issue_cols)