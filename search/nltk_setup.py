from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path

import nltk


REQUIRED = [
    # (nltk.data.find path, downloader name)
    ("corpora/stopwords", "stopwords"),
    ("corpora/wordnet", "wordnet"),
    ("corpora/omw-1.4", "omw-1.4"),
]


class NLTKDataMissing(RuntimeError):
    pass


def _repo_root() -> Path:
    # .../ElasticSearchClone/search/nltk_setup.py -> repo root is parents[1]
    return Path(__file__).resolve().parents[1]


def _project_nltk_dir() -> Path:
    return _repo_root() / "nltk_data"


def _ensure_nltk_path(project_dir: Path) -> None:
    project_dir.mkdir(parents=True, exist_ok=True)
    project_dir_str = str(project_dir)
    if project_dir_str not in nltk.data.path:
        nltk.data.path.insert(0, project_dir_str)


def _extract_zip_to_corpora(project_dir: Path, package_name: str) -> None:
    """
    If nltk_data/corpora/<package>.zip exists, extract it to nltk_data/corpora/.
    This avoids the 'wordnet/wordnet' nesting caused by extracting into an already-named directory.
    """
    corpora_dir = project_dir / "corpora"
    zip_path = corpora_dir / f"{package_name}.zip"
    if not zip_path.exists():
        return

    corpora_dir.mkdir(parents=True, exist_ok=True)

    # Extract into corpora/ (not corpora/<package>/)
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(corpora_dir)


def _repair_nested_dir(project_dir: Path, package_name: str) -> None:
    """
    Repairs cases like:
      corpora/wordnet/wordnet/<files>  -> corpora/wordnet/<files>
      corpora/omw-1.4/omw-1.4/<files> -> corpora/omw-1.4/<files>

    We detect if corpora/<name>/<name>/ exists and move its contents up one level.
    """
    corpora_dir = project_dir / "corpora"
    outer = corpora_dir / package_name
    inner = outer / package_name

    if not inner.exists() or not inner.is_dir():
        return

    # Move inner contents up into outer
    for child in inner.iterdir():
        target = outer / child.name
        if target.exists():
            # remove existing target to avoid merge conflicts
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        shutil.move(str(child), str(target))

    # Remove now-empty inner directory
    try:
        inner.rmdir()
    except OSError:
        # If it isn't empty for some reason, fall back to full removal
        shutil.rmtree(inner, ignore_errors=True)


def _nltk_find_ok(path: str) -> bool:
    try:
        nltk.data.find(path)
        return True
    except LookupError:
        return False


def ensure_nltk_data() -> None:
    """
    Ensures NLTK resources exist in repo-local ./nltk_data and are in a usable layout.

    Behavior:
    - Always creates ./nltk_data if missing
    - Automatically downloads missing corpora into ./nltk_data
    - Automatically extracts any corpora zip files and repairs nested directory layout
    - Verifies WordNet can be loaded (not just present on disk)
    """
    project_dir = _project_nltk_dir()
    _ensure_nltk_path(project_dir)

    # 1) Determine what is missing according to nltk.data.find
    missing = [name for (find_path, name) in REQUIRED if not _nltk_find_ok(find_path)]

    # 2) Auto-download missing corpora into project_dir
    if missing:
        for name in missing:
            nltk.download(name, download_dir=str(project_dir), quiet=True)

    # 3) If corpora exist only as zips, extract them correctly into corpora/
    for _, name in REQUIRED:
        _extract_zip_to_corpora(project_dir, name)

    # 4) Repair double-nesting if it happened (wordnet/wordnet, omw-1.4/omw-1.4)
    for _, name in REQUIRED:
        _repair_nested_dir(project_dir, name)

    # 5) Final verification: ensure nltk sees the resources now
    still_missing = [name for (find_path, name) in REQUIRED if not _nltk_find_ok(find_path)]
    if still_missing:
        raise NLTKDataMissing(
            "NLTK resources still missing after auto-download/extract/repair: "
            + ", ".join(still_missing)
            + f". Expected under: {project_dir}"
        )

    # 6) Strong verification: actually load wordnet (this catches broken layouts early)
    try:
        from nltk.corpus import wordnet as wn  # noqa: F401
        # touch the corpus to force file access
        next(wn.all_synsets())
    except Exception as e:
        raise NLTKDataMissing(
            "WordNet appears present but failed to load. "
            f"Check contents under: {project_dir / 'corpora' / 'wordnet'}"
        ) from e
