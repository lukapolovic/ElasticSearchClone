from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path
from contextlib import contextmanager

import nltk

REQUIRED = [
    ("corpora/stopwords", "stopwords"),
    ("corpora/wordnet", "wordnet"),
    ("corpora/omw-1.4", "omw-1.4"),
]


class NLTKDataMissing(RuntimeError):
    pass


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _project_nltk_dir() -> Path:
    return _repo_root() / "nltk_data"


def _ensure_nltk_path(project_dir: Path) -> None:
    project_dir.mkdir(parents=True, exist_ok=True)
    project_dir_str = str(project_dir)
    if project_dir_str not in nltk.data.path:
        nltk.data.path.insert(0, project_dir_str)


def _extract_zip_to_corpora(project_dir: Path, package_name: str) -> None:
    corpora_dir = project_dir / "corpora"
    zip_path = corpora_dir / f"{package_name}.zip"
    if not zip_path.exists():
        return

    corpora_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(corpora_dir)


def _repair_nested_dir(project_dir: Path, package_name: str) -> None:
    corpora_dir = project_dir / "corpora"
    outer = corpora_dir / package_name
    inner = outer / package_name

    if not inner.exists() or not inner.is_dir():
        return

    for child in inner.iterdir():
        target = outer / child.name
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        shutil.move(str(child), str(target))

    try:
        inner.rmdir()
    except OSError:
        shutil.rmtree(inner, ignore_errors=True)


def _nltk_find_ok(path: str) -> bool:
    try:
        nltk.data.find(path)
        return True
    except LookupError:
        return False


@contextmanager
def _acquire_lock(project_dir: Path):
    """
    Cross-process lock for NLTK setup.
    Works on macOS/Linux via fcntl.
    On platforms without fcntl, it becomes a no-op (fine for your current environment).
    """
    lock_path = project_dir / ".nltk_setup.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    f = open(lock_path, "a+", encoding="utf-8")
    try:
        try:
            import fcntl  # macOS/Linux
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        except Exception:
            # If fcntl isn't available, proceed without a lock.
            # (On macOS it is available.)
            pass
        yield
    finally:
        try:
            import fcntl
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        f.close()


def _delete_corpus(project_dir: Path, name: str) -> None:
    """
    Remove both extracted dir and zip (if present). Used for repair.
    """
    corpora_dir = project_dir / "corpora"
    extracted = corpora_dir / name
    zipped = corpora_dir / f"{name}.zip"

    if extracted.exists():
        shutil.rmtree(extracted, ignore_errors=True)
    if zipped.exists():
        try:
            zipped.unlink()
        except OSError:
            pass


def ensure_nltk_data() -> None:
    """
    Ensures NLTK resources exist in repo-local ./nltk_data and are concurrency-safe.

    Behavior:
    - Always creates ./nltk_data if missing
    - Downloads missing corpora into ./nltk_data
    - Extracts any corpora zip files and repairs nested directory layout
    - Verifies WordNet can be loaded (not just present on disk)
    - Uses a cross-process file lock so multiple shards starting at once do not corrupt the folder
    """
    project_dir = _project_nltk_dir()
    _ensure_nltk_path(project_dir)

    with _acquire_lock(project_dir):
        # 1) Determine missing according to nltk.data.find
        missing = [name for (find_path, name) in REQUIRED if not _nltk_find_ok(find_path)]

        # 2) Download missing corpora
        if missing:
            for name in missing:
                nltk.download(name, download_dir=str(project_dir), quiet=True)

        # 3) Extract zips if they exist
        for _, name in REQUIRED:
            _extract_zip_to_corpora(project_dir, name)

        # 4) Repair double-nesting
        for _, name in REQUIRED:
            _repair_nested_dir(project_dir, name)

        # 5) Final find() verification
        still_missing = [name for (find_path, name) in REQUIRED if not _nltk_find_ok(find_path)]
        if still_missing:
            raise NLTKDataMissing(
                "NLTK resources still missing after auto-download/extract/repair: "
                + ", ".join(still_missing)
                + f". Expected under: {project_dir}"
            )

        # 6) Strong verification: actually load wordnet
        try:
            from nltk.corpus import wordnet as wn
            next(wn.all_synsets())
        except Exception:
            # Treat ANY exception here as corruption/partial contents.
            # Repair under the same lock so no other process can race us.
            _delete_corpus(project_dir, "wordnet")
            _delete_corpus(project_dir, "omw-1.4")

            nltk.download("wordnet", download_dir=str(project_dir), quiet=True)
            nltk.download("omw-1.4", download_dir=str(project_dir), quiet=True)

            _extract_zip_to_corpora(project_dir, "wordnet")
            _extract_zip_to_corpora(project_dir, "omw-1.4")

            _repair_nested_dir(project_dir, "wordnet")
            _repair_nested_dir(project_dir, "omw-1.4")

            # Re-verify hard
            try:
                from nltk.corpus import wordnet as wn
                next(wn.all_synsets())
            except Exception as e:
                raise NLTKDataMissing(
                    "WordNet appears present but failed to load after repair. "
                    f"Check contents under: {project_dir / 'corpora' / 'wordnet'}"
                ) from e