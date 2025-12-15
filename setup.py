from setuptools import setup, find_packages

setup(
    name="elasticsearch_clone",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pytest",
        "nltk",
        "rapidfuzz",
    ],
    python_requires='>=3.11',
)