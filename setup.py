from setuptools import setup, find_packages

setup(
    name="data-ingestion",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "requests",
        "pandas",
        "openpyxl",
        "sqlite3-offline",
    ],
)