from setuptools import setup, find_packages

setup(
    name="db_conductor",
    version="0.1.0",
    description="A Python adapter tool for database interaction in ETL pipelines",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Pen Hsuan, Wang",
    url="https://github.com/PenHsuanWang/db-conductor",
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "pyspark>=3.0.0",
        "pandas>=1.3.0",
        "mysql-connector-python",
        "psycopg2-binary",
        "pymongo",
        "snowflake-connector-python",
    ],
    extras_require={
        "dev": [
            "pytest",
            "tox",
            "flake8",
            "mypy",
        ],
    },
    python_requires=">=3.10",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points={
        "console_scripts": [
            "db-conductor=db_conductor.cli:main",  # CLI entry point if needed
        ],
    },
)
