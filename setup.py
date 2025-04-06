"""
Setup script for the ETL Pipeline package.
"""

from setuptools import setup, find_packages

setup(
    name="etl_pipeline",
    version="1.0.0",
    description="ETL, Reporting & PDF Export Pipeline",
    author="Data Engineering Team",
    author_email="data-engineering@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        "matplotlib>=3.4.0",
        "seaborn>=0.11.0",
        "requests>=2.25.0",
        "python-dotenv>=0.19.0",
        "psycopg2-binary>=2.9.0",
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "etl-pipeline=etl_pipeline.main:main",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
