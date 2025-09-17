# currency-shared/setup.py
from setuptools import setup

setup(
    name="currency-shared",
    version="0.1.0",
    description="Shared utilities for currency services",
    py_modules=[
        "config",
        "database", 
        "logging",
        "models",
        "utils",
        "exceptions"
    ],
    install_requires=[
        "pydantic==2.5.0",
        "aiomysql==0.2.0",
        "redis[hiredis]==5.0.1",
        "structlog==23.2.0",
        "python-dateutil==2.8.2",
        "aiohttp==3.9.1",
        "boto3==1.34.0",
        "motor==3.3.2",
        "pymongo==4.6.0",
    ],
    python_requires=">=3.11",
)