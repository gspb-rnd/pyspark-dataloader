from setuptools import setup, find_packages

setup(
    name="pyspark_api_app",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.0.0",
        "requests>=2.25.0",
    ],
    python_requires=">=3.6",
    author="Devin AI",
    author_email="example@example.com",
    description="A PySpark application that fetches data from APIs and loads it into dataframes",
    keywords="pyspark, api, dataframe, json",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
