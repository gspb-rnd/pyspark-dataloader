"""
Schema definitions for different API data structures.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    LongType, BooleanType, ArrayType
)

USERS_SCHEMA = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("username", StringType(), True),
    StructField("email", StringType(), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("suite", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zipcode", StringType(), True),
        StructField("geo", StructType([
            StructField("lat", StringType(), True),
            StructField("lng", StringType(), True)
        ]), True)
    ]), True),
    StructField("phone", StringType(), True),
    StructField("website", StringType(), True),
    StructField("company", StructType([
        StructField("name", StringType(), True),
        StructField("catchPhrase", StringType(), True),
        StructField("bs", StringType(), True)
    ]), True)
])

POSTS_SCHEMA = StructType([
    StructField("userId", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("body", StringType(), True)
])

TODOS_SCHEMA = StructType([
    StructField("userId", IntegerType(), True),
    StructField("id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("completed", BooleanType(), True)
])
