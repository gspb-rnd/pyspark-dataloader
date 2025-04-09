"""
Module for DataFrame operations and transformations.
"""
import json
from typing import Dict, List, Union, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

def json_to_dataframe(spark: SparkSession, 
                      json_data: Union[Dict, List], 
                      schema: Optional[StructType] = None) -> DataFrame:
    """
    Convert JSON data to a Spark DataFrame.
    
    Args:
        spark (SparkSession): The Spark session
        json_data (Union[Dict, List]): The JSON data to convert
        schema (StructType, optional): The schema for the DataFrame
        
    Returns:
        DataFrame: The Spark DataFrame created from the JSON data
    """
    json_str = json.dumps(json_data)
    
    rdd = spark.sparkContext.parallelize([json_str])
    
    if schema:
        df = spark.read.schema(schema).json(rdd)
    else:
        df = spark.read.json(rdd)
    
    return df

def display_dataframe_info(df: DataFrame, name: str = "DataFrame", 
                           show_schema: bool = True, 
                           show_data: bool = True,
                           num_rows: int = 10,
                           truncate: bool = False) -> None:
    """
    Display information about a DataFrame.
    
    Args:
        df (DataFrame): The DataFrame to display
        name (str): Name to display for the DataFrame
        show_schema (bool): Whether to show the schema
        show_data (bool): Whether to show the data
        num_rows (int): Number of rows to show
        truncate (bool): Whether to truncate long values
    """
    print(f"\n{name} Information:")
    
    if show_schema:
        print(f"{name} Schema:")
        df.printSchema()
    
    if show_data:
        print(f"{name} Content:")
        df.show(n=num_rows, truncate=truncate)
