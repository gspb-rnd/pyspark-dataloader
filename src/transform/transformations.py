"""
Module for data transformation operations.
"""
from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, lit, to_date, to_timestamp, concat, upper, lower, trim
from pyspark.sql.types import StructType

class DataTransformer:
    """
    Class for transforming data in PySpark DataFrames.
    """
    
    @staticmethod
    def rename_columns(df: DataFrame, rename_mapping: Dict[str, str]) -> DataFrame:
        """
        Rename columns in a DataFrame.
        
        Args:
            df (DataFrame): Input DataFrame
            rename_mapping (Dict[str, str]): Mapping of old column names to new column names
            
        Returns:
            DataFrame: DataFrame with renamed columns
        """
        for old_name, new_name in rename_mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df
    
    @staticmethod
    def add_constant_column(df: DataFrame, column_name: str, value: Any) -> DataFrame:
        """
        Add a constant value column to a DataFrame.
        
        Args:
            df (DataFrame): Input DataFrame
            column_name (str): Name of the new column
            value (Any): Constant value for the column
            
        Returns:
            DataFrame: DataFrame with the new column
        """
        return df.withColumn(column_name, lit(value))
    
    @staticmethod
    def flatten_nested_structure(df: DataFrame, nested_column: str) -> DataFrame:
        """
        Flatten a nested structure in a DataFrame.
        
        Args:
            df (DataFrame): Input DataFrame
            nested_column (str): Name of the nested column to flatten
            
        Returns:
            DataFrame: Flattened DataFrame
        """
        nested_schema = df.schema[nested_column].dataType
        
        if isinstance(nested_schema, StructType):
            for field in nested_schema.fields:
                df = df.withColumn(f"{nested_column}_{field.name}", col(f"{nested_column}.{field.name}"))
            
            df = df.drop(nested_column)
        
        return df
    
    @staticmethod
    def format_string_columns(df: DataFrame, upper_case_cols: List[str] = None, 
                             lower_case_cols: List[str] = None,
                             trim_cols: List[str] = None) -> DataFrame:
        """
        Format string columns in a DataFrame.
        
        Args:
            df (DataFrame): Input DataFrame
            upper_case_cols (List[str], optional): Columns to convert to uppercase
            lower_case_cols (List[str], optional): Columns to convert to lowercase
            trim_cols (List[str], optional): Columns to trim whitespace
            
        Returns:
            DataFrame: DataFrame with formatted columns
        """
        if upper_case_cols:
            for col_name in upper_case_cols:
                df = df.withColumn(col_name, upper(col(col_name)))
        
        if lower_case_cols:
            for col_name in lower_case_cols:
                df = df.withColumn(col_name, lower(col(col_name)))
        
        if trim_cols:
            for col_name in trim_cols:
                df = df.withColumn(col_name, trim(col(col_name)))
        
        return df
    
    @staticmethod
    def filter_and_select(df: DataFrame, filter_condition: Optional[str] = None, 
                         select_columns: Optional[List[str]] = None) -> DataFrame:
        """
        Filter and select columns from a DataFrame.
        
        Args:
            df (DataFrame): Input DataFrame
            filter_condition (str, optional): SQL-like filter condition
            select_columns (List[str], optional): Columns to select
            
        Returns:
            DataFrame: Filtered and selected DataFrame
        """
        result_df = df
        
        if filter_condition:
            result_df = result_df.filter(filter_condition)
        
        if select_columns:
            result_df = result_df.select(*select_columns)
        
        return result_df
