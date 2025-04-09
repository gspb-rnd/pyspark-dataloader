"""
Module for creating and managing Spark sessions.
"""
from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "PySpark Application", 
                         master: str = "local[*]") -> SparkSession:
    """
    Create and return a Spark session.
    
    Args:
        app_name (str): Name of the Spark application
        master (str): Spark master URL
        
    Returns:
        SparkSession: Configured Spark session
    """
    return SparkSession.builder \
        .appName(app_name) \
        .master(master) \
        .getOrCreate()
