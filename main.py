"""
Main entry point for the PySpark API application.
"""
import logging
from typing import Dict, Any, Optional

from src.api.client import ApiClient
from src.spark.session import create_spark_session
from src.spark.dataframe import json_to_dataframe, display_dataframe_info
from src.utils.logger import setup_logger
from src.transform.transformations import DataTransformer
from src.persistence.mongodb import MongoDBClient
from src.config.settings import API_CONFIG, SPARK_CONFIG, LOG_CONFIG, MONGODB_CONFIG, TRANSFORM_CONFIG
from src.config.schemas import USERS_SCHEMA, POSTS_SCHEMA, TODOS_SCHEMA

def process_users_data(spark_session: Any, logger: logging.Logger, 
                      mongodb_client: Optional[MongoDBClient] = None) -> None:
    """
    Process users data from the API.
    
    Args:
        spark_session: Active Spark session
        logger: Logger instance
        mongodb_client: MongoDB client for persistence
    """
    api_config = API_CONFIG["users_api"]
    client = ApiClient(timeout=api_config["timeout"])
    
    logger.info(f"Fetching user data from {api_config['url']}...")
    json_data = client.get(api_config["url"])
    
    if not json_data:
        logger.error("Failed to fetch user data from API")
        return
    
    logger.info(f"Successfully fetched data for {len(json_data)} users")
    
    df = json_to_dataframe(spark_session, json_data, USERS_SCHEMA)
    
    display_dataframe_info(df, "Original Users DataFrame", truncate=False)
    
    logger.info("Applying transformations to users data...")
    transformer = DataTransformer()
    
    transform_config = TRANSFORM_CONFIG["users"]
    
    if "rename_columns" in transform_config:
        df = transformer.rename_columns(df, transform_config["rename_columns"])
        logger.info(f"Renamed columns: {transform_config['rename_columns']}")
    
    if "uppercase_columns" in transform_config:
        df = transformer.format_string_columns(
            df, 
            upper_case_cols=transform_config["uppercase_columns"]
        )
        logger.info(f"Converted columns to uppercase: {transform_config['uppercase_columns']}")
    
    if "flatten_nested" in transform_config:
        for nested_col in transform_config["flatten_nested"]:
            df = transformer.flatten_nested_structure(df, nested_col)
            logger.info(f"Flattened nested structure: {nested_col}")
    
    display_dataframe_info(df, "Transformed Users DataFrame")
    
    if mongodb_client:
        collection_name = MONGODB_CONFIG["collections"]["users"]
        batch_size = MONGODB_CONFIG["batch_size"]
        
        logger.info(f"Persisting users data to MongoDB collection: {collection_name}")
        inserted_count = mongodb_client.insert_dataframe(
            df, 
            collection_name,
            batch_size
        )
        
        if inserted_count > 0:
            logger.info(f"Successfully persisted {inserted_count} user records to MongoDB")
        else:
            logger.warning("Failed to persist user data to MongoDB")
    
    logger.info("Extracting user contact information...")
    contact_fields = ["id", "full_name" if "name" in transform_config["rename_columns"] else "name", 
                     "email_address" if "email" in transform_config["rename_columns"] else "email", 
                     "phone"]
    contact_df = df.select(*[col for col in contact_fields if col in df.columns])
    display_dataframe_info(contact_df, "Contact Information DataFrame")
    
    logger.info("Filtering users with .org websites...")
    org_users_df = df.filter(df.website.endswith(".org"))
    display_dataframe_info(org_users_df, "Users with .org Websites DataFrame")
    
    city_col = "address_city" if "address" in transform_config.get("flatten_nested", []) else "address.city"
    if city_col in df.columns or "." in city_col:
        logger.info("Counting users by city...")
        city_counts_df = df.groupBy(city_col).count()
        display_dataframe_info(city_counts_df, "User Count by City DataFrame")

def main() -> None:
    """Main function to run the application."""
    logger = setup_logger("pyspark_api_app", 
                         logging.getLevelName(LOG_CONFIG["level"]),
                         LOG_CONFIG["format"])
    
    spark = create_spark_session(
        app_name=SPARK_CONFIG["app_name"],
        master=SPARK_CONFIG["master"]
    )
    
    spark.sparkContext.setLogLevel(SPARK_CONFIG["log_level"])
    logger.info("Spark session created successfully")
    
    mongodb_client = None
    try:
        logger.info("Initializing MongoDB client...")
        mongodb_client = MongoDBClient(
            connection_string=MONGODB_CONFIG["connection_string"],
            database=MONGODB_CONFIG["database"]
        )
        
        if mongodb_client.connect():
            logger.info("Successfully connected to MongoDB")
        else:
            logger.warning("Failed to connect to MongoDB, will proceed without persistence")
            mongodb_client = None
    except Exception as e:
        logger.error(f"Error initializing MongoDB client: {e}")
        logger.warning("Will proceed without MongoDB persistence")
    
    try:
        process_users_data(spark, logger, mongodb_client)
        
        
    except Exception as e:
        logger.error(f"Error processing data: {e}")
    finally:
        if mongodb_client:
            mongodb_client.close()
            
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
