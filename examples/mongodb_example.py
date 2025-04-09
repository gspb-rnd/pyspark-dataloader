"""
Example script demonstrating MongoDB persistence with the PySpark API application.
"""
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.api.client import ApiClient
from src.spark.session import create_spark_session
from src.spark.dataframe import json_to_dataframe, display_dataframe_info
from src.utils.logger import setup_logger
from src.transform.transformations import DataTransformer
from src.persistence.mongodb import MongoDBClient
from src.config.settings import API_CONFIG, SPARK_CONFIG, LOG_CONFIG, MONGODB_CONFIG, TRANSFORM_CONFIG
from src.config.schemas import USERS_SCHEMA

def main():
    """Example of using MongoDB with the PySpark API application."""
    logger = setup_logger("mongodb_example", logging.INFO)
    
    spark = create_spark_session(
        app_name="MongoDB Example",
        master="local[*]"
    )
    
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created successfully")
    
    logger.info("Initializing MongoDB client...")
    
    mongodb_client = MongoDBClient(
        connection_string="mongodb://localhost:27017/",
        database="api_data_example"
    )
    
    if mongodb_client.connect():
        logger.info("Successfully connected to MongoDB")
    else:
        logger.error("Failed to connect to MongoDB. Make sure MongoDB is running.")
        logger.info("You can start MongoDB with: sudo systemctl start mongod")
        logger.info("Or install it with: sudo apt-get install -y mongodb")
        return
    
    try:
        api_config = API_CONFIG["users_api"]
        client = ApiClient(timeout=api_config["timeout"])
        
        logger.info(f"Fetching user data from {api_config['url']}...")
        json_data = client.get(api_config["url"])
        
        if not json_data:
            logger.error("Failed to fetch user data from API")
            return
        
        logger.info(f"Successfully fetched data for {len(json_data)} users")
        
        df = json_to_dataframe(spark, json_data, USERS_SCHEMA)
        
        logger.info("Applying transformations to users data...")
        transformer = DataTransformer()
        
        transform_config = TRANSFORM_CONFIG["users"]
        
        if "rename_columns" in transform_config:
            df = transformer.rename_columns(df, transform_config["rename_columns"])
        
        if "uppercase_columns" in transform_config:
            df = transformer.format_string_columns(
                df, 
                upper_case_cols=transform_config["uppercase_columns"]
            )
        
        if "flatten_nested" in transform_config:
            for nested_col in transform_config["flatten_nested"]:
                df = transformer.flatten_nested_structure(df, nested_col)
        
        display_dataframe_info(df, "Transformed Users DataFrame")
        
        collection_name = "users_example"
        batch_size = MONGODB_CONFIG["batch_size"]
        
        logger.info(f"Persisting users data to MongoDB collection: {collection_name}")
        inserted_count = mongodb_client.insert_dataframe(
            df, 
            collection_name,
            batch_size
        )
        
        if inserted_count > 0:
            logger.info(f"Successfully persisted {inserted_count} user records to MongoDB")
            
            logger.info("Retrieving data from MongoDB to verify persistence...")
            documents = mongodb_client.find_documents(collection_name, limit=3)
            logger.info(f"Retrieved {len(documents)} documents from MongoDB")
            
            for doc in documents:
                doc.pop('_id', None)
                logger.info(f"Sample document: {doc}")
        else:
            logger.warning("Failed to persist user data to MongoDB")
    
    except Exception as e:
        logger.error(f"Error in MongoDB example: {e}")
    finally:
        mongodb_client.close()
        logger.info("MongoDB connection closed")
        
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()
