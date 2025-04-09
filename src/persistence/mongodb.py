"""
Module for MongoDB persistence operations.
"""
import json
import logging
from typing import Dict, List, Any, Optional, Union
import pymongo
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pyspark.sql import DataFrame

class MongoDBClient:
    """
    Client for MongoDB operations.
    """
    
    def __init__(self, connection_string: str, database: str):
        """
        Initialize MongoDB client.
        
        Args:
            connection_string (str): MongoDB connection string
            database (str): Database name
        """
        self.connection_string = connection_string
        self.database_name = database
        self.client = None
        self.db = None
        self.logger = logging.getLogger(__name__)
    
    def connect(self) -> bool:
        """
        Connect to MongoDB.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
            self.client.admin.command('ping')
            self.logger.info(f"Connected to MongoDB database: {self.database_name}")
            return True
        except PyMongoError as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def close(self) -> None:
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            self.logger.info("MongoDB connection closed")
    
    def insert_dataframe(self, df: DataFrame, collection: str, 
                        batch_size: int = 1000) -> int:
        """
        Insert DataFrame records into MongoDB collection.
        
        Args:
            df (DataFrame): DataFrame to insert
            collection (str): Collection name
            batch_size (int): Batch size for bulk inserts
            
        Returns:
            int: Number of documents inserted
        """
        if not self.db:
            if not self.connect():
                return 0
        
        try:
            records = df.toJSON().map(lambda x: json.loads(x)).collect()
            
            coll = self.db[collection]
            
            total_inserted = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                result = coll.insert_many(batch)
                total_inserted += len(result.inserted_ids)
            
            self.logger.info(f"Inserted {total_inserted} documents into collection: {collection}")
            return total_inserted
        
        except PyMongoError as e:
            self.logger.error(f"Error inserting data into MongoDB: {e}")
            return 0
    
    def find_documents(self, collection: str, query: Dict[str, Any] = None, 
                      projection: Dict[str, int] = None, 
                      limit: int = 0) -> List[Dict[str, Any]]:
        """
        Find documents in a collection.
        
        Args:
            collection (str): Collection name
            query (Dict[str, Any], optional): Query filter
            projection (Dict[str, int], optional): Fields to include/exclude
            limit (int, optional): Maximum number of documents to return
            
        Returns:
            List[Dict[str, Any]]: List of documents
        """
        if not self.db:
            if not self.connect():
                return []
        
        try:
            coll = self.db[collection]
            cursor = coll.find(query or {}, projection or {})
            
            if limit > 0:
                cursor = cursor.limit(limit)
            
            return list(cursor)
        
        except PyMongoError as e:
            self.logger.error(f"Error finding documents in MongoDB: {e}")
            return []
    
    def update_documents(self, collection: str, query: Dict[str, Any], 
                        update: Dict[str, Any], 
                        upsert: bool = False) -> int:
        """
        Update documents in a collection.
        
        Args:
            collection (str): Collection name
            query (Dict[str, Any]): Query filter
            update (Dict[str, Any]): Update operations
            upsert (bool): Whether to insert if document doesn't exist
            
        Returns:
            int: Number of documents modified
        """
        if not self.db:
            if not self.connect():
                return 0
        
        try:
            coll = self.db[collection]
            result = coll.update_many(query, update, upsert=upsert)
            self.logger.info(f"Updated {result.modified_count} documents in collection: {collection}")
            return result.modified_count
        
        except PyMongoError as e:
            self.logger.error(f"Error updating documents in MongoDB: {e}")
            return 0
    
    def delete_documents(self, collection: str, query: Dict[str, Any]) -> int:
        """
        Delete documents from a collection.
        
        Args:
            collection (str): Collection name
            query (Dict[str, Any]): Query filter
            
        Returns:
            int: Number of documents deleted
        """
        if not self.db:
            if not self.connect():
                return 0
        
        try:
            coll = self.db[collection]
            result = coll.delete_many(query)
            self.logger.info(f"Deleted {result.deleted_count} documents from collection: {collection}")
            return result.deleted_count
        
        except PyMongoError as e:
            self.logger.error(f"Error deleting documents from MongoDB: {e}")
            return 0
