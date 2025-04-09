"""
Mock database module for simulating database operations without requiring MongoDB.
"""
import json
import os
import logging
from typing import Dict, List, Any, Optional, Union
import pandas as pd
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

class MockDBClient:
    """
    Mock database client that simulates MongoDB operations.
    
    This class provides an in-memory database with file persistence
    capabilities to simulate MongoDB operations without requiring
    an actual MongoDB installation.
    """
    
    def __init__(self, storage_dir: str = "mock_db", database: str = "mock_database"):
        """
        Initialize the mock database client.
        
        Args:
            storage_dir (str): Directory to store the database files
            database (str): Name of the database
        """
        self.storage_dir = storage_dir
        self.database = database
        self.collections = {}
        self.connected = False
        self.db_path = os.path.join(storage_dir, database)
        
    def connect(self) -> bool:
        """
        Simulate connecting to a database.
        
        Returns:
            bool: True if connection is successful
        """
        try:
            if not os.path.exists(self.storage_dir):
                os.makedirs(self.storage_dir)
            
            if not os.path.exists(self.db_path):
                os.makedirs(self.db_path)
                
            self.connected = True
            logger.info(f"Connected to mock database: {self.database}")
            
            self._load_collections()
            
            return True
        except Exception as e:
            logger.error(f"Error connecting to mock database: {e}")
            return False
    
    def _load_collections(self) -> None:
        """Load existing collections from storage."""
        try:
            for collection_file in os.listdir(self.db_path):
                if collection_file.endswith('.json'):
                    collection_name = collection_file[:-5]  # Remove .json extension
                    file_path = os.path.join(self.db_path, collection_file)
                    
                    with open(file_path, 'r') as f:
                        self.collections[collection_name] = json.load(f)
                        
                    logger.info(f"Loaded collection: {collection_name} with {len(self.collections[collection_name])} documents")
        except Exception as e:
            logger.error(f"Error loading collections: {e}")
    
    def _save_collection(self, collection_name: str) -> bool:
        """
        Save a collection to disk.
        
        Args:
            collection_name (str): Name of the collection to save
            
        Returns:
            bool: True if save is successful
        """
        try:
            if collection_name not in self.collections:
                return False
                
            file_path = os.path.join(self.db_path, f"{collection_name}.json")
            
            with open(file_path, 'w') as f:
                json.dump(self.collections[collection_name], f, indent=2)
                
            return True
        except Exception as e:
            logger.error(f"Error saving collection {collection_name}: {e}")
            return False
    
    def insert_document(self, collection_name: str, document: Dict[str, Any]) -> bool:
        """
        Insert a document into a collection.
        
        Args:
            collection_name (str): Name of the collection
            document (Dict[str, Any]): Document to insert
            
        Returns:
            bool: True if insertion is successful
        """
        if not self.connected:
            logger.error("Not connected to mock database")
            return False
            
        try:
            if collection_name not in self.collections:
                self.collections[collection_name] = []
            
            if '_id' not in document:
                document['_id'] = len(self.collections[collection_name]) + 1
                
            self.collections[collection_name].append(document)
            
            self._save_collection(collection_name)
            
            return True
        except Exception as e:
            logger.error(f"Error inserting document: {e}")
            return False
    
    def insert_many(self, collection_name: str, documents: List[Dict[str, Any]]) -> int:
        """
        Insert multiple documents into a collection.
        
        Args:
            collection_name (str): Name of the collection
            documents (List[Dict[str, Any]]): Documents to insert
            
        Returns:
            int: Number of documents inserted
        """
        if not self.connected:
            logger.error("Not connected to mock database")
            return 0
            
        try:
            if collection_name not in self.collections:
                self.collections[collection_name] = []
            
            inserted_count = 0
            
            for document in documents:
                if '_id' not in document:
                    document['_id'] = len(self.collections[collection_name]) + 1
                    
                self.collections[collection_name].append(document)
                inserted_count += 1
            
            self._save_collection(collection_name)
            
            return inserted_count
        except Exception as e:
            logger.error(f"Error inserting documents: {e}")
            return 0
    
    def insert_dataframe(self, df: DataFrame, collection_name: str, batch_size: int = 1000) -> int:
        """
        Insert a DataFrame into a collection.
        
        Args:
            df (DataFrame): DataFrame to insert
            collection_name (str): Name of the collection
            batch_size (int): Number of records to insert in each batch
            
        Returns:
            int: Number of records inserted
        """
        if not self.connected:
            logger.error("Not connected to mock database")
            return 0
            
        try:
            pdf = df.toPandas()
            records = pdf.to_dict('records')
            
            total_inserted = 0
            for i in range(0, len(records), batch_size):
                batch = records[i:i+batch_size]
                inserted = self.insert_many(collection_name, batch)
                total_inserted += inserted
                
            return total_inserted
        except Exception as e:
            logger.error(f"Error inserting DataFrame: {e}")
            return 0
    
    def find_documents(self, collection_name: str, query: Optional[Dict[str, Any]] = None, 
                      projection: Optional[Dict[str, int]] = None, limit: int = 0) -> List[Dict[str, Any]]:
        """
        Find documents in a collection.
        
        Args:
            collection_name (str): Name of the collection
            query (Dict[str, Any], optional): Query filter
            projection (Dict[str, int], optional): Fields to include/exclude
            limit (int): Maximum number of documents to return
            
        Returns:
            List[Dict[str, Any]]: List of matching documents
        """
        if not self.connected:
            logger.error("Not connected to mock database")
            return []
            
        try:
            if collection_name not in self.collections:
                return []
                
            documents = self.collections[collection_name]
            
            if query:
                filtered_docs = []
                for doc in documents:
                    match = True
                    for key, value in query.items():
                        if key not in doc or doc[key] != value:
                            match = False
                            break
                    if match:
                        filtered_docs.append(doc)
                documents = filtered_docs
            
            if projection:
                projected_docs = []
                for doc in documents:
                    projected_doc = {}
                    for key, include in projection.items():
                        if include == 1 and key in doc:
                            projected_doc[key] = doc[key]
                    projected_docs.append(projected_doc)
                documents = projected_docs
            
            if limit > 0:
                documents = documents[:limit]
                
            return documents
        except Exception as e:
            logger.error(f"Error finding documents: {e}")
            return []
    
    def update_document(self, collection_name: str, query: Dict[str, Any], 
                       update: Dict[str, Any]) -> int:
        """
        Update documents in a collection.
        
        Args:
            collection_name (str): Name of the collection
            query (Dict[str, Any]): Query filter
            update (Dict[str, Any]): Update to apply
            
        Returns:
            int: Number of documents updated
        """
        if not self.connected:
            logger.error("Not connected to mock database")
            return 0
            
        try:
            if collection_name not in self.collections:
                return 0
                
            updated_count = 0
            
            for doc in self.collections[collection_name]:
                match = True
                for key, value in query.items():
                    if key not in doc or doc[key] != value:
                        match = False
                        break
                
                if match:
                    for key, value in update.items():
                        doc[key] = value
                    updated_count += 1
            
            if updated_count > 0:
                self._save_collection(collection_name)
                
            return updated_count
        except Exception as e:
            logger.error(f"Error updating documents: {e}")
            return 0
    
    def delete_documents(self, collection_name: str, query: Dict[str, Any]) -> int:
        """
        Delete documents from a collection.
        
        Args:
            collection_name (str): Name of the collection
            query (Dict[str, Any]): Query filter
            
        Returns:
            int: Number of documents deleted
        """
        if not self.connected:
            logger.error("Not connected to mock database")
            return 0
            
        try:
            if collection_name not in self.collections:
                return 0
                
            original_count = len(self.collections[collection_name])
            
            self.collections[collection_name] = [
                doc for doc in self.collections[collection_name]
                if not all(key in doc and doc[key] == value for key, value in query.items())
            ]
            
            deleted_count = original_count - len(self.collections[collection_name])
            
            if deleted_count > 0:
                self._save_collection(collection_name)
                
            return deleted_count
        except Exception as e:
            logger.error(f"Error deleting documents: {e}")
            return 0
    
    def drop_collection(self, collection_name: str) -> bool:
        """
        Drop a collection.
        
        Args:
            collection_name (str): Name of the collection
            
        Returns:
            bool: True if collection was dropped
        """
        if not self.connected:
            logger.error("Not connected to mock database")
            return False
            
        try:
            if collection_name not in self.collections:
                return False
                
            del self.collections[collection_name]
            
            file_path = os.path.join(self.db_path, f"{collection_name}.json")
            if os.path.exists(file_path):
                os.remove(file_path)
                
            return True
        except Exception as e:
            logger.error(f"Error dropping collection: {e}")
            return False
    
    def close(self) -> None:
        """Close the database connection."""
        self.connected = False
        logger.info("Closed connection to mock database")
