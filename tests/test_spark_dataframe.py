"""
Tests for the Spark DataFrame module.
"""
import unittest
from unittest.mock import patch, MagicMock
import json
from pyspark.sql import SparkSession
from src.spark.dataframe import json_to_dataframe

class TestSparkDataframe(unittest.TestCase):
    """Test cases for the Spark DataFrame functions."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures that are used by all test methods."""
        cls.spark = SparkSession.builder \
            .appName("TestSparkDataframe") \
            .master("local[1]") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test fixtures after all tests have run."""
        cls.spark.stop()
    
    def test_json_to_dataframe(self):
        """Test conversion of JSON data to DataFrame."""
        test_data = [
            {"id": 1, "name": "Test User", "email": "test@example.com"}
        ]
        
        df = json_to_dataframe(self.spark, test_data)
        
        self.assertEqual(df.count(), 1)
        self.assertEqual(len(df.columns), 3)
        
        result = [row.asDict() for row in df.collect()]
        
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "Test User")
        self.assertEqual(result[0]["email"], "test@example.com")

if __name__ == '__main__':
    unittest.main()
