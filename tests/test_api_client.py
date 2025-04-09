"""
Tests for the API client module.
"""
import unittest
import requests
from unittest.mock import patch, MagicMock
from src.api.client import ApiClient

class TestApiClient(unittest.TestCase):
    """Test cases for the ApiClient class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.client = ApiClient(base_url="https://example.com/api", timeout=10)
    
    def test_init(self):
        """Test initialization of ApiClient."""
        self.assertEqual(self.client.base_url, "https://example.com/api")
        self.assertEqual(self.client.timeout, 10)
    
    @patch('requests.get')
    def test_get_success(self, mock_get):
        """Test successful GET request."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"key": "value"}
        mock_get.return_value = mock_response
        
        result = self.client.get("/endpoint")
        
        mock_get.assert_called_once_with(
            "https://example.com/api/endpoint", 
            params=None, 
            timeout=10
        )
        self.assertEqual(result, {"key": "value"})
    
    @patch('requests.get')
    def test_get_failure(self, mock_get):
        """Test failed GET request."""
        mock_get.side_effect = requests.exceptions.RequestException("API error")
        
        result = self.client.get("/endpoint")
        
        self.assertIsNone(result)

if __name__ == '__main__':
    unittest.main()
