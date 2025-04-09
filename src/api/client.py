"""
API client module for fetching data from external APIs.
"""
import requests
from typing import Dict, List, Union, Optional, Any

class ApiClient:
    """
    Client for making API requests and handling responses.
    """
    
    def __init__(self, base_url: str = "", timeout: int = 30):
        """
        Initialize the API client.
        
        Args:
            base_url (str): Base URL for API requests
            timeout (int): Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        
    def get(self, endpoint: str = "", params: Optional[Dict[str, Any]] = None) -> Union[Dict, List, None]:
        """
        Make a GET request to the API.
        
        Args:
            endpoint (str): API endpoint to call
            params (Dict[str, Any], optional): Query parameters
            
        Returns:
            Union[Dict, List, None]: JSON response or None if request failed
        """
        url = f"{self.base_url}{endpoint}" if self.base_url else endpoint
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from API: {e}")
            return None
