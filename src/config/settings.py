"""
Configuration settings for the application.
"""
from typing import Dict, Any

API_CONFIG: Dict[str, Any] = {
    "users_api": {
        "url": "https://jsonplaceholder.typicode.com/users",
        "timeout": 30
    },
    "posts_api": {
        "url": "https://jsonplaceholder.typicode.com/posts",
        "timeout": 30
    },
    "todos_api": {
        "url": "https://jsonplaceholder.typicode.com/todos",
        "timeout": 30
    }
}

SPARK_CONFIG: Dict[str, Any] = {
    "app_name": "API Data to Spark DataFrame",
    "master": "local[*]",
    "log_level": "WARN"
}

LOG_CONFIG: Dict[str, Any] = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
}

MONGODB_CONFIG: Dict[str, Any] = {
    "connection_string": "mongodb://localhost:27017/",
    "database": "api_data",
    "collections": {
        "users": "users",
        "posts": "posts",
        "todos": "todos"
    },
    "batch_size": 1000
}

TRANSFORM_CONFIG: Dict[str, Any] = {
    "users": {
        "rename_columns": {
            "name": "full_name",
            "email": "email_address"
        },
        "uppercase_columns": ["username"],
        "flatten_nested": ["address", "company"]
    },
    "posts": {
        "rename_columns": {
            "title": "post_title",
            "body": "post_content"
        }
    },
    "todos": {
        "rename_columns": {
            "title": "task_title"
        }
    }
}
