# PySpark API Data Processor

A structured PySpark application that fetches data from APIs, processes JSON responses, applies transformations, and loads the data into PySpark DataFrames with mock database persistence.

## Project Structure

```
pyspark_api_project/
├── main.py                 # Main application entry point
├── setup.py                # Package setup file
├── requirements.txt        # Project dependencies
├── README.md               # Project documentation
├── src/                    # Source code
│   ├── api/                # API-related modules
│   │   ├── client.py       # API client for making requests
│   ├── config/             # Configuration modules
│   │   ├── schemas.py      # DataFrame schema definitions
│   │   ├── settings.py     # Application settings
│   ├── persistence/        # Data persistence modules
│   │   ├── mongodb.py      # MongoDB client for data storage
│   │   ├── mockdb.py       # Mock database client for data storage
│   ├── spark/              # Spark-related modules
│   │   ├── dataframe.py    # DataFrame operations
│   │   ├── session.py      # Spark session management
│   ├── transform/          # Data transformation modules
│   │   ├── transformations.py # Data transformation operations
│   └── utils/              # Utility modules
│       ├── logger.py       # Logging utilities
└── tests/                  # Test modules
    ├── test_api_client.py  # Tests for API client
    └── test_spark_dataframe.py  # Tests for DataFrame operations
```

## Requirements

- Python 3.6+
- PySpark 3.0+
- Requests library
- PyMongo (for MongoDB persistence)

## Installation

1. Create a virtual environment (recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

2. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

3. Install the package in development mode:
   ```
   pip install -e .
   ```

## Usage

Run the application:
```
python main.py
```

## Features

### 1. API Data Fetching
The application fetches data from external APIs using the `ApiClient` module, which handles HTTP requests and response parsing.

### 2. Data Transformation
The `DataTransformer` class provides various data transformation capabilities:
- Renaming columns
- Converting text to uppercase/lowercase
- Flattening nested structures
- Adding constant columns
- Filtering and selecting data

Configure transformations in `src/config/settings.py` under the `TRANSFORM_CONFIG` section.

### 3. Data Persistence
The application can store processed data using either:

#### Mock Database
The application uses a mock database by default, which simulates database operations without requiring an actual database installation. Data is stored in JSON files in a local directory.

To use the mock database:
1. No additional setup is required
2. Data will be stored in the `mock_db` directory by default
3. Run the example script to see it in action:
   ```
   python examples/mockdb_example.py
   ```

#### MongoDB (Alternative)
The application also includes MongoDB support if needed.

To enable MongoDB persistence:
1. Install MongoDB on your system or use a cloud MongoDB service
2. Update the MongoDB connection settings in `src/config/settings.py`:
   ```python
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
   ```
3. Update the import in `main.py` to use `MongoDBClient` instead of `MockDBClient`

## Customization

To use a different API or add new data processing functionality:

1. Update the API configuration in `src/config/settings.py`
2. Add new schema definitions in `src/config/schemas.py`
3. Configure transformations in the `TRANSFORM_CONFIG` section of `settings.py`
4. Create new processing functions in `main.py` or add new modules as needed

## Testing

Run the tests:
```
python -m unittest discover tests
```

## Example APIs

The default example uses JSONPlaceholder's APIs:
- Users API: https://jsonplaceholder.typicode.com/users
- Posts API: https://jsonplaceholder.typicode.com/posts
- Todos API: https://jsonplaceholder.typicode.com/todos
