# db-conductor
## **Overview**

**`db_conductor`** is a Python library designed to simplify interactions between big data pipelines (e.g., PySpark ETL workflows) and various databases. It provides an abstraction layer for managing both temporary and final data in different database systems, such as MySQL, PostgreSQL, MongoDB, Snowflake, and more. 

This library allows developers to decouple database-specific logic from their ETL business logic, offering a flexible, scalable, and maintainable approach to handle both static and dynamic data sources.

### **Use Case Example**

`db_conductor` is designed to handle various scenarios in big data ETL workflows, such as:

- Storing and caching temporary data during multi-step transformations.
- Loading static data from one source and waiting for dynamic data from another source to join the datasets.
- Managing real-time streaming and batch processing pipelines by integrating with databases in a uniform way.
  
For example, consider an ETL pipeline where:
1. **Source 1** (static data) is stored in **MongoDB**.
2. **Source 2** (dynamic data) arrives later and must be joined with **Source 1** data.
3. After joining the datasets, the final output is stored in a target database, such as **MySQL**.

---

## **Key Features**

- **Database Adapters**: Seamless integration with multiple database systems (e.g., MySQL, PostgreSQL, MongoDB, Snowflake) through adapter classes that handle connections, reads, writes, updates, and deletions.
- **Batch Operations**: Efficient batch processing for high-volume data loads and updates.
- **Modular Design**: Each adapter is designed to be modular, making it easy to extend the library to support additional databases in the future.
- **Configuration Management**: Centralized configuration for managing database credentials, connection settings, and environmental variables.
- **Integration with PySpark**: Includes utilities to interact with PySpark, making it easy to integrate with Spark DataFrames for large-scale data transformations.
- **Logging and Monitoring**: Built-in logging for tracking database operations, query executions, and error handling.

---

## **Folder Structure**

```plaintext
db_conductor/
│
├── db_conductor/
│   ├── adapters/                     # Database adapter implementations
│   │   ├── base_adapter.py           # Abstract base class for all database adapters
│   │   ├── mysql_adapter.py          # MySQL adapter
│   │   ├── postgres_adapter.py       # PostgreSQL adapter
│   │   ├── mongo_adapter.py          # MongoDB adapter
│   │   └── snowflake_adapter.py      # Snowflake adapter
│   ├── utils/
│   │   ├── data_checkout.py          # Utility for managing data stashing and retrieval
│   │   ├── spark_helper.py           # Utilities to integrate with PySpark
│   └── config/
│       ├── settings.py               # Configuration management
│       └── logging_config.py         # Logging setup
│
├── tests/                            # Unit tests for all components
│
├── README.md                         # Project documentation
├── requirements.txt                  # Project dependencies
├── setup.py                          # Build script
└── tox.ini                           # Tox configuration for testing
```

---

## **Setup and Installation**

### **Prerequisites**
- Python 3.7+
- PySpark 3.x
- MongoDB, MySQL, PostgreSQL, or any supported database

### **Installation**

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/db_conductor.git
   cd db_conductor
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Database Credentials**
   Update the `settings.py` file inside the `db_conductor/config` folder with your database credentials.

4. **Run Tests**
   To ensure everything is set up correctly, you can run the unit tests:
   ```bash
   tox
   ```

---

## **Usage Example: ETL Pipeline with Different Update Frequencies**

Here’s an example scenario where you have **two data sources** with different update frequencies:

- **Source 1**: Provides static or rarely changing data (e.g., customer information) and is stored in **MongoDB**.
- **Source 2**: Provides dynamic or frequently updated data (e.g., transactions) and must be joined with Source 1 data.

### **Pipeline Flow**

1. **Source 1 (Static Data)**: 
   - Stash static data in **MongoDB** using `db_conductor` until dynamic data (Source 2) arrives.
2. **Source 2 (Dynamic Data)**:
   - After Source 2 data arrives, retrieve the static data from **MongoDB**, join both datasets, and perform transformations.
3. **Final Data Storage**:
   - Store the final joined data in a target database like **MySQL**.

### **Code Example**

```python
from pyspark.sql import SparkSession
from db_conductor.adapters.mongo_adapter import MongoDBAdapter
from db_conductor.adapters.mysql_adapter import MySQLAdapter
from db_conductor.utils.spark_helper import join_dataframes

# Initialize Spark session
spark = SparkSession.builder.appName("ETL with Different Data Sources").getOrCreate()

# Step 1: Load Source 1 static data and stash it in MongoDB
source1_data = spark.read.csv("hdfs://path/to/source1_static.csv")

mongo_adapter = MongoDBAdapter(host="localhost", port=27017, database="etl_db", collection="source1_static")
mongo_adapter.connect()
mongo_adapter.save_batch([row.asDict() for row in source1_data.collect()])
mongo_adapter.disconnect()

# Step 2: Wait for Source 2 (dynamic data) and load it into Spark
source2_data = spark.read.csv("hdfs://path/to/source2_dynamic.csv")

# Step 3: Fetch Source 1 static data from MongoDB
mongo_adapter.connect()
source1_stashed_data = mongo_adapter.get_batch({})
source1_df = spark.createDataFrame(source1_stashed_data)
mongo_adapter.disconnect()

# Step 4: Join the two datasets (Source 1 and Source 2) using PySpark
joined_data = source2_data.join(source1_df, on="id")

# Step 5: Store the final joined data in MySQL
mysql_adapter = MySQLAdapter(host="localhost", user="root", password="password", database="etl_target_db")
mysql_adapter.connect()
mysql_adapter.save_batch([row.asDict() for row in joined_data.collect()], table_name="joined_data")
mysql_adapter.commit()
mysql_adapter.disconnect()

# Step 6: Cleanup and stop Spark session
spark.stop()
```

### **Explanation of Key Steps**

- **Source 1 (Static Data)**: Data from Source 1 is read into a Spark DataFrame and stashed in **MongoDB** using the `save_batch()` method.
- **Source 2 (Dynamic Data)**: Once Source 2 data arrives, it is read into another Spark DataFrame.
- **Join Operation**: Data from both sources is joined in memory using PySpark's `join()` method.
- **Final Data Storage**: The joined data is then stored in **MySQL** using `save_batch()`, with the `commit()` method ensuring all changes are saved.

---

## **Detailed Design and API**

### **1. Database Adapter APIs**

The base adapter and all specific database adapters (e.g., MySQL, MongoDB) expose a uniform set of methods for interacting with the database:

- **`connect()`**: Establishes a connection to the database.
- **`save()` / `save_batch()`**: Inserts or saves records into the database, supporting both single record and batch inserts.
- **`get()` / `get_batch()`**: Retrieves a single record or batch of records from the database based on filters.
- **`update()` / `update_batch()`**: Updates existing records in the database.
- **`delete()` / `delete_batch()`**: Deletes single or multiple records from the database.
- **`commit()`**: Commits the transaction (for transactional databases).
- **`rollback()`**: Rolls back the transaction in case of errors.
- **`disconnect()`**: Closes the database connection.

Refer to the `base_adapter.py` design for detailed API documentation.

---

## **Development Workflow**

### **Step 1: Setup Your Development Environment**

Ensure you have the following dependencies installed:
- Python 3.7+
- PySpark 3.x
- Docker (if using containers for testing)

### **Step 2: Implement Adapters**

Start by implementing individual database adapters by extending the `BaseAdapter` class in `db_conductor/adapters`. Each adapter should implement methods like `connect()`, `save()`, `get()`, and so on, according to the specific database's API.

### **Step 3: Unit Testing**

For each adapter, add unit tests in the `tests/` directory. Ensure that tests cover all CRUD operations (Create, Read, Update, Delete) and edge cases.

Use the following command to run tests across environments:
```bash
tox
```

### **Step 4: Build CI/CD Pipeline**

We recommend using **GitHub Actions** for CI/CD. A sample workflow (`ci.yml`) is included in the repository, which runs tests on every push or pull request.

---

## **Future Extensions**

- **Support for More Databases**: Extend the library

 to support additional databases like Cassandra, Redis, or Elasticsearch.
- **Streaming Integrations**: Add integrations with real-time processing systems like Kafka or Spark Streaming.
- **Advanced Monitoring**: Integrate with monitoring tools like Prometheus to track the performance of database operations.

---