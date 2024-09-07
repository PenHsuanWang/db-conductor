from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from typing import List, Dict, Optional, Any
from db_conductor.adapters.base_adapter import BaseAdapter


class MongoDBAdapter(BaseAdapter):
    """
    Concrete adapter for MongoDB, implementing the BaseAdapter interface.
    """

    def __init__(self, 
                 host: str,
                 user: str,
                 password: str,
                 database: str,
                 port: Optional[int] = None,
                 collection_name: str = ""
                 ) -> None:
        """
        Initialize the MongoDB adapter with the connection parameters.
        
        :param collection_name: The name of the collection.
        :type collection_name: str
        """
        super().__init__(host, user, password, database, port)
        self.collection_name = collection_name
        self.client: Optional[MongoClient] = None  # Holds the MongoClient object
        self.collection: Optional[Collection] = None  # Holds the MongoDB collection

    def connect(self) -> None:
        """
        Establish a connection to the MongoDB server and select the database.
        """
        try:
            self.client = MongoClient(
                host=self.host,
                port=self.port or 27017,
                username=self.user,
                password=self.password
            )
            self.connection = self.client[self.database]
            self.collection = self.connection[self.collection_name]
            self._connected = True
            print(f"Connected to MongoDB at {self.host}:{self.port}")
        except PyMongoError as e:
            raise RuntimeError(f"Error connecting to MongoDB: {e}")

    def disconnect(self) -> None:
        """
        Close the MongoDB connection.
        """
        if self.client:
            self.client.close()
            self._connected = False
            print("Disconnected from MongoDB")

    def save(self, record: Dict[str, Any]) -> None:
        """
        Insert a single document into the collection.
        """
        if self.collection:
            self.collection.insert_one(record)
            print(f"Record inserted: {record}")
        else:
            raise RuntimeError("Collection is not selected.")

    def save_batch(self, records: List[Dict[str, Any]]) -> None:
        """
        Insert multiple documents into the collection.
        """
        if self.collection:
            self.collection.insert_many(records)
            print(f"Batch of records inserted: {records}")
        else:
            raise RuntimeError("Collection is not selected.")

    def update(self, record: Dict[str, Any], condition: Dict[str, Any]) -> None:
        """
        Update a single document based on a condition.
        """
        if self.collection:
            result = self.collection.update_one(condition, {"$set": record})
            if result.modified_count > 0:
                print(f"Record updated: {record}")
            else:
                print("No records matched the condition.")
        else:
            raise RuntimeError("Collection is not selected.")

    def update_batch(self, records: List[Dict[str, Any]], conditions: List[Dict[str, Any]]) -> None:
        """
        Update multiple documents based on conditions.
        """
        if self.collection:
            for record, condition in zip(records, conditions):
                result = self.collection.update_one(condition, {"$set": record})
                if result.modified_count > 0:
                    print(f"Record updated: {record}")
                else:
                    print("No records matched the condition.")
        else:
            raise RuntimeError("Collection is not selected.")

    def get(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fetch a single document based on a condition.
        """
        if self.collection:
            document = self.collection.find_one(condition)
            return document
        else:
            raise RuntimeError("Collection is not selected.")

    def get_batch(self, conditions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Fetch multiple documents based on conditions.
        """
        if self.collection:
            documents = []
            for condition in conditions:
                documents.append(self.collection.find_one(condition))
            return documents
        else:
            raise RuntimeError("Collection is not selected.")

    def delete(self, condition: Dict[str, Any]) -> None:
        """
        Delete a single document based on a condition.
        """
        if self.collection:
            result = self.collection.delete_one(condition)
            if result.deleted_count > 0:
                print(f"Record deleted with condition: {condition}")
            else:
                print("No records matched the condition.")
        else:
            raise RuntimeError("Collection is not selected.")

    def delete_batch(self, conditions: List[Dict[str, Any]]) -> None:
        """
        Delete multiple documents based on conditions.
        """
        if self.collection:
            for condition in conditions:
                result = self.collection.delete_one(condition)
                if result.deleted_count > 0:
                    print(f"Record deleted with condition: {condition}")
                else:
                    print("No records matched the condition.")
        else:
            raise RuntimeError("Collection is not selected.")

    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a raw MongoDB query.
        
        This is a simplified example as MongoDB does not use raw SQL queries.
        """
        raise NotImplementedError("Raw query execution is not supported in MongoDB")

    def ensure_connection(self) -> None:
        """
        Ensure the MongoDB connection is active.
        """
        if not self._connected:
            print("Reconnecting to MongoDB.")
            self.connect()
