from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType
from snowflake.snowpark.functions import col
from typing import List, Dict, Optional, Any
from db_conductor.adapters.base_adapter import BaseAdapter


class SnowflakeAdapter(BaseAdapter):
    """
    Concrete adapter for Snowflake, implementing the BaseAdapter interface using Snowpark.
    """

    def __init__(self, 
                 host: str,
                 user: str,
                 password: str,
                 database: str,
                 warehouse: str,
                 role: str,
                 schema: str,
                 port: Optional[int] = None,
                 table_name: str = ""
                 ) -> None:
        """
        Initialize the Snowflake adapter with the connection parameters.
        
        :param table_name: The name of the table to operate on.
        :type table_name: str
        """
        super().__init__(host, user, password, database, port)
        self.warehouse = warehouse
        self.role = role
        self.schema = schema
        self.table_name = table_name
        self.session: Optional[Session] = None

    def connect(self) -> None:
        """
        Establish a connection to the Snowflake server using Snowpark Session.
        """
        try:
            connection_params = {
                "account": self.host,
                "user": self.user,
                "password": self.password,
                "role": self.role,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema
            }
            self.session = Session.builder.configs(connection_params).create()
            self._connected = True
            print(f"Connected to Snowflake as {self.user} in {self.database}.{self.schema}")
        except Exception as e:
            raise RuntimeError(f"Error connecting to Snowflake: {e}")

    def disconnect(self) -> None:
        """
        Close the Snowflake connection.
        """
        if self.session:
            self.session.close()
            self._connected = False
            print("Disconnected from Snowflake.")

    def save(self, record: Dict[str, Any]) -> None:
        """
        Insert a single record into the Snowflake table using Snowpark.
        """
        if not self.session:
            raise RuntimeError("No active Snowflake session.")
        
        try:
            df = self.session.create_dataframe([record], schema=StructType([
                StructField(key, StringType()) for key in record.keys()
            ]))
            df.write.save_as_table(self.table_name, mode="append")
            print(f"Record inserted: {record}")
        except Exception as e:
            print(f"Error saving record: {e}")
            self.rollback()

    def save_batch(self, records: List[Dict[str, Any]]) -> None:
        """
        Insert multiple records into the Snowflake table using Snowpark.
        """
        if not self.session:
            raise RuntimeError("No active Snowflake session.")
        
        try:
            if not records:
                print("No records to insert.")
                return
            
            # Dynamically build schema based on record keys
            df = self.session.create_dataframe(records, schema=StructType([
                StructField(key, StringType()) for key in records[0].keys()
            ]))
            df.write.save_as_table(self.table_name, mode="append")
            print(f"Batch of records inserted.")
        except Exception as e:
            print(f"Error saving batch of records: {e}")
            self.rollback()

    def update(self, record: Dict[str, Any], condition: Dict[str, Any]) -> None:
        """
        Update a single record in Snowflake based on a condition.
        Snowpark does not natively support row-level update, so we need to use SQL.
        """
        if not self.session:
            raise RuntimeError("No active Snowflake session.")

        try:
            # Construct update query using Snowpark's SQL expression
            set_clause = ", ".join([f"{k} = '{v}'" for k, v in record.items()])
            condition_clause = " AND ".join([f"{k} = '{v}'" for k, v in condition.items()])
            query = f"UPDATE {self.table_name} SET {set_clause} WHERE {condition_clause}"
            self.session.sql(query).collect()
            print(f"Record updated where: {condition}")
        except Exception as e:
            print(f"Error updating record: {e}")
            self.rollback()

    def update_batch(self, records: List[Dict[str, Any]], conditions: List[Dict[str, Any]]) -> None:
        """
        Update multiple records in Snowflake based on conditions using SQL.
        """
        if not self.session:
            raise RuntimeError("No active Snowflake session.")

        try:
            for record, condition in zip(records, conditions):
                self.update(record, condition)
            print(f"Batch of records updated.")
        except Exception as e:
            print(f"Error updating batch of records: {e}")
            self.rollback()

    def get(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fetch a single record from Snowflake based on a condition using Snowpark.
        """
        if not self.session:
            raise RuntimeError("No active Snowflake session.")
        
        try:
            query = self.session.table(self.table_name)
            for key, value in condition.items():
                query = query.filter(col(key) == value)
            result = query.collect()
            if result:
                return result[0].as_dict()
            return None
        except Exception as e:
            print(f"Error fetching record: {e}")
            self.rollback()
            return None

    def get_batch(self, conditions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Fetch multiple records from Snowflake based on conditions using Snowpark.
        """
        if not self.session:
            raise RuntimeError("No active Snowflake session.")
        
        results = []
        try:
            for condition in conditions:
                result = self.get(condition)
                if result:
                    results.append(result)
            return results
        except Exception as e:
            print(f"Error fetching batch of records: {e}")
            self.rollback()
            return []

    def delete(self, condition: Dict[str, Any]) -> None:
        """
        Delete a single record from Snowflake based on a condition using Snowpark SQL.
        """
        if not self.session:
            raise RuntimeError("No active Snowflake session.")
        
        try:
            condition_clause = " AND ".join([f"{k} = '{v}'" for k, v in condition.items()])
            query = f"DELETE FROM {self.table_name} WHERE {condition_clause}"
            self.session.sql(query).collect()
            print(f"Record deleted where: {condition}")
        except Exception as e:
            print(f"Error deleting record: {e}")
            self.rollback()

    def delete_batch(self, conditions: List[Dict[str, Any]]) -> None:
        """
        Delete multiple records from Snowflake based on conditions using Snowpark SQL.
        """
        if not self.session:
            raise RuntimeError("No active Snowflake session.")
        
        try:
            for condition in conditions:
                self.delete(condition)
            print(f"Batch of records deleted.")
        except Exception as e:
            print(f"Error deleting batch of records: {e}")
            self.rollback()

    def ensure_connection(self) -> None:
        """
        Ensure the Snowflake session is active.
        """
        if not self._connected:
            print("Reconnecting to Snowflake.")
            self.connect()
