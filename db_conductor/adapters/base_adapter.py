from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any

class BaseAdapter(ABC):
    """
    Abstract base class for all database adapters.

    This class defines the common interface that all concrete database
    adapters must implement.

    :param host: The database host.
    :type host: str
    :param user: The database user.
    :type user: str
    :param password: The database password.
    :type password: str
    :param database: The name of the database.
    :type database: str
    :param port: The port to connect to the database, defaults to None.
    :type port: int, optional
    """

    def __init__(self, 
                 host: str,
                 user: str, 
                 password: str, 
                 database: str, 
                 port: Optional[int] = None
                ) -> None:
        """
        Initialize the base adapter with the database connection parameters.

        :param host: The database host.
        :type host: str
        :param user: The database user.
        :type user: str
        :param password: The database password.
        :type password: str
        :param database: The name of the database.
        :type database: str
        :param port: The port to connect to the database, defaults to None.
        :type port: int, optional
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.connection = None  # Holds the database connection object
        self._connected = False
        self._transaction_active = False

    @abstractmethod
    def connect(self) -> None:
        """
        Establish a connection to the database.

        This method must be implemented by concrete adapters.
        """
        raise NotImplementedError("Implement the concrete class")

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close the connection to the database.

        This method must be implemented by concrete adapters.
        """
        raise NotImplementedError("Implement the concrete class")

    @abstractmethod
    def save(self, record: Dict[str, Any]) -> None:
        """
        Save a single record to the database.

        :param record: The record to be saved in the database.
        :type record: dict
        """
        raise NotImplementedError("Implement the concrete class")

    @abstractmethod
    def save_batch(self, records: List[Dict[str, Any]]) -> None:
        """
        Save multiple records to the database in a single transaction.

        :param records: A list of dictionaries representing the records.
        :type records: list of dict
        """
        raise NotImplementedError("Implement the concrete class")

    @abstractmethod
    def update(self, record: Dict[str, Any], condition: Dict[str, Any]) -> None:
        """
        Update a single record in the database based on a condition.

        :param record: The updated record values.
        :type record: dict
        :param condition: A condition to match the record to update.
        :type condition: dict
        """
        raise NotImplementedError("Implement the concrete class")

    @abstractmethod
    def update_batch(self, records: List[Dict[str, Any]], conditions: List[Dict[str, Any]]) -> None:
        """
        Update multiple records in the database based on conditions.

        :param records: A list of dictionaries representing the updated records.
        :type records: list of dict
        :param conditions: A list of conditions for each record to update.
        :type conditions: list of dict
        """
        raise NotImplementedError("Implement the concrete class")

    @abstractmethod
    def get(self, condition: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fetch a single record from the database based on a condition.

        :param condition: A condition to match the record to fetch.
        :type condition: dict
        :returns: The fetched record.
        :rtype: dict or None
        """
        raise NotImplementedError("Implement the concrete class")

    @abstractmethod
    def get_batch(self, conditions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Fetch multiple records from the database based on conditions.

        :param conditions: A list of conditions for each record to fetch.
        :type conditions: list of dict
        :returns: A list of fetched records.
        :rtype: list of dict
        """
        raise NotImplementedError("Implement the concrete class")

    @abstractmethod
    def delete(self, condition: Dict[str, Any]) -> None:
        """
        Delete a single record from the database based on a condition.

        :param condition: A condition to match the record to delete.
        :type condition: dict
        """
        raise NotImplementedError("Implement the concrete class")

    @abstractmethod
    def delete_batch(self, conditions: List[Dict[str, Any]]) -> None:
        """
        Delete multiple records from the database based on conditions.

        :param conditions: A list of conditions for each record to delete.
        :type conditions: list of dict
        """
        raise NotImplementedError("Implement the concrete class")

    def commit(self) -> None:
        """
        Commit the current transaction.

        This should be called after a successful save/update/delete operation.
        """
        if self._transaction_active:
            try:
                self.connection.commit()
                self._transaction_active = False
                print("Transaction committed successfully.")
            except Exception as e:
                print(f"Error during commit: {e}")
                self.rollback()
        else:
            print("No active transaction to commit.")

    def rollback(self) -> None:
        """
        Roll back the current transaction in case of failure.

        This should be called when an error occurs during a transaction.
        """
        if self._transaction_active:
            try:
                self.connection.rollback()
                self._transaction_active = False
                print("Transaction rolled back successfully.")
            except Exception as e:
                print(f"Error during rollback: {e}")
        else:
            print("No active transaction to roll back.")

    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a raw SQL query and return the result.

        :param query: The raw SQL query to execute.
        :type query: str
        :param params: Optional query parameters for binding.
        :type params: tuple, optional
        :return: The query result, usually a list of dictionaries.
        :rtype: list of dict or None
        """
        try:
            self.ensure_connection()
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")
            self.rollback()
            return None

    def ensure_connection(self) -> None:
        """
        Ensure the database connection is active, reconnect if necessary.
        """
        if not self._connected:
            print("Reconnecting to the database.")
            self.connect()

    def close_connection(self) -> None:
        """
        Close the database connection if active.
        """
        if self._connected:
            print("Closing the database connection.")
            self.disconnect()
            self._connected = False
