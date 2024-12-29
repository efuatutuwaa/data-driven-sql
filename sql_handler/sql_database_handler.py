import json
import os
import mysql.connector
from mysql.connector import Error


class SQLDatabaseHandler:
    def __init__(self, db_type):
        """Initialises the database handler with the given database type."""
        self.db_type = db_type
        self.db_config = self.load_db_config()
        self.conn = None
        self.cursor = None

    def load_db_config(self):
        """Loads the database config from the config file"""
        try:
            base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            config_file = os.path.join(base_dir, "config", "db_config.json")
            with open(config_file, "r") as f:
                config =json.load(f)
            if self.db_type not in config['database']:
                raise ValueError(f"Invalid database type : {self.db_type}")
            db_config = {
                "host": config.get('host'),
                "user": config.get('user'),
                "password": os.getenv('DB_PASSWORD'),
                "database": config['database'].get(self.db_type)
            }
            return db_config
        except FileNotFoundError:
            print("Database config file not found")
            raise
        except json.JSONDecodeError as err:
            print(f"JSON decoding error: {err}")
            raise

    def connect_to_db(self):
        """Connects to the MySQL database."""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print(f"Connected to MySQL database {self.db_type}")
        except Error as err:
            print(f"Error connecting to MySQL database {self.db_type}: {err}")
            raise

    def close_db_connection(self):
        """Closes the database connection."""

        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
            print(f"Closed connection to {self.db_type} database")

    def truncate_table(self, table_name):
        """Truncates a table. Removes rows all records without deleting the table
        :param table_name: The name of the table to truncate"""

        try:
            self.cursor.execute(f"TRUNCATE TABLE {table_name}")
            self.conn.commit()
            print(f"Truncated table {table_name} in {self.db_type} database")
        except Error as err:
            print(f"Error truncating table {table_name} in {self.db_type} database :{err}")
            raise

    def execute_query(self, query, data=None):
        """Executes the given SQL query, with optional data.
        :param query: SQL query to execute
        :type query: str
        :param data: Data to be passed into the query"""

        try:
            if data:
                self.cursor.execute(query, data)
            else:
                self.cursor.execute(query)
            self.conn.commit()
            print(f"Query executed successfully on {self.db_type} database")
        except Error as err:
            print(f"Error executing query on {self.db_type} database: {err}")
            raise

    def execute_many(self, query, data_list):
        """Executes a batch query using execute many
        :param query: SQL query to execute
        :param data_list: Data to be passed into the query"""

        try:
            self.cursor.executemany(query, data_list)
            self.conn.commit()
            print(f"Query executed successfully on {self.db_type} database")
        except Error as err:
            print(f"Error executing query on {self.db_type} database: {err}")
            raise

    def upsert_data(self, query, data_list):
        """Performs an UPSERT query operation using the provided query
        :param query: The UPSERT SQL query to execute
        :param data_list: Data to be passed into the query"""

        try:
            self.cursor.executemany(query, data_list)
            self.conn.commit()
            print(f"Upserted {self.cursor.rowcount} rows to {self.db_type} database")
        except Error as err:
            print(f"Error executing query on {self.db_type} database: {err}")
            raise

    def fetch_query(self, query, data=None):
        """Executes a SELECT query and fetches the results
        :param query: SQL query to execute
        :param data: Data to be passed into the query
        :return: list - query results"""

        try:
            if data:
                self.cursor.execute(query, data)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Error as err:
            print(f"Error executing query on {self.db_type} database: {err}")



