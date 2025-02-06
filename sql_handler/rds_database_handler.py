
import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv


class RDSDatabaseHandler:
    def __init__(self):
        """Initialise the RDS database handler."""
        load_dotenv(dotenv_path='../.env')  # loads the .env variables
        self.conn = None
        self.cursor = None
        self.db_config = self.load_rds_config()  # Call method to load RDS config

    def load_rds_config(self):
        """Load the RDS database configuration file."""
        try:
            rds_config = {
                'host': os.getenv('RDS_HOST'),
                'user': os.getenv('RDS_USER'),
                'password': os.getenv('RDS_PASSWORD'),
                'database': os.getenv('RDS_DB_NAME'),
                'port': os.getenv('RDS_PORT')
            }
            return rds_config
        except KeyError as e:
            print(f"Missing environment variable: {e}")
            raise

    def connect_to_db(self):
        """Connects to the RDS database."""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("Connected to RDS Database")
        except Error as err:
            print(f"Error connecting to RDS Database: {err}")
            raise

    def close_db_connection(self):
        """Close the RDS database connection."""
        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
            print("RDS database connection closed")

    def begin_transaction(self):
        """Begins a database transaction by disabling autocommit"""
        if self.conn:
            self.conn.autocommit = False
            print("Transaction begun successfully")

    def commit_transaction(self):
        """Commits the transaction and re-enables autocommit"""
        if self.conn:
            self.conn.commit()
            self.conn.autocommit = True
            print("Transaction commit successfully")

    def rollback_transaction(self):
        """Rolls back the transaction and re-enables autocommit"""
        if self.conn:
            self.conn.rollback()
            self.conn.autocommit = False
            print("Transaction rollback successfully")

    def execute_query(self, query, data=None):
        """Executes a single SQL query on the RDS database."""
        try:
            if data:
                self.cursor.execute(query, data)
            else:
                self.cursor.execute(query)
            self.conn.commit()
            print(f"Query executed successfully on RDS Database")
        except Error as err:
            print(f"Error executing query on RDS Database: {err}")

    def execute_many(self, query, data_list):
        """Executes a batch query using execute many"""
        try:
            self.cursor.executemany(query, data_list)
            self.conn.commit()
            print(f"Batch query executed successfully on RDS Database")
        except Error as err:
            print(f"Error executing query on RDS Database: {err}")
            raise

    def upsert_data(self, query, data_list):
        """Performs an UPSERT query operation using the provided query"""
        try:
            self.cursor.executemany(query, data_list)
            self.conn.commit()
            print(f"Upserted {self.cursor.rowcount} rows to RDS database")
        except Error as err:
            print(f"Error performing upsert on RDS Database: {err}")
            raise

    def fetch_query(self, query, data=None):
        """Executes a SELECT query and fetches the results"""
        try:
            if data:
                self.cursor.execute(query, data)
            else:
                self.cursor.execute(query)
            return self.cursor.fetchall()
        except Error as err:
            print(f"Error fetching query from RDS Database: {err}")

    def truncate_table(self, table_name):
        """Truncates a table."""
        try:
            self.cursor.execute(f"TRUNCATE TABLE {table_name}")
            self.conn.commit()
            print(f"Table {table_name} truncated successfully in RDS database")
        except Error as err:
            print(f"Error truncating table {table_name} in RDS Database: {err}")
            raise
