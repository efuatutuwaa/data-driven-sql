# 1.  importing libraries
import os
import requests
import mysql.connector
from mysql.connector import Error

class CountriesETL:
    def __init__(self, db_config, api_key):
        self.db_config = db_config
        self.api_key = api_key
        self.conn = None
        self.cursor = None

    def connect_db(self):
        """Connect to the database."""
        try:
            self.conn = mysql.connector.connect(**self.db_config)
            self.cursor = self.conn.cursor()
            print("Connected to MySQL database")

        except mysql.connector.Error as err:
            print(f"Failed connecting to MySQL database: {err}")

    def close_db_connection(self):
        """Close the database connection."""
        if self.conn and self.conn.is_connected():
            self.cursor.close()
            self.conn.close()
            print("MySQL connection is closed")

    def truncate_table(self, table_name):
        """Clears the staging table to prevent duplicates."""
        try:
            self.cursor.execute(f"TRUNCATE TABLE {table_name}")
            self.conn.commit()
            print(f"Truncated {table_name}")

        except mysql.connector.Error as err:
            print(f"Error truncating {table_name} : {err}")

    def extract_data(self):
        """Extracts data from AviationStack countries API"""
        url = "https://api.aviationstack.com/v1/countries/"
        records = []
        offset = 0
        limit = 100

        while True:
            params = {
                'access_token': self.api_key,
                'limit': limit,
                'offset': offset
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                country_data = response.json().get('data', [])  # will assign an empty list to country data,
                # if 'data' is empty
                records.extend(country_data)









