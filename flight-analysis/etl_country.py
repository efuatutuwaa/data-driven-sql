# 1.  importing libraries
import time
import os
import requests
import mysql.connector
from mysql.connector import Error

class CountriesETL:
    def __init__(self, db_config,  api_key):
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
        total = 252

        for offset in range(0, total, limit):
            params = {
                'access_key': self.api_key,
                'limit': limit,
                'offset': offset
            }

            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Raise exception for HTTP errors
                data = response.json()

                # Append data to records
                records.extend(data.get('data', []))
                print(f"Fetched {len(data.get('data', []))} records for offset {offset}")

                # Manage the  API rate limits
                time.sleep(1)

            except requests.exceptions.RequestException as err:
                print(f"Error fetching data: {err}")
                break

        print(f"Total records retrieved: {len(records)}")
        return records

    def transform_data(self, country_data):
        """Transforms countries data from AviationStack countries API"""
        transformed_data = []
        for country in country_data:
            transformed_data.append((
                country.get('country_name'),
                country.get('country_iso2'),
                country.get('country_iso3'),
                country.get('country_iso_numeric'),
                country.get('population'),
                country.get('capital'),
                country.get('continent'),
                country.get('currency_name'),
                country.get('currency_code'),
                country.get('fips_code'),
                country.get('phone_prefix'),
                country.get('id')  # this will retrieve the country id from the API
            ))
        return transformed_data

    def load_data(self, transformed_data, table_name):
        """Loads transformed countries data from AviationStack to the countries_staging table """
        insert_query = f"""
        INSERT INTO {table_name} (
        country_name, country_iso2, country_iso3, country_iso_numeric, population, 
        capital, continent, currency_name, currency_code, fips_code, phone_prefix, api_country_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        try:
            self.cursor.executemany(insert_query, transformed_data)
            self.conn.commit()
            print(f"Inserted {len(transformed_data)} records into {table_name}")
        except mysql.connector.Error as err:
            print(f"Error loading data into {table_name} : {err}")

    def run(self, table_name):
        """Starting the ETL processes."""
        self.connect_db()
        self.truncate_table(table_name)
        raw_data = self.extract_data()
        if raw_data:
            transformed_data = self.transform_data(raw_data)
            self.load_data(transformed_data, table_name)
        self.close_db_connection()


# database configurations
db_config = {
    "host": "localhost",
    "user": "root",
    "password": os.getenv('DB_PASSWORD'),
    "database": "staging_flights",
}

api_key = os.getenv('API_KEY')

# call the class to run the ETL process for `countries_staging`
countries_etl = CountriesETL(db_config, api_key)
countries_etl.run('countries_staging')














