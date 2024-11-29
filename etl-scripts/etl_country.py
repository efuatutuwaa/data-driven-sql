# import lib
from sql_handler.sql_database_handler import SQLDatabaseHandler
from data_cleaning_scripts.data_cleaning import CountryDataCleaner
import os
import requests
import time

class CountriesETL:
    def __init__(self, db_handler, api_key):
        self.db_handler = db_handler
        self.api_key = api_key
        self.cleaner = CountryDataCleaner()

    def extract_data(self):
        """Extract data from AviationStack API."""
        url = "https://api.aviationstack.com/v1/countries"
        records = []
        offset = 0
        limit = 100
        total = 252

        for offset in range(0, total, limit):
            params = {
                'access_key': self.api_key,
                'offset': offset,
                'limit': limit
            }
            headers = {"User-Agent": "PostmanRuntime/7.29.2"}
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            records.extend(data.get('data', []))
            time.sleep(1)  # Rate limit handling

        print(f"Total records retrieved: {len(records)}")
        return records

    def transform_data(self, country_data):
        """Transform and clean the data."""
        transformed_data = []
        for country in country_data:
            corrected_name = self.cleaner.clean_country_name(country.get('country_name'))
            transformed_data.append((
                corrected_name,
                country.get('country_iso2') or 'XX',
                country.get('country_iso3') or 'XXX',
                country.get('country_iso_numeric') or 0,
                country.get('population') or 0,
                country.get('capital') or 'Unknown',
                country.get('continent') or 'Unknown',
                country.get('currency_name') or 'Unknown',
                country.get('currency_code') or 'Unknown',
                country.get('fips_code') or 'XX',
                country.get('phone_prefix') or 0,
                country.get('id')
            ))
        return transformed_data

    def load_data(self, transformed_data, table_name):
        """Load data into the staging table."""
        truncate_query = f"TRUNCATE TABLE {table_name}"
        insert_query = f"""
        INSERT INTO {table_name} (
            country_name, country_iso2, country_iso3, country_iso_numeric, population, 
            capital, continent, currency_name, currency_code, fips_code, phone_prefix, api_country_id,
            updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        try:
            # Truncate the staging table first
            self.db_handler.execute_query(truncate_query)

            # Insert the transformed data into the staging table
            self.db_handler.execute_many(insert_query, transformed_data)
            print(f"Inserted {len(transformed_data)} records into {table_name}")
        except Exception as e:
            print(f"Error loading data into {table_name}: {e}")

    def run(self, table_name):
        """Run the ETL process."""
        raw_data = self.extract_data()
        transformed_data = self.transform_data(raw_data)
        self.load_data(transformed_data, table_name)


# Initialize and run the ETL process
db_handler = SQLDatabaseHandler("staging")
db_handler.connect_to_db()

api_key = os.getenv("API_KEY")
countries_etl = CountriesETL(db_handler, api_key)
countries_etl.run("countries_staging")
db_handler.close_db_connection()
