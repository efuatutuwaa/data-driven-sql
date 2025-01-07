# import libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os
import requests
import time


class AirlineETL:
    def __init__(self, db_handler, api_key):
        self.db_handler = db_handler
        self.api_key = api_key

    def get_airlines(self):
        """Extracts airline data from Aviation stack API"""
        url = "https://api.aviationstack.com/v1/airlines"
        records = []
        offset = 0
        limit = 1000
        total = 13135

        for offset in range(0, total, limit):
            params = {
                'access_key': self.api_key,
                'offset': offset,
                'limit': limit
            }

            headers = {
                "User-Agent": "PostmanRuntime/7.29.2"
            }

            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()

                # append api data to records list
                records.extend(data.get("data", []))
                print(f"Fetched {len(data.get("data", []))} records for offset {offset}")

                # manage the API rate limit
                time.sleep(1)

            except requests.exceptions.RequestException as err:
                print(f"Error fetching data: {err}")

        print(f"Total records: {len(records)}")
        return records

    def transform_airline(self, airline_data):
        transformed_airline_data = []
        for airline in airline_data:
            transformed_airline_data.append((
                airline.get("airline_name") or 'Missing airline name',
                airline.get("iata_code") or 'UNK',
                airline.get("icao_code") or 'UNK',
                airline.get("type") or 'UNK',
                airline.get("status") or 'UNK',
                airline.get("fleet_size") or 0,
                airline.get("fleet_average_age") or 0,
                airline.get("date_founded") or 1900,
                airline.get("hub_code") or 'UNK',
                airline.get("country_name"),
                airline.get("country_iso2"),
                airline.get("id")  # returns airline id from API
            ))
        return transformed_airline_data

    def load_data(self, transformed_airline_data, table_name):
        """Inserts transformed airline data into the airline_staging table"""
        truncate_query = f"""TRUNCATE TABLE {table_name};"""
        insert_query = f"""INSERT INTO {table_name} (
        airline_name, airline_iata, airline_icao, airline_type, airline_status, fleet_size, 
        fleet_average_age, date_founded, hub_code, country_name, country_iso2,api_airline_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ;"""

        try:
            self.db_handler.execute_query(truncate_query)  # deletes existing records in the table

            self.db_handler.execute_many(insert_query, transformed_airline_data)
            print(f"Inserted {len(transformed_airline_data)} records into {table_name}")

        except Exception as err:
            print(f"Error loading data into {table_name}: {err}")

    def run(self, table_name):
        """Runs the ETL script"""
        raw_data = self.get_airlines()
        transformed_airline_data = self.transform_airline(raw_data)
        self.load_data(transformed_airline_data, table_name)


# initialise the database and run the ETL
db_handler = SQLDatabaseHandler('staging')
db_handler.connect_to_db()

api_key = os.getenv('API_KEY')
airline_etl = AirlineETL(db_handler, api_key)
airline_etl.run("airline_staging")
db_handler.close_db_connection()
