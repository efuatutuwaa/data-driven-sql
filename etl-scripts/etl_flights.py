# import libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os
import requests
import time


class FlightETL:
    def __init__(self, db_handler, api_key):
        self.db_handler = db_handler
        self.api_key = api_key

    def get_flight(self):
        """Extracts airline data from Aviation stack API"""
        url = "https://api.aviationstack.com/v1/flights"
        records = []
        offset = 0
        limit = 100
        total = 22346263

        for offset in range(0, total, limit):
            params = {
                'access_key': self.api_key,
                'offset': offset,
                'limit': limit
            }
            headers = {
                "User-Agent": "Python-Requests/2.28.1"

            }
            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()

                # append api data to records list
                records.extend(data.get("data", []))
                print(f"Fetched {len(data.get("data", []))} records for offset {offset}")

                # manage the API rate limit
                time.sleep(30)

            except requests.exceptions.RequestException as err:
                print(f"Error fetching data: {err}")

        print(f"Total records: {len(records)}")
        return records

    def transform_flight(self, flight_data):
        transformed_flights_data = []
        for flights in flight_data:
            transformed_flights_data.append((
                flights.get("flight_date"),
                flights.get("flight_status"),
                flights.get("number"),
                flights.get("iata"),
                flights.get("icao"),
                flights.get("codeshared")
            ))
        return transformed_flights_data

    def load_data(self, transformed_flights_data, table_name):

        truncate_query = f"""TRUNCATE TABLE {table_name};"""
        insert_query = f"""INSERT INTO {table_name} (
        flight_date, flight_status, flight_number, flight_iata,
        flight_icao, flight_codeshared
        ) VALUES (%s, %s, %s, %s, %s, %s)
        ;"""

        try:
            self.db_handler.execute_query(truncate_query)  # deletes existing records in the table

            self.db_handler.execute_many(insert_query, transformed_flights_data)
            print(f"Inserted {len(transformed_flights_data)} records into {table_name}")

        except Exception as err:
            print(f"Error loading data into {table_name}: {err}")

    def run(self, table_name):
        """Runs the ETL script"""
        raw_data = self.get_flight()
        transformed_flights_data = self.transform_flight(raw_data)
        self.load_data(transformed_flights_data, table_name)


# initialise the database and run the ETL
db_handler = SQLDatabaseHandler('staging')
db_handler.connect_to_db()

api_key = os.getenv('API_KEY')
airline_etl = FlightETL(db_handler, api_key)
airline_etl.run("flight_details_staging")
db_handler.close_db_connection()
