# import libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os
import requests
import time


class AirportETL:
    def __init__(self, db_handler, api_key):
        self.db_handler = db_handler
        self.api_key = api_key

    def get_airports(self):
        """Extracts airline data from Aviation stack API"""
        url = "https://api.aviationstack.com/v1/airports"
        records = []
        offset = 0
        limit = 100
        total = 6710

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
                time.sleep(1)

            except requests.exceptions.RequestException as err:
                print(f"Error fetching data: {err}")

        print(f"Total records: {len(records)}")
        return records

    def transform_airport(self, airport_data):
        transformed_airport_data = []
        for airport in airport_data:
            transformed_airport_data.append((
                airport.get("airport_name"),
                airport.get("iata_code"),
                airport.get("icao_code"),
                airport.get("latitude"),
                airport.get("longitude"),
                airport.get("geoname_id"),
                airport.get("timezone"),
                airport.get("gmt"),
                airport.get("country_iso2"),
                airport.get("city_iata_code"),
            ))
        return transformed_airport_data

    def load_data(self, transformed_airport_data, table_name):

        truncate_query = f"""TRUNCATE TABLE {table_name};"""
        insert_query = f"""INSERT INTO {table_name} (
        airport_name, airport_iata, airport_icao, latitude, longitude,
        geoname_id, timezone_name, gmt_offset, country_iso2, city_iata_code
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ;"""

        try:
            self.db_handler.execute_query(truncate_query)  # deletes existing records in the table

            self.db_handler.execute_many(insert_query, transformed_airport_data)
            print(f"Inserted {len(transformed_airport_data)} records into {table_name}")

        except Exception as err:
            print(f"Error loading data into {table_name}: {err}")

    def run(self, table_name):
        """Runs the ETL script"""
        raw_data = self.get_airports()
        transformed_airport_data = self.transform_airport(raw_data)
        self.load_data(transformed_airport_data, table_name)


# initialise the database and run the ETL
db_handler = SQLDatabaseHandler('staging')
db_handler.connect_to_db()

api_key = os.getenv('API_KEY')
airline_etl = AirportETL(db_handler, api_key)
airline_etl.run("airport_staging")
db_handler.close_db_connection()
