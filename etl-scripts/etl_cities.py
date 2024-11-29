# import libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os
import requests
import time


class CitiesETL:
    def __init__(self, db_handler, api_key):
        self.db_handler = db_handler
        self.api_key = api_key

    def get_cities(self):
        """Extracts cities data from Aviation stack api"""
        url = 'https://api.aviationstack.com/v1/cities'
        records = []
        offset = 0
        limit = 100
        total = 9370

        for offset in range(0, total, limit):
            params ={
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

                # appends data to records
                records.extend(data.get('data', []))
                print(f"Fetched {len(data.get("data", []))} records for offset {offset}")

                # manage the API rate limits
                time.sleep(1)

            except requests.exceptions.RequestException as err:
                print(f"Error fetching data: {err}")

        print(f"Total records: {len(records)}")
        return records

    def tranform_records(self, city_data):
        """Transforms the data from the city API"""
        transformed_records = []
        for city in city_data:
            transformed_records.append((
                city.get('city_name'),
                city.get('iata_code'),
                city.get('country_iso2'),
                city.get('latitude'),
                city.get('longitude'),
                city.get('timezone'),
                city.get('gmt'),
                city.get('geoname_id'),
                city.get('id')  # this retrieves the city id from the API
            ))
        return transformed_records

    def load_data(self, transformed_records, table_name):

        truncate_query = f"TRUNCATE TABLE {table_name}"
        insert_query = f"""
        INSERT INTO {table_name}(
        city_name, iata_code, country_iso2, latitude, longitude,timezone_name, gmt, geoname_id, api_city_id
        ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        # truncate the cities_staging table first
        try:
            self.db_handler.execute_query(truncate_query)

            # insert the transformed data into the cities_table
            self.db_handler.execute_many(insert_query, transformed_records)
            print(f"Inserted {len(transformed_records)} records into {table_name}")

        except Exception as err:
            print(f"Error loading data into {table_name}: {err}")

    def run(self, table_name):
        """Runs the ETL script"""
        raw_records = self.get_cities()
        transformed_records = self.tranform_records(raw_records)
        self.load_data(transformed_records, table_name)


# initialise database and run the ETL
db_handler = SQLDatabaseHandler('staging')
db_handler.connect_to_db()

api_key = os.getenv('API_KEY')
cities_etl = CitiesETL(db_handler, api_key)
cities_etl.run("cities_staging")
db_handler.close_db_connection()





