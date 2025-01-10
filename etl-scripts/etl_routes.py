# import libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os
import requests
import time


class RoutesETL:
    def __init__(self, db_handler, api_key):
        self.db_handler = db_handler
        self.api_key = api_key

    def get_routes(self):
        """Extracts airline data from Aviation stack API"""
        url = "https://api.aviationstack.com/v1/routes"
        records = []
        offset = 0
        limit = 1000
        total = 308033

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

    def transform_routes(self, routes_data):
        transformed_routes = []
        for route in routes_data:
            departure = route.get("departure", {})
            arrival = route.get("arrival", {})
            flight = route.get("flight", {})
            airline = route.get("airline", {})
            transformed_routes.append((
                departure.get("airport"),
                departure.get("timezone"),
                departure.get("iata"),
                departure.get("icao"),
                departure.get("terminal"),
                departure.get("time"),
                arrival.get("airport"),
                arrival.get("timezone"),
                arrival.get("iata"),
                arrival.get("icao"),
                arrival.get("terminal"),
                arrival.get("time"),
                flight.get('number'),
                airline.get('iata'),
                airline.get('icao')
            ))
        return transformed_routes

    def load_data(self, transformed_routes, table_name):

        truncate_query = f"""TRUNCATE TABLE {table_name};"""
        insert_query = f"""INSERT INTO {table_name} (
        departure_airport,departure_timezone, departure_iata,departure_icao,departure_terminal, 
        scheduled_departure_time, arrival_airport, arrival_timezone, arrival_iata, 
        arrival_icao,arrival_terminal, scheduled_arrival_time, flight_number, airline_iata,airline_icao
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s)
        ;"""

        try:
            self.db_handler.execute_query(truncate_query)  # deletes existing records in the table

            self.db_handler.execute_many(insert_query, transformed_routes)
            print(f"Inserted {len(transformed_routes)} records into {table_name}")

        except Exception as err:
            print(f"Error loading data into {table_name}: {err}")

    def run(self, table_name):
        """Runs the ETL script"""
        raw_data = self.get_routes()
        transformed_routes = self.transform_routes(raw_data)
        self.load_data(transformed_routes, table_name)


# initialise the database and run the ETL
db_handler = SQLDatabaseHandler('staging')
db_handler.connect_to_db()

api_key = os.getenv('API_KEY')
airline_etl = RoutesETL(db_handler, api_key)
airline_etl.run("airline_route_staging")
db_handler.close_db_connection()
