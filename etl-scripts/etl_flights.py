# import libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os
import requests
from datetime import datetime, timedelta
import time


class FlightETL:
    def __init__(self, db_handler, api_key):
        self.db_handler = db_handler
        self.api_key = api_key

    def get_flight(self, flight_date):
        """Extracts airline data from Aviation stack API"""
        url = "https://api.aviationstack.com/v1/flights"
        records = []
        offset = 0
        limit = 1000

        while True:
            params = {
                'access_key': self.api_key,
                'offset': offset,
                'limit': limit,
                'flight_date': flight_date
            }
            headers = {
                "User-Agent": "Python-Requests/2.28.1"

            }
            try:
                response = requests.get(url, params=params, headers=headers)
                response.raise_for_status()
                data = response.json()

                batch_records = data.get("data", [])
                print(f"Offset: {offset}, Records fetched: {len(batch_records)}")

                # append api data to records list
                records.extend(batch_records)
                print(f"Fetched {len(data.get('data', []))} records for offset {offset}")

                if len(batch_records) < limit:
                    break
                # update offset for the next batch of records
                offset += limit
                # manage the API rate limit
                time.sleep(1)
            except requests.exceptions.HTTPError as http_err:
                if response.status_code == 429:
                    print(f"Rate limit exceeded. Retrying...")
                    time.sleep(5)
                else:
                    print(f"HTTP error for {flight_date}, offset {offset}: {http_err}")
                    break
            except requests.exceptions.RequestException as err:
                print(f"Error fetching data: {err}")

        print(f"Total records: {len(records)}")
        return records

    def get_historical_flight_data(self, start_date, end_date):
        """Extracts historical flight data for a range of dates"""
        current_date = start_date
        all_flights = []
        skipped_flights = []
        while current_date <= end_date:
            flight_date = current_date.strftime("%Y-%m-%d")
            try:
                print(f"Fetching data for {flight_date}")
                flights = self.get_flight(flight_date)
                if flights:
                    all_flights.extend(flights)
                    print(f"Fetched {len(flights)} records for {flight_date}")
                else:
                    print(f"No flight data for {flight_date}")
            except Exception as err:
                print(f"Error fetching data: {err}")
                skipped_flights.append(flight_date)
            finally:
                current_date += timedelta(days=1)
        print(f"Total records fetched for date range :{len(all_flights)}")
        if skipped_flights:
            print(f"Skipped flights due to errors: {skipped_flights}")
        return all_flights

    def transform_flight(self, flight_data):
        transformed_flights_data = []
        for flights in flight_data:
            codeshared = flights.get('flight', {}).get('codeshared', {}) or {}
            transformed_flights_data.append((
                flights.get('flight_date'),
                flights.get('flight_status'),
                flights.get('flight', {}).get('number'),
                flights.get('flight', {}).get('iata'),
                flights.get('flight', {}).get('icao'),
                flights.get('arrival', {}).get('airport'),
                flights.get('arrival', {}).get('timezone'),
                flights.get('arrival', {}).get('iata'),
                flights.get('arrival', {}).get('icao'),
                flights.get('arrival', {}).get('terminal'),
                flights.get('arrival', {}).get('gate'),
                flights.get('arrival', {}).get('baggage'),
                flights.get('arrival', {}).get('delay'),
                flights.get('arrival', {}).get('scheduled'),
                flights.get('arrival', {}).get('estimated'),
                flights.get('arrival', {}).get('actual'),
                flights.get('arrival', {}).get('estimated_runway'),
                flights.get('arrival', {}).get('actual_runway'),
                flights.get('departure', {}).get('airport'),
                flights.get('departure', {}).get('timezone'),
                flights.get('departure', {}).get('iata'),
                flights.get('departure', {}).get('icao'),
                flights.get('departure', {}).get('terminal'),
                flights.get('departure', {}).get('gate'),
                flights.get('departure', {}).get('delay'),
                flights.get('departure', {}).get('scheduled'),
                flights.get('departure', {}).get('estimated'),
                flights.get('departure', {}).get('actual'),
                flights.get('departure', {}).get('estimated_runway'),
                flights.get('departure', {}).get('actual_runway'),
                codeshared.get('airline_name') if codeshared else None,
                codeshared.get('airline_iata') if codeshared else None,
                codeshared.get('airline_icao') if codeshared else None,
                codeshared.get('flight_number') if codeshared else None,
                codeshared.get('flight_iata') if codeshared else None,
                codeshared.get('flight_icao') if codeshared else None
            ))
        return transformed_flights_data

    def load_data(self, transformed_flights_data, table_name):
        # truncate_query = f"TRUNCATE TABLE {table_name}"
        insert_query = f"""INSERT INTO {table_name} (
        flight_date, flight_status, flight_number,flight_iata,flight_icao,
         arrival_airport, arrival_timezone, arrival_iata, arrival_icao, 
        arrival_terminal, arrival_gate, arrival_baggage, arrival_delay_mins,
        scheduled_arrival_time, estimated_arrival_time, actual_arrival_time, arrival_estimated_runway,
        arrival_actual_runway, departure_airport, departure_timezone, departure_iata, departure_icao,
        departure_terminal, departure_gate, departure_delay_mins, scheduled_departure_time,
        estimated_departure_time, actual_departure_time, departure_estimated_runway, departure_actual_runway,
        codeshare_airline_name, codeshare_airline_iata, codeshare_airline_icao, codeshare_flight_number, 
        codeshare_flight_iata, codeshare_flight_icao
        ) 
        VALUES (%s,  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        # self.db_handler.execute_query(truncate_query)

        #  for batch processing :)
        batch_size = 1000
        try:
            for i in range(0, len(transformed_flights_data), batch_size):
                batch = transformed_flights_data[i:i + batch_size]
                self.db_handler.execute_many(insert_query, batch)
                print(f"Inserted batch {i // batch_size + 1}: {len(batch)} records")
        except Exception as err:
            print(f"Error loading data into {table_name} : {err}")

        # try:
        #     self.db_handler.execute_many(upsert_query, transformed_flights_data)
        #     print(f"Inserted {len(transformed_flights_data)} records into {table_name}")
        #
        # except Exception as err:
        #     print(f"Error loading data into {table_name}: {err}")

    def run(self, table_name, start_date, end_date):
        """Runs the ETL script"""
        raw_data = self.get_historical_flight_data(start_date, end_date)
        transformed_flights_data = self.transform_flight(raw_data)
        self.load_data(transformed_flights_data, table_name)
        print(f"ETL process completed for date range: {start_date} to {end_date}")


# initialise the database and run the ETL
db_handler = SQLDatabaseHandler('staging')
db_handler.connect_to_db()

api_key = os.getenv('API_KEY')
start_date = datetime(2024, 12, 18)
end_date = datetime(2024, 12, 26)
airline_etl = FlightETL(db_handler, api_key)
airline_etl.run("flight_details_staging", start_date, end_date)
db_handler.close_db_connection()
