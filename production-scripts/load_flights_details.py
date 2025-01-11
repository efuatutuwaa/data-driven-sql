# importing libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os


class LoadFlightsDetails:
    def __init__(self, staging_handler, production_handler):
        self.staging_handler = staging_handler
        self.production_handler = production_handler

    def load_flights_details(self, staging_table, production_table, airlines_table, airport_table):
        """Loads data from flights staging table to production table"""
        # 1. create a temp table
        create_query = f"""
        CREATE TEMPORARY TABLE temp_flights AS 
            SELECT 
                a.airline_id, 
                f.flight_date, 
                f.flight_status,
                f.flight_number, 
                COALESCE(f.flight_iata, 'not_available')  AS flight_iata,
                COALESCE(f.flight_icao, 'not_available')  AS flight_icao,
                dep.airport_id AS departure_airport_id,
                arr.airport_id AS arrival_airport_id,
                COALESCE(f.arrival_icao, 'not_available')  AS arrival_icao,
                COALESCE(f.arrival_iata, 'not_available')  AS arrival_iata,
                COALESCE(f.departure_icao, 'not_available')  AS departure_icao,
                COALESCE(f.departure_iata, 'not_available')  AS departure_iata,
                COALESCE(f.arrival_delay_mins, TIMESTAMPDIFF(MINUTE, f.scheduled_arrival_time, f.actual_arrival_time )) AS arrival_delay,
                COALESCE(f.departure_delay_mins, TIMESTAMPDIFF(MINUTE , f.scheduled_departure_time, f.actual_departure_time )) AS dep_delay_mins,
                f.arrival_airport,  
                f.arrival_timezone, 
                f.arrival_terminal, 
                f.arrival_gate, 
                f.arrival_baggage, 
                f.scheduled_arrival_time, 
                f.estimated_arrival_time, 
                f.arrival_estimated_runway,
                f.arrival_actual_runway, 
                f.departure_airport, 
                f.departure_timezone, 
                f.departure_terminal, 
                f.departure_gate, 
                f.scheduled_departure_time, 
                f.estimated_departure_time, 
                f.departure_estimated_runway,
                f.departure_actual_runway,
                f.actual_arrival_time,
                f.actual_departure_time,
                f.airline_iata, 
                f.airline_icao,
                f.codeshare_airline_name, 
                f.codeshare_airline_iata, 
                f.codeshare_airline_icao,
                f.codeshare_flight_number, 
                f.codeshare_flight_iata, 
                f.codeshare_flight_icao
            FROM {self.staging_handler.db_config['database']}.{staging_table} f
            INNER JOIN {self.production_handler.db_config['database']}.{airport_table} dep
                ON (f.departure_iata = dep.airport_iata OR f.departure_icao = dep.airport_icao) 
            INNER JOIN {self.production_handler.db_config['database']}.{airport_table} arr 
                ON (f.arrival_iata = arr.airport_iata OR f.arrival_icao = arr.airport_icao) 
            INNER JOIN {self.production_handler.db_config['database']}.{airlines_table} a
                ON f.airline_icao = a.airline_icao
            WHERE f.flight_number IS NOT NULL 
                AND f.arrival_airport IS NOT NULL 
                AND f.departure_airport IS NOT NULL 
                AND f.flight_status IN ('landed', 'cancelled');"""

        # 2. extract data from temporary table
        fetch_query = f"""
        SELECT 
            airline_id, 
            flight_date, 
            flight_status, 
            flight_number, 
            flight_iata,
            flight_icao,
            departure_airport_id, 
            arrival_airport_id,
            arrival_icao, 
            arrival_iata, 
            departure_icao, 
            departure_iata, 
            arrival_delay, 
            dep_delay_mins, 
            arrival_airport, 
            arrival_timezone, 
            arrival_terminal, 
            arrival_gate, 
            arrival_baggage, 
            scheduled_arrival_time, 
            estimated_arrival_time, 
            arrival_estimated_runway, 
            arrival_actual_runway, 
            departure_airport, 
            departure_timezone, 
            departure_terminal, 
            departure_gate, 
            scheduled_departure_time, 
            estimated_departure_time, 
            departure_estimated_runway, 
            departure_actual_runway,
            actual_arrival_time, 
            actual_departure_time,
            airline_iata, 
            airline_icao,
            codeshare_airline_name, 
            codeshare_airline_iata, 
            codeshare_airline_icao,
            codeshare_flight_number, 
            codeshare_flight_iata, 
            codeshare_flight_icao
        FROM temp_flights;"""

        # 3. Insert data from temporary table to production table
        insert_query = f"""
        INSERT INTO {production_table} (
            airline_id, 
            flight_date, 
            flight_status, 
            flight_number, 
            flight_iata, 
            flight_icao,
            departure_airport_id, 
            arrival_airport_id,
            arrival_icao, 
            arrival_iata, 
            departure_icao, 
            departure_iata, 
            arrival_delay_mins, 
            departure_delay_mins, 
            arrival_airport, 
            arrival_timezone, 
            arrival_terminal, 
            arrival_gate, 
            arrival_baggage, 
            scheduled_arrival_time, 
            estimated_arrival_time, 
            arrival_estimated_runway, 
            arrival_actual_runway, 
            departure_airport, 
            departure_timezone, 
            departure_terminal, 
            departure_gate, 
            scheduled_departure_time, 
            estimated_departure_time, 
            departure_estimated_runway, 
            departure_actual_runway,
            actual_arrival_time, 
            actual_departure_time,
            airline_iata,
            airline_icao,
            codeshare_airline_name, 
            codeshare_airline_iata, 
            codeshare_airline_icao,
            codeshare_flight_number, 
            codeshare_flight_iata, 
            codeshare_flight_icao
        ) VALUES (%s, %s, %s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, %s,%s, %s,%s, %s, %s, %s, %s, %s,%s, %s,%s, %s, %s, 
        %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s,%s,%s);"""

        try:
            # Begin a transaction
            self.production_handler.begin_transaction()

            # Create the temporary table
            self.staging_handler.execute_query(create_query)

            # Fetch data from the temporary table
            staging_data = self.staging_handler.fetch_query(fetch_query)
            if not staging_data:
                print(f"Error fetching data from temp_flights table")
                self.production_handler.rollback_transaction()
                return

            # Batch insert data into the production table
            batch_size = 1000  # Define batch size
            for i in range(0, len(staging_data), batch_size):
                batch = staging_data[i:i + batch_size]  # Get a batch of data
                self.production_handler.execute_many(insert_query, batch)
                print(f"Batch {i // batch_size + 1}: Inserted {len(batch)} rows into {production_table}")

            # Commit the transaction
            self.production_handler.commit_transaction()
            print(f"Data successfully inserted into {production_table}")

        except Exception as err:
            # Rollback the transaction in case of an error
            self.production_handler.rollback_transaction()
            print(f"Error populating data from temp_flights table to {production_table}: {err}")
            raise


# connect to staging database
staging_handler = SQLDatabaseHandler('staging')
staging_handler.connect_to_db()

# connect to production database
production_handler = SQLDatabaseHandler('production')
production_handler.connect_to_db()

# run production
flight_production = LoadFlightsDetails(staging_handler, production_handler)
flight_production.load_flights_details('flight_details_staging', 'flight_details', 'airline', 'airports')
