from sql_handler.sql_database_handler import SQLDatabaseHandler
import os


class LoadDeparture:

    def __init__(self, production_handler):
        self.production_handler = production_handler

    def load_arrivals(self, flight_table, departure_table):
        """Loads data from staging to production database"""
        fetch_query = f"""SELECT 
        flight_id,
        departure_airport_id,
        flight_date,
        flight_number,
        flight_status,
        flight_iata,
        flight_icao,
        departure_iata,
        departure_icao,
        departure_airport,
        departure_timezone,
        departure_terminal,
        departure_gate,
        departure_delay_mins,
        scheduled_departure_time,
        estimated_departure_time,
        actual_departure_time,
        departure_estimated_runway,
        departure_actual_runway
        FROM {self.production_handler.db_config['database']}.{flight_table}; """

        insert_query = f"""
        INSERT INTO 
        {departure_table} (flight_id,
                           arrival_airport_id,
                           flight_date,
                           flight_number,
                           flight_status,
                           flight_iata,
                           flight_icao,
                           airport_iata,
                           airport_icao,
                           airport_name,
                           timezone,
                           terminal,
                           gate,
                           delay,
                           scheduled,
                           estimated,
                           actual,
                           estimated_runway,
                           actual_runway)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        try:
            staging_data = self.production_handler.fetch_query(fetch_query)
            if not staging_data:
                print(f"No data found in {flight_table}")
                return

            batch_size = 1000
            for i in range(0, len(staging_data), batch_size):
                batch = staging_data[i:i + batch_size]
                self.production_handler.execute_many(insert_query, batch)
                print(f"Batch {i // batch_size + 1}: Inserted {len(batch)} rows into {arrivals_table}")

        except Exception as err:
            print(f"Error populating {departure_table}: {err}")


# connect to the production database
production_handler = SQLDatabaseHandler('production')
production_handler.connect_to_db()

arrivals_production = LoadDeparture(production_handler)
arrivals_production.load_arrivals('flight_details', 'departure')

# close database connection
production_handler.close_db_connection()

