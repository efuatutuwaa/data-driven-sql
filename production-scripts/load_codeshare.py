# importing libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os


class LoadCodeShare:

    def __init__(self, production_handler):
        self.production_handler = production_handler

    def load_codeshare(self, flights_detail, airline_table, codeshare_table):
        """ Loads codeshare table from staging to production """

        fetch_query = f"""
            SELECT
                f.flight_id,
                a.airline_id,
                f.codeshare_flight_number,
                f.codeshare_airline_name,
                f.codeshare_airline_iata,
                f.codeshare_airline_icao,
                f.codeshare_flight_iata,
                f.codeshare_flight_icao
            FROM {self.production_handler.db_config['database']}.{flights_detail} f
            INNER JOIN {self.production_handler.db_config['database']}.{airline_table} a 
                ON f.codeshare_airline_icao = a.airline_icao 
            WHERE f.codeshare_airline_icao IS NOT NULL
                AND f.codeshare_airline_iata IS NOT NULL;"""

        insert_query = f"""
            INSERT INTO {codeshare_table} ( 
                flight_id, 
                airline_id, 
                flight_number, 
                airline_name, 
                airline_iata,
                airline_icao,
                flight_iata, 
                flight_icao
             ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"""

        try:
            staging_data = self.production_handler.fetch_query(fetch_query)
            if not staging_data:
                print(f"No data found for production")
                return

            batch_size = 1000
            for i in range(0, len(staging_data), batch_size):
                batch_data = staging_data[i:i + batch_size]
                self.production_handler.execute_many(insert_query, batch_data)
                print(f"Batch {i // batch_size + 1}: Inserted {len(batch_data)} rows into {codeshare_table}")

        except Exception as err:
            print(f"Error inserting production into {codeshare_table} {err}")


# connect to the database
production_handler = SQLDatabaseHandler('production')
production_handler.connect_to_db()

# run production
load_codeshare = LoadCodeShare(production_handler)
load_codeshare.load_codeshare('flight_details', 'airline', 'codeshare')

# close connection to database
production_handler.close_db_connection()
