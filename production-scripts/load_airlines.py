# importing libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os


class LoadAirlines:
    def __init__(self, staging_handler, production_handler):
        self.staging_handler = staging_handler
        self.production_handler = production_handler

    def load_airlines(self, staging_table, production_table, countries_table):
        """Loads data from airline_staging table to airline_production_table."""
        fetch_query = f"""
        WITH airline_icao_duplicate AS (
            SELECT airline_name, 
            airline_iata, airline_icao, airline_type, 
            airline_status, fleet_size, fleet_average_age, 
            date_founded, hub_code, country_name, country_iso2,
            api_airline_id ,
            ROW_NUMBER() OVER (PARTITION BY airline_icao ORDER BY api_airline_id ) AS row_count
            FROM {self.staging_handler.db_config['database']}.{staging_table}
            WHERE airline_icao != 'UNK'
            AND airline_name IS NOT NULL
            AND country_name IS NOT NULL
            AND country_iso2 IS NOT NULL    
        )
        SELECT a.airline_name, 
        a.airline_iata, a.airline_icao, a.airline_type, 
        a.airline_status, a.fleet_size, a.fleet_average_age, 
        a.date_founded, a.hub_code, a.country_name, c.country_id, 
        a.country_iso2, a.api_airline_id
        FROM airline_icao_duplicate a
        INNER JOIN {self.production_handler.db_config['database']}.{countries_table} c 
        ON a.country_iso2 = c.country_iso2 
        WHERE a.row_count = 1;"""

        insert_query = f"""
        INSERT INTO {production_table} ( 
            airline_name, 
            airline_iata, airline_icao, airline_type, 
            airline_status, fleet_size, fleet_average_age, 
            date_founded, hub_code, country_name, country_id, country_iso2,
            api_airline_id
        ) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        airline_name = VALUES(airline_name),
        airline_iata = VALUES(airline_iata),
        airline_icao = VALUES(airline_icao),
        airline_type = VALUES(airline_type),
        airline_status = VALUES(airline_status),
        fleet_size = VALUES(fleet_size),
        fleet_average_age = VALUES(fleet_average_age),
        date_founded = VALUES(date_founded),
        hub_code = VALUES(hub_code),
        country_name = VALUES(country_name),
        country_id = VALUES(country_id),
        country_iso2 = VALUES(country_iso2) 
        """

        try:
            # fetching data from the staging table
            staging_data = self.staging_handler.fetch_query(fetch_query)
            if not staging_data:
                print(f"No data found in the {staging_table} ")
                return

            # loading data into the production table
            self.production_handler.upsert_data(insert_query, staging_data)
            print(f"Data upserted into the {production_table} ")

        except Exception as err:
            print(f"Error loading data into the {production_table} : {err}")


# connect to the databases : staging

staging_handler = SQLDatabaseHandler('staging')
staging_handler.connect_to_db()

# connect to the database: production
production_handler = SQLDatabaseHandler('production')
production_handler.connect_to_db()

# run the production
airlines_production = LoadAirlines(staging_handler, production_handler)
airlines_production.load_airlines('airline_staging', 'airline', 'countries')

# close database connection
staging_handler.close_db_connection()
production_handler.close_db_connection()
