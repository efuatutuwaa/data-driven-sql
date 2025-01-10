# importing libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os


class LoadAirports:
    def __init__(self, staging_handler, production_handler):
        self.staging_handler = staging_handler
        self.production_handler = production_handler


    def load_airports(self, staging_table, production_table, cities_table, countries_table):
        """loads airports data from airports_staging table to airports_production table"""
        fetch_query = f"""
        SELECT 
            s.airport_name, 
            s.airport_iata, 
            s.airport_icao, 
            s.latitude, 
            s.longitude, 
            s.geoname_id, 
            s.timezone_name, 
            s.gmt_offset,
            s.country_iso2, 
            s.city_iata_code, 
            s.api_airport_id, 
            ct.city_id
        FROM {self.staging_handler.db_config['database']}.{staging_table} s
        JOIN {self.production_handler.db_config['database']}.{cities_table} ct 
            ON s.city_iata_code = ct.iata_code
        JOIN {self.production_handler.db_config['database']}.{countries_table} c
            ON s.country_iso2 = c.country_iso2
        WHERE s.city_iata_code != 'Unknown' 
            AND c.country_iso2 != 'Unknown'
        """

        upsert_query = f"""
        INSERT INTO {production_table} (
            airport_name, 
            airport_iata, 
            airport_icao, 
            latitude, 
            longitude, 
            geoname_id, 
            timezone_name, 
            gmt_offset, 
            country_iso2, 
            city_iata_code, 
            api_airport_id,
            airport_city_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            airport_name = VALUES(airport_name),
            airport_iata = VALUES(airport_iata),
            airport_icao = VALUES(airport_icao),
            latitude = VALUES(latitude),
            longitude = VALUES(longitude),
            geoname_id = VALUES(geoname_id),
            timezone_name = VALUES(timezone_name),
            gmt_offset = VALUES(gmt_offset),
            country_iso2 = VALUES(country_iso2),
            city_iata_code = VALUES(city_iata_code),
            api_airport_id = VALUES(api_airport_id),
            airport_city_id = VALUES(airport_city_id)
        """

        try:
            # fetching data from staging_table to production_table
            staging_data = self.staging_handler.fetch_query(fetch_query)
            if not staging_data:
                print(f"No data found in {staging_table}")
                return
            self.production_handler.upsert_data(upsert_query, staging_data)
            print(f"Data upserted into {production_table}")
        except Exception as e:
            print(f"Error inserting data into {production_table}: {e}")


staging_handler = SQLDatabaseHandler('staging')
staging_handler.connect_to_db()

production_handler = SQLDatabaseHandler('production')
production_handler.connect_to_db()

# run production
airports_production = LoadAirports(staging_handler, production_handler)
airports_production.load_airports('airport_staging', 'airports', 'cities', 'countries')

# close database connection
staging_handler.close_db_connection()
production_handler.close_db_connection()



