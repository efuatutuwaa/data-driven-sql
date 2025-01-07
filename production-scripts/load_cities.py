# importing libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os


class LoadCities:
    def __init__(self, staging_handler,  production_handler):
        self.staging_handler = staging_handler
        self.production_handler = production_handler


    def load_cities(self, staging_table, production_table, countries_table):
        """loads cities data from cities_staging table to cities_production table"""
        fetch_query = f"""SELECT 
        c.city_name,
        c.iata_code,
        c.country_iso2,
        c.latitude,
        c.longitude,
        c.timezone_name,
        c.gmt,
        c.geoname_id,
        c.api_city_id
        FROM {self.staging_handler.db_config['database']}.{staging_table} c
        INNER JOIN {self.production_handler.db_config['database']}.{countries_table} cc
            ON c.country_iso2 = cc.country_iso2
        WHERE c.iata_code IS NOT NULL
        """
        upsert_query = f"""
        INSERT INTO {production_table} (
        city_name, iata_code, country_iso2, latitude, longitude, timezone, gmt_offset,
        geoname_id, api_city_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        api_city_id = VALUES(api_city_id),
        city_name= VALUES(city_name),
        iata_code= VALUES(iata_code),
        country_iso2= VALUES(country_iso2), 
        latitude= VALUES(latitude), 
        longitude= VALUES(longitude), 
        timezone_name= VALUES(timezone), 
        gmt_offset= VALUES(gmt_offset), 
        geoname_id= VALUES(geoname_id);
        """

        try:
            # fetching data from staging table to production table
            staging_data = self.staging_handler.fetch_query(fetch_query)
            if not staging_data:
                print(f"No data found in {staging_table}")
                return

                # loading data into the production table
            self.production_handler.upsert_data(upsert_query, staging_data)
            print(f"Data upserted into {production_table}")
        except Exception as e:
            print(f"Error loading data into {production_table}: {e}")


# connect to the database : staging, error and production databases
staging_handler = SQLDatabaseHandler('staging')
staging_handler.connect_to_db()

production_handler = SQLDatabaseHandler('production')
production_handler.connect_to_db()

# run the production process
cities_production = LoadCities(staging_handler, production_handler)
cities_production.load_cities('cities_staging', 'cities', 'countries')

# close database connection
staging_handler.close_db_connection()
production_handler.close_db_connection()




