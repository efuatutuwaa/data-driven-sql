# importing libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os

class LoadCountries:
    def __init__(self, staging_handler, production_handler):
        self.staging_handler = staging_handler
        self.production_handler = production_handler

    def load_countries(self, staging_table, production_table):
        """Loads countries data from the country_staging table"""
        fetch_query = f"""
        SELECT country_name, country_iso2, country_iso3, country_iso_numeric,  
        population, capital, continent, currency_name, currency_code, fips_code, 
        phone_prefix, api_country_id 
        FROM {staging_table}
        """

        upsert_query = f"""
        INSERT INTO {production_table} (
        country_name, country_iso2, country_iso3, country_iso_numeric,  
        population, capital, continent, currency_name, currency_code, fips_code, 
        phone_prefix, api_country_id
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        
        ON DUPLICATE KEY UPDATE 
        api_country_id = VALUES(api_country_id),
        country_name=VALUES(country_name),
        capital=VALUES(capital),
        population=VALUES(population),
        continent=VALUES(continent),
        currency_name=VALUES(currency_name),
        currency_code=VALUES(currency_code),
        fips_code=VALUES(fips_code),
        phone_prefix=VALUES(phone_prefix);
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


# connect to the database : staging and production
staging_handler = SQLDatabaseHandler('staging')
staging_handler.connect_to_db()

production_handler = SQLDatabaseHandler('production')
production_handler.connect_to_db()


# run the production process
countries_production = LoadCountries(staging_handler, production_handler)
countries_production.load_countries('countries_staging', 'countries')

# close database connection
staging_handler.close_db_connection()
production_handler.close_db_connection()
