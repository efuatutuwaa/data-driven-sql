# importing libraries and packages
# importing libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os


class LoadFlightsDetails:
    def __init__(self, staging_handler, production_handler):
        self.staging_handler = staging_handler
        self.production_handler = production_handler

    def load_flights_details(self, staging_table, production_table, airlines_table):
        """Loads data from flights staging table to production table"""
        # 1. create a temp table
        create_query = f"""
        CREATE TEMPORARY TABLE temp_flights AS 
            SELECT 
                f.flight_date,
                f.flight_status,
                f.flight_number,
                f.flight_iata,
                f.flight_icao,
                a.airline_id
        FROM {self.staging_handler.db_config['database']}.{staging_table} as f
        INNER JOIN {self.production_handler.db_config['database']}.{airlines_table} as a
        ON f.airline_icao = a.airline_icao
        WHERE f.flight_number IS NOT NULL
        AND flight_status IN ('landed','cancelled')
        AND a.airline_icao != 'UNK'
        AND a.airline_iata != 'UNK'
        AND (a.airline_type = 'scheduled' OR a.airline_type LIKE 'scheduled,%')
        AND (f.flight_iata IS NOT NULL OR f.flight_icao IS NOT NULL);"""

        # 2. extract data from temporary table
        fetch_query = f"""
        SELECT 
            airline_id, 
            flight_date, 
            flight_status, 
            flight_number, 
            COALESCE(flight_iata, 'not_available') AS flight_iata,
            flight_icao
        FROM temp_flights;"""

        # 3. Insert data from temporary table to production table
        insert_query = f"""
        INSERT INTO {production_table} (
            airline_id, 
            flight_date, 
            flight_status, 
            flight_number, 
            flight_iata, 
            flight_icao
        ) VALUES (%s, %s, %s, %s, %s, %s);"""

        try:
            # Begin a transaction
            self.production_handler.begin_transaction()

            # Create the temporary table
            self.staging_handler.execute_query(create_query)

            # Fetch data from the temporary table
            staging_data = self.staging_handler.fetch_query(fetch_query)
            if not staging_data:
                print(f"Error fetching data from temp_flights table")
                self.production_handler.rollback()
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
flight_production.load_flights_details('flight_details_staging', 'flight_details', 'airline')
