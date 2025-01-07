from sql_handler.sql_database_handler import SQLDatabaseHandler
import os


class ETLDeparture:
    def __init__(self, db_handler):
        self.db_handler = db_handler

    def load_departure_staging(self, source_table, destination_table):
        """Truncates and loads data into the departure_staging table."""
        try:
            # Step 1 : truncate the destination table
            self.db_handler.truncate_table(destination_table)
            print(f"Truncated table {destination_table}")

            # Step 2: Insert data from the source table into the destination table
            insert_query = f""" 
                INSERT INTO {destination_table} (
                departure_airport, departure_timezone, departure_iata, 
                departure_icao, departure_terminal, departure_gate, departure_delay_mins, 
                scheduled_departure_time, estimated_departure_time, actual_departure_time, 
                departure_estimated_runway, departure_actual_runway
                )
                SELECT 
                departure_airport, departure_timezone, departure_iata, 
                departure_icao, departure_terminal, departure_gate, departure_delay_mins, 
                scheduled_departure_time, estimated_departure_time, actual_departure_time, 
                departure_estimated_runway, departure_actual_runway
                FROM {source_table};
                """
            self.db_handler.execute_query(insert_query)
            print(f"Inserted data into {destination_table} from {source_table}")
        except Exception as err:
            print(f"Error loading data into {destination_table}: {err}")

    #    connect to the database


db_handler = SQLDatabaseHandler("staging")
db_handler.connect_to_db()

# initialise the ETL process
arrival_etl = ETLDeparture(db_handler)

# load data from flight_details_staging to arrival_staging
arrival_etl.load_departure_staging("flight_details_staging", "departure_staging")

# close database connection
db_handler.close_db_connection()
