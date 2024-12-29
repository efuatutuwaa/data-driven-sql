from sql_handler.sql_database_handler import SQLDatabaseHandler


class ETLArrival:
    def __init__(self, db_handler):
        self.db_handler = db_handler

    def load_arrival_staging(self, source_table, destination_table):
        """Truncates and loads data into the arrival_staging table."""
        try:
            # Step 1 : truncate the destination table
            self.db_handler.truncate_table(destination_table)
            print(f"Truncated table {destination_table}")

            # Step 2: Insert data from the source table into the destination table
            insert_query = f""" 
            INSERT INTO {destination_table} (
            arrival_airport, arrival_timezone, arrival_iata, 
            arrival_icao, arrival_terminal, arrival_gate, arrival_baggage, 
            arrival_delay_mins, scheduled_arrival_time, estimated_arrival_time, actual_arrival_time, 
            arrival_estimated_runway, arrival_actual_runway
            )
            SELECT 
            arrival_airport, arrival_timezone, arrival_iata, 
            arrival_icao, arrival_terminal, arrival_gate, arrival_baggage, 
            arrival_delay_mins, scheduled_arrival_time, estimated_arrival_time, actual_arrival_time, 
            arrival_estimated_runway, arrival_actual_runway
            FROM {source_table};
            """
            self.db_handler.execute_many(insert_query)
            print(f"Inserted data into {destination_table} from {source_table}")
        except Exception as err:
            print(f"Error loading data into {destination_table}: {err}")


#    connect to the database
db_handler = SQLDatabaseHandler("staging")
db_handler.connect_to_db()

# initialise the ETL process
arrival_etl = ETLArrival(db_handler)

# load data from flight_details_staging to arrival_staging
arrival_etl.load_arrival_staging("flight_details_staging", "arrival_staging")
db_handler.close_db_connection()

