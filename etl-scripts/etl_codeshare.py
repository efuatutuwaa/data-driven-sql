from sql_handler.sql_database_handler import SQLDatabaseHandler

class ETLCodeShare:
    def __init__(self, db_handler):
        self.db_handler = db_handler

    def load_codeshare_staging(self, source_table, destination_table):

        """Truncates and loads codeshare data from flights_details_stating to codeshare_staging"""
        try:
            # Step 1: Truncate destination table
            self.db_handler.truncate_table(destination_table)
            print(f"Truncated {destination_table}")

            # Step 2 : Insert data from source table to destination table

            insert_query = f"""INSERT INTO {destination_table} (
            codeshare_airline_name,codeshare_airline_iata, codeshare_airline_icao,
            codeshare_flight_number, codeshare_flight_iata, codeshare_flight_icao
            )
            SELECT codeshare_airline_name,codeshare_airline_iata, codeshare_airline_icao,
            codeshare_flight_number, codeshare_flight_iata, codeshare_flight_icao
            FROM {source_table};
            """
            self.db_handler.execute_many(insert_query)
            print(f"Inserted data from {source_table} to {destination_table}")

        except Exception as err:
            print(f"Inserted data into {destination_table} failed: {err}")


#  connect to the database
db_handler = SQLDatabaseHandler("staging")
db_handler.connect_to_db()

# initialise ETL process
codeshare_etl = ETLCodeShare(db_handler)

# load data from flight_details_staging to codeshare_staging
codeshare_etl.load_codeshare_staging("flight_details_staging", "codeshare_staging")

# close database connection
db_handler.close_db_connection()

