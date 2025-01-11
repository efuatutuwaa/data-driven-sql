# importing libraries and packages
from sql_handler.sql_database_handler import SQLDatabaseHandler
import os


class LoadRoutes:
    def __init__(self, production_handler):
        self.production_handler = production_handler

    def load_routes(self, flights_table, airport_table, routes_table):
        """Loads routes date from staging to production """
        fetch_query = f"""
        SELECT 
        f.flight_id,
        dep.airport_id AS departure_airport_id,
        arr.airport_id AS arrival_airport_id,
        f.airline_id,
        f.flight_number,
        ROUND(6371 * 2 * ASIN(SQRT(POWER(SIN((RADIANS(arr.latitude) - RADIANS(dep.latitude)) / 2), 2) + 
        COS(RADIANS(dep.latitude)) * COS(RADIANS(arr.latitude)) * 
        POWER(SIN((RADIANS(arr.longitude) - RADIANS(dep.longitude)) / 2), 2))),2) AS route_distance,
        SEC_TO_TIME(TIMESTAMPDIFF(SECOND , f.scheduled_departure_time, f.scheduled_arrival_time)) AS route_duration,
        f.scheduled_departure_time,
        f.scheduled_arrival_time
        FROM {self.production_handler.db_config['database']}.{flights_table} f
        INNER JOIN {self.production_handler.db_config['database']}.{airport_table} dep
            ON f.departure_airport_id = dep.airport_id
        INNER JOIN {self.production_handler.db_config['database']}.{airport_table} arr 
            ON f.arrival_airport_id = arr.airport_id
        WHERE f.flight_status IN ('landed','cancelled');"""

        insert_query = f"""
        INSERT INTO {routes_table} (
                flight_id, 
                departure_airport_id, 
                arrival_airport_id, 
                airline_id, 
                flight_number, 
                distance, 
                duration, 
                scheduled_departure_time, 
                scheduled_arrival_time
        ) VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s);"""

        try:
            staging_data = self.production_handler.fetch_query(fetch_query)
            if not staging_data:
                print(f"No data found for production")
                return

            batch_size = 1000
            for i in range(0, len(staging_data), batch_size):
                batch_data = staging_data[i:i + batch_size]
                self.production_handler.execute_many(insert_query, batch_data)
                print(f"Batch {i // batch_size + 1}: Inserted {len(batch_data)} rows into {routes_table}")

        except Exception as err:
            print(f"Error loading data to production: {err}")


# connect to production database
production_handler = SQLDatabaseHandler('production')
production_handler.connect_to_db()

# run production process
routes_production = LoadRoutes(production_handler)
routes_production.load_routes('flight_details', 'airports', 'routes')

# close database connection
production_handler.close_db_connection()
