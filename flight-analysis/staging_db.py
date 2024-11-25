import os
import mysql.connector

# environment variables
DB_PASSWORD = os.getenv('DB_PASSWORD')

# database configurations
config = {
    'host': 'localhost',
    'user': 'root',
    'password': DB_PASSWORD,
    'database': 'staging_flights',
}

# creating staging database to for ETL processes
def create_staging_db(conn, create_staging_tbl,staging_tables_names):
    try:
        cursor = conn.cursor()
        for index, query in enumerate(create_staging_tbl):
            cursor.execute(query)
            print(f"Table {staging_tables_names[index]} created successfully")
        conn.commit()

    except mysql.connector.Error as err:
        print(f"Failed creating table {staging_tables_names[index]}")

    finally:
        if cursor:
            cursor.close()


#  connecting to MySQL to create databases and tables

try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print(f"Connected to MySQL database")


# create database
    staging_db_name = 'staging_flights'
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {staging_db_name}")
    cursor.execute(f"USE {staging_db_name}")


# create staging tables

    create_staging_tbl = [
        # 1. airline_staging
        """
        CREATE TABLE IF NOT EXISTS airline_staging (
        airline_name VARCHAR(255),
        airline_iata VARCHAR(50),
        airline_icao VARCHAR(50),
        airline_type VARCHAR(50),
        airline_status VARCHAR(50),
        fleet_size INT,
        fleet_average_size DECIMAL(5,2),
        date_founded DATE,
        hub_code VARCHAR(50) ,
        country_name VARCHAR(50) 
        );""",

        # 2. airline_routes
        """
        CREATE TABLE IF NOT EXISTS airline_route_staging (
        departure_airport VARCHAR(255),
        departure_timezone VARCHAR(255),
        departure_iata VARCHAR(50),
        departure_icao VARCHAR(50),
        departure_terminal VARCHAR(50),
        scheduled_departure_time TIME,
        arrival_airport VARCHAR(255),
        arrival_timezone VARCHAR(255),
        arrival_iata VARCHAR(50),
        arrival_icao VARCHAR(50),
        arrival_terminal VARCHAR(50),
        scheduled_arrival_time TIME
         );""",
        
        # 3. airport_staging table
        """
        CREATE TABLE IF NOT EXISTS airport_staging (
        airport_name VARCHAR(255),
        airport_iata VARCHAR(50),
        airport_icao VARCHAR(50),
        latitude DECIMAL(9,6),
        longitude DECIMAL(9,6),
        geoname_id VARCHAR(50),
        timezone_name VARCHAR(50),
        gmt_offset INT
        );""",

        # 4. arrival_info_staging table

        """
        CREATE TABLE IF NOT EXISTS arrival_staging (
        arrival_airport VARCHAR(255),
        arrival_timezone VARCHAR(255),
        arrival_iata VARCHAR(50),
        arrival_icao VARCHAR(50),
        arrival_terminal VARCHAR(50),
        arrival_gateway VARCHAR(50),
        arrival_baggage VARCHAR(50),
        arrival_delay INT,
        scheduled_arrival_time DATETIME,
        estimated_arrival_time DATETIME,
        actual_arrival_time DATETIME,
        arrival_estimated_runway DATETIME,
        arrival_actual_runway DATETIME
         );""",

        # 5. cities staging table
        """
        CREATE TABLE IF NOT EXISTS cities_staging (
        city_name VARCHAR(255),
        iata_code VARCHAR(50),
        country_iso2 CHAR(2),
        latitude DECIMAL(9,6),
        longitude DECIMAL(9,6),
        timezone_name VARCHAR(50),
        gmt INT,
        geoname_id VARCHAR(50)
        );""",


        # 6. codeshare staging table
        """
        CREATE TABLE IF NOT EXISTS codeshare_staging (
        codeshare_airline_name VARCHAR(255), 
        codeshare_airline_iata VARCHAR(50),
        codeshare_airline_icao VARCHAR(50),
        codeshare_flight_number VARCHAR(50),
        codeshare_flight_iata VARCHAR(50),
        codeshare_flight_icao VARCHAR(50)
        );""",


        #  7. countries staging tables

        """
        CREATE TABLE IF NOT EXISTS countries_staging (
        country_name VARCHAR(255),
        country_iso2 CHAR(2),
        country_iso3 CHAR(3),
        country_iso_numeric VARCHAR(5),
        population BIGINT,
        capital VARCHAR(255),
        continent VARCHAR(50),
        currency_name VARCHAR(50),
        currency_code VARCHAR(50),
        fips_code VARCHAR(10),
        phone_prefix VARCHAR(10)
        );""",

        # 8. departure_info staging

        """
        CREATE TABLE IF NOT EXISTS departure_staging (
        departure_airport VARCHAR(255),
        departure_timezone VARCHAR(255),
        departure_iata VARCHAR(50),
        departure_icao VARCHAR(50),
        departure_terminal VARCHAR(50),
        departure_gate VARCHAR(20),
        departure_delay_mins INT,
        scheduled_departure_time DATETIME,
        estimated_departure_time DATETIME,
        actual_departure_time DATETIME, 
        departure_estimated_runway DATETIME,
        departure_actual_runway DATETIME
        );""",

        # 9.  flight details staging

        """
        CREATE TABLE IF NOT EXISTS flight_details_staging (
        flight_date DATE,
        flight_status ENUM ('scheduled', 'active', 'landed', 'cancelled', 'incident', 'diverted'),
        flight_number VARCHAR(50),
        flight_iata VARCHAR(50),
        flight_icao VARCHAR(50),
        codeshared VARCHAR(50)
        );"""


    ]

    #  table names to print during creation -- this will help track which staging tables has been created

    staging_tables_names = [
        "airline_staging",
        "airline_route_staging",
        "airport_staging",
        "arrival_staging",
        "cities_staging",
        "codeshare_staging",
        "countries_staging",
        "departure_staging",
        "flight_details_staging"
    ]

    create_staging_db(conn, create_staging_tbl, staging_tables_names)

except mysql.connector.Error as err:
    print(f"Failed to connect MySQL database: {err}")

finally:
    if conn.is_connected():
        conn.close()
        print("MySQL connection is closed")



