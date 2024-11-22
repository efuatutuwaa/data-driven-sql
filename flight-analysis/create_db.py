import mysql.connector

# Database connection configuration
config = {
    "host": "localhost",
    "user": "root",
    "password": "Efua1234",
}

# function to create tables in database

def create_tables_in_db(conn,create_tables,table_names):
    try:
        cursor = conn.cursor()
        for index, table_query in enumerate(create_tables):
            cursor.execute(table_query)
            print(f"Table '{table_names[index]}' created successfully")
        conn.commit()

    except mysql.connector.Error as err:
        print(f"Failed creating tables: {err}")

    finally:
        if cursor:
            cursor.close()

# connect to MySQL server to create tables
try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print("Connected to MySQL database")


# creating the database
    database_name = "flights"
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    print(f"Database  {database_name} created successfully")
    cursor.execute(f"USE {database_name}")

# creating tables

# 1. flight_table
# creating tables

    create_tables = [
        """
        CREATE TABLE IF NOT EXISTS flight_details (
        flight_id INT AUTO_INCREMENT NOT NULL,
        codeshared_id INT, -- foreign key to codeshared table
        airline_id INT NOT NULL,
        flight_date DATE NOT NULL,
        flight_status ENUM('scheduled', 'active', 'landed', 'cancelled', 'incident', 'diverted') NULL,
        flight_number VARCHAR(50) NOT NULL ,
        flight_iata VARCHAR(50) NOT NULL ,
        flight_icao VARCHAR(50) NOT NULL ,
        codeshared VARCHAR(50) NULL,
        PRIMARY KEY (flight_id)
         );""",

        """
        # 2. departure_info table 
        CREATE TABLE IF NOT EXISTS departure_info (
        departure_id INT AUTO_INCREMENT NOT NULL,
        flight_id INT NOT NULL,
        origin_airport_id INT NOT NULL, -- foreign key to airport_id
        airline_id INT NOT NULL, -- foreign key to airline table
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
        departure_actual_runway DATETIME,
        PRIMARY KEY (departure_id),
        FOREIGN KEY (flight_id) REFERENCES flight_details(flight_id)
        );""",

        """
         # 3. arrival_info table
        CREATE TABLE IF NOT EXISTS arrival_info (
        arrival_id INT AUTO_INCREMENT NOT NULL,
        flight_id INT NOT NULL,
        arrival_airport_id INT NOT NULL, -- foreign key to airport table
        airline_id INT NOT NULL, -- foreign key to airline table
        arrival_airport VARCHAR(255),
        arrival_timezone VARCHAR(255),
        arrival_iata VARCHAR(50),
        arrival_icao VARCHAR(50),
        arrival_terminal VARCHAR(50),
        arrival_gate VARCHAR(20),
        arrival_baggage VARCHAR(20),
        arrival_delay_mins INT,
        scheduled_arrival_time DATETIME,
        estimated_arrival_time DATETIME,
        actual_arrival_time DATETIME,
        arrival_estimated_runway DATETIME,
        arrival_actual_runway DATETIME,
        PRIMARY KEY (arrival_id),
        FOREIGN KEY (flight_id) REFERENCES flight_details(flight_id)
        );""",

        """
        # 4. airline table
        CREATE TABLE IF NOT EXISTS airline (
        airline_id INT AUTO_INCREMENT NOT NULL,
        airline_name VARCHAR(255),
        airline_iata VARCHAR(50),
        airline_icao VARCHAR(50),
        airline_type VARCHAR(50),
        airline_status VARCHAR(50),
        fleet_size INT,
        fleet_average_age DECIMAL(5,2),
        date_founded DATE NOT NULL,
        hub_code VARCHAR(50),
        country_name VARCHAR(255),
        PRIMARY KEY (airline_id)
        );""",

        """
        # 5. airport table
        CREATE TABLE IF NOT EXISTS airports (
        airport_id INT AUTO_INCREMENT NOT NULL,
        airport_name VARCHAR(255) NULL ,
        airport_iata VARCHAR(50) NULL ,
        airport_icao VARCHAR(50) NULL,
        latitude DECIMAL(9,6) NULL,
        longitude DECIMAL(9,6) NULL,
        geoname_id VARCHAR(50) NULL,
        timezone_name VARCHAR(50) NULL,
        gmt_offset INT  NULL,
        PRIMARY KEY (airport_id)
        );""",

        """
        # 6. airline_routes 
        CREATE TABLE IF NOT EXISTS airline_routes(
        route_id INT AUTO_INCREMENT NOT NULL,
        airline_id INT NULL, -- foreign key to airline table
        origin_airport_id INT NULL, -- foreign key to airport table
        destination_airport_id INT NULL, -- foreign key to airport table
        flight_number VARCHAR(50) NULL,
        scheduled_departure_time TIME, -- scheduled departure time 
        scheduled_arrival_time TIME, -- scheduled arrival time
        PRIMARY KEY (route_id),
        FOREIGN KEY (airline_id) REFERENCES airline (airline_id),
        FOREIGN KEY (origin_airport_id) REFERENCES airports (airport_id),
        FOREIGN KEY (destination_airport_id) REFERENCES airports (airport_id)
        );""",

        """
    # 7. live_data table
        CREATE TABLE IF NOT EXISTS live_info (
        live_id INT AUTO_INCREMENT NOT NULL,
        flight_id INT NULL, -- foreign key to flight table
        update_time TIMESTAMP NULL,
        latitude DECIMAL(9,6) NULL,
        longitude DECIMAL(9,6) NULL,
        altitude DECIMAL(9,6) NULL,
        direction DECIMAL(9,6) NULL,
        speed_horizontal DECIMAL(9,6) NULL,
        speed_vertical DECIMAL(9,6) NULL,
        is_ground BOOLEAN NULL,
        PRIMARY KEY (live_id),
        FOREIGN KEY (flight_id) REFERENCES flight_details(flight_id)
        ); """,

        """
    # 8. codeshare table 
        CREATE TABLE IF NOT EXISTS codeshare (
        codeshare_id INT AUTO_INCREMENT NOT NULL,
        flight_id INT NULL, -- foreign key to flight table
        codeshare_airline_name VARCHAR(255) NOT NULL,
        codeshare_airline_iata VARCHAR(50) NOT NULL,
        codeshare_airline_icao VARCHAR(50) NULL,
        codeshare_flight_number VARCHAR(50) NULL,
        codeshare_flight_iata VARCHAR(50) NULL,
        codeshare_flight_icao VARCHAR(50) NULL,
        PRIMARY KEY (codeshare_id),
        FOREIGN KEY (flight_id) REFERENCES flight_details(flight_id)
        ); """,

        """
        # 9. pagination_tracking -- to monitor API usage
        CREATE TABLE IF NOT EXISTS pagination_tracking (
        pagination_id INT AUTO_INCREMENT NOT NULL,
        api_name VARCHAR(100) NOT NULL,
        limits INT,
        offsets INT,
        counts INT,  
        totals INT,
        last_updated DATETIME NULL,
        PRIMARY KEY (pagination_id)
        );""",

        """
    # 10. countries table
        CREATE TABLE IF NOT EXISTS countries (
        country_id INT AUTO_INCREMENT NOT NULL,
        country_name VARCHAR(255) NOT NULL,
        country_iso2 CHAR(2) NOT NULL, 
        country_iso3 CHAR(3) NOT NULL,
        country_iso_numeric VARCHAR(5),
        population BIGINT,
        capital VARCHAR(255),
        continent VARCHAR(50),
        currency_name VARCHAR(50),
        currency_code VARCHAR(10),
        fips_code VARCHAR(10),
        phone_prefix VARCHAR(10),
        PRIMARY KEY (country_iso2)
        );""",

        """
        # 11. cities table
        CREATE TABLE IF NOT EXISTS cities (
        city_id INT AUTO_INCREMENT NOT NULL,
        city_name VARCHAR(255) NOT NULL,
        iata_code VARCHAR(3) NOT NULL,
        country_iso2 CHAR(2) NOT NULL, -- foreign key to countries table
        latitude DECIMAL(9,6),
        longitude DECIMAL(9,6),
        timezone VARCHAR(50),
        gmt_offset INT,
        geoname_id VARCHAR(50),
        PRIMARY KEY (city_id)
        );"""
]

    # names of table to print during creation -- this will help track which table has been created

    table_names = [
        "flight_details",
        "departure_info",
        "arrival_info",
        "airline",
        "airports",
        "airline_routes",
        "live_info",
        "codeshare",
        "pagination_tracking",
        "countries",
        "cities"
    ]

    create_tables_in_db(conn, create_tables,table_names)
except mysql.connector.Error as err:
    print(f"Failed to connect to MySQL database: {err}")

finally:
    if conn.is_connected():
        conn.close()
        print("MySQL connection is closed")
