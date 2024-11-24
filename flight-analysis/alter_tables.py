import mysql.connector

config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Efua1234',
    'database': 'flights'
}

# creating connection
try:
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    print("Connected to MySQL database")

#   list of foreign key constraints to be added

    foreign_key_constraints = [

        # flight_table
        """
        ALTER TABLE flight_details
        ADD CONSTRAINT fk_codeshared_id_new
        FOREIGN KEY (codeshared_id) REFERENCES codeshare (codeshare_id) ON DELETE CASCADE,
        ADD CONSTRAINT fk_airline_id_new
        FOREIGN KEY (airline_id) REFERENCES airline (airline_id) ON DELETE CASCADE
        ;""",

        """
        ALTER TABLE departure_info
        ADD CONSTRAINT fk_origin_airport_id
        FOREIGN KEY (origin_airport_id) REFERENCES airports (airport_id) ON DELETE CASCADE,
        ADD CONSTRAINT fk_airline_id_departure
        FOREIGN KEY (airline_id) REFERENCES airline (airline_id) ON DELETE CASCADE
        ;
        """,

        """
        ALTER TABLE arrival_info
        ADD CONSTRAINT fk_arrival_airport_id
        FOREIGN KEY (arrival_airport_id) REFERENCES airports (airport_id) ON DELETE CASCADE,
        ADD CONSTRAINT fk_airline_id_arrival
        FOREIGN KEY (airline_id) REFERENCES airline (airline_id) ON DELETE CASCADE
        ;
        """,

        """
        ALTER TABLE airline_routes
        ADD CONSTRAINT fk_airline_id_routes_new
        FOREIGN KEY (airline_id) REFERENCES airline (airline_id) ON DELETE CASCADE,
        ADD CONSTRAINT fk_origin_airport_id_routes
        FOREIGN KEY (origin_airport_id) REFERENCES airports (airport_id) ON DELETE CASCADE,
        ADD CONSTRAINT fk_destination_airport_id_routes
        FOREIGN KEY (destination_airport_id) REFERENCES airports (airport_id) ON DELETE CASCADE
        ;""",

        """
        ALTER TABLE cities
        ADD CONSTRAINT fk_country_iso2
        FOREIGN KEY (country_iso2) REFERENCES countries (country_iso2) ON DELETE CASCADE
       ;""",


    ]

    table_names = [
        # "flight_details",
        # "departure_info",
        # "arrival_info",
        # "airline_routes",
        "cities"
    ]

    for i, constraint in enumerate(foreign_key_constraints):
        cursor.execute(constraint)
        print(f"Foreign key constraint added to {table_names[i]}")
    conn.commit()

except mysql.connector.Error as err:
    print(f"Failed to add foreign key constraints: {err}")

finally:
    if cursor:
        cursor.close()
        print("MySQL connection is closed")


