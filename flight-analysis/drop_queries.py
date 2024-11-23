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

    foreign_keys_to_drop = [
        "ALTER TABLE flight_details DROP FOREIGN KEY fk_airline_id;",

        # Drop constraints from departure_info table
        "ALTER TABLE departure_info DROP FOREIGN KEY fk_origin_airport_id;",
        "ALTER TABLE departure_info DROP FOREIGN KEY fk_airline_id;",

        # Drop constraints from arrival_info table
        "ALTER TABLE arrival_info DROP FOREIGN KEY fk_arrival_airport_id;",
        "ALTER TABLE arrival_info DROP FOREIGN KEY fk_airline_id;",

        # Drop constraints from airline_routes table
         "ALTER TABLE airline_routes DROP FOREIGN KEY fk_airline_id;",
         "ALTER TABLE airline_routes DROP FOREIGN KEY fk_origin_airport_id;",
         "ALTER TABLE airline_routes DROP FOREIGN KEY fk_destination_airport_id;",

            # Drop constraints from cities table
        "ALTER TABLE cities DROP FOREIGN KEY fk_country_iso2;"
]

    for foreign_key in foreign_keys_to_drop:
        cursor.execute(foreign_key)
        print(f"Dropped foreign key: {foreign_key}")

    conn.commit()

except mysql.connector.Error as err:
    print(f"Failed dropping foreign keys: {err}")

finally:
    if cursor:
        cursor.close()
        print("MySQL connection is  closed")


