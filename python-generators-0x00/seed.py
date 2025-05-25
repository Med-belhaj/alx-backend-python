import mysql.connector
import uuid
import csv
import os

DB_NAME = "ALX_prodev"
TABLE_NAME = "user_data"

def connect_db():
    """Connects to the MySQL server (not a specific database)."""
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",       # Change this if your user is different
            password=""        # Set your password here
        )
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


def create_database(connection):
    """Creates the ALX_prodev database if it doesn't exist."""
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME};")
        connection.commit()
        print("Database created or already exists")
        cursor.close()
    except mysql.connector.Error as err:
        print(f"Error creating database: {err}")


def connect_to_prodev():
    """Connects to the ALX_prodev database."""
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",       # Change this if needed
            password="",       # Set your password
            database=DB_NAME
        )
    except mysql.connector.Error as err:
        print(f"Error connecting to {DB_NAME}: {err}")
        return None


def create_table(connection):
    """Creates the user_data table if it does not exist."""
    try:
        cursor = connection.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                user_id VARCHAR(36) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                age DECIMAL NOT NULL,
                INDEX(user_id)
            );
        """)
        connection.commit()
        cursor.close()
        print("Table user_data created successfully")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")


def insert_data(connection, file_path):
    """Inserts data from CSV into user_data table (avoiding duplicates)."""
    try:
        cursor = connection.cursor()
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Check if the UUID already exists
                cursor.execute(f"SELECT 1 FROM {TABLE_NAME} WHERE user_id = %s", (row["user_id"],))
                if not cursor.fetchone():
                    cursor.execute(f"""
                        INSERT INTO {TABLE_NAME} (user_id, name, email, age)
                        VALUES (%s, %s, %s, %s);
                    """, (row["user_id"], row["name"], row["email"], row["age"]))
        connection.commit()
        cursor.close()
        print("Data inserted successfully")
    except FileNotFoundError:
        print(f"CSV file not found: {file_path}")
    except mysql.connector.Error as err:
        print(f"Database error during insert: {err}")
