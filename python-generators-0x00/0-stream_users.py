import mysql.connector

def stream_users():
    """Generator that yields one user row at a time from the user_data table."""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",        # Update if necessary
            password="",        # Update if necessary
            database="ALX_prodev"
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM user_data")

        for row in cursor:
            yield row

        cursor.close()
        connection.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
