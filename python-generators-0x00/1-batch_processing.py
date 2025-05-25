import mysql.connector

def stream_users_in_batches(batch_size):
    """Generator that yields users in batches of given size."""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",        # Update if needed
            password="",        # Update if needed
            database="ALX_prodev"
        )
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT * FROM user_data")

        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            yield batch

        cursor.close()
        connection.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")


def batch_processing(batch_size):
    """Yields users over the age of 25 from each batch."""
    for batch in stream_users_in_batches(batch_size):
        for user in batch:
            if user["age"] > 25:
                yield user
