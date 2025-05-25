import seed


def stream_user_ages():
    """
    Generator that yields user ages one by one from the user_data table.
    """
    connection = seed.connect_to_prodev()
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT age FROM user_data")
    row = cursor.fetchone()
    while row:
        yield float(row['age'])
        row = cursor.fetchone()
    cursor.close()
    connection.close()


def calculate_average_age():
    """
    Uses the stream_user_ages generator to compute and print the average age.
    """
    total = 0.0
    count = 0
    for age in stream_user_ages():  # Loop 2: aggregate ages
        total += age
        count += 1
    average = total / count if count else 0
    print(f"Average age of users: {average}")


if __name__ == '__main__':
    calculate_average_age()
