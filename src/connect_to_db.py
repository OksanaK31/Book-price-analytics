import mysql.connector


def get_db_connection():

    server = 'localhost'
    username = 'oksana'
    password = None
    db = "books_price"

    connection = mysql.connector.connect(
        host=server,
        user=username,
        password=password,
        database=db,
        use_pure=True,
    )
    
    print(connection)