import mysql.connector


def get_db_connection():
    server = 'localhost'
    username = 'oksana'
    password = None
    db = "knygy_db"

    mydb = mysql.connector.connect(
        host=server,
        user=username,
        password=password,
        database=db,
        use_pure=True,
    )

    print(mydb) 

