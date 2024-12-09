import requests
import time
from .connect_to_db import get_db_connection
from mysql.connector import Error


connection = get_db_connection()
cursor = connection.cursor()


create_table_query = """
CREATE TABLE IF NOT EXISTS kmbooks (
    title TEXT,
    author TEXT,
    price int
)
"""
cursor.execute(create_table_query)


try:
    cursor.execute("""
    CREATE UNIQUE INDEX idx_title_price ON kmbooks (title, price)
    """)
except Error as e:
    print(f"Error creating index: {e}")


check_duplicate_sql = """
SELECT 1 FROM kmbooks 
WHERE title = %s AND price = %s
LIMIT 1
"""

insert_sql = """
INSERT IGNORE INTO kmbooks (title, author, price) 
VALUES (%s, %s, %s)
"""


total_processed = 0
total_inserted = 0
duplicates = 0

url = "https://api.kmbooks.com.ua/api/books/search"
page = 0
limit = 20
total_books = 0


while True:
    params = {
        "page": page,
        "limit": limit
    }
    
    response = requests.get(url, params=params, timeout=30)
    
    if response.status_code == 200:
        data = response.json()
        books = data['bookList']
        book_count = len(books)
        total_books += book_count
        
        for book in books:
            title = book['name']
            author = book['author']
            price = book['price']
            
            try:
                cursor.execute(check_duplicate_sql, (title, price))
                if cursor.fetchone() is None:
                    cursor.execute(insert_sql, (title, author, price))
                    if cursor.rowcount > 0:
                        total_inserted += 1
                    else:
                        duplicates += 1
                        print(f"Duplicate found: {title} by {author} at price {price}")
                else:
                    duplicates += 1
                    print(f"Duplicate found: {title} by {author} at price {price}")
            except Error as e:
                print(f"Error processing book {title}: {e}")

            total_processed += 1

        connection.commit()
        
        print(f"Page {page} - Books on this page: {book_count}")
        print(f"Total books so far: {total_books}")
        print("---")
        
        if book_count < limit:
            break
        
        page += 1
        time.sleep(2)
    else:
        print(f"Failed to fetch data: {response.status_code}")
        break


print(f"Total number of books processed: {total_processed}")
print(f"Total number of books inserted: {total_inserted}")
print(f"Number of duplicates found: {duplicates}")

cursor.close()
connection.close()
