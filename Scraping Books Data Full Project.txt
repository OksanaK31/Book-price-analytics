from bs4 import BeautifulSoup as bs
import requests
import mysql.connector
import time 
import re


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


cursor = mydb.cursor(buffered=True)

base_url = 'https://knygy.com.ua/'
page_num = 1 


def extract_price(price_text):
    numbers = re.findall(r'\d+', price_text)
    if numbers:
       return int(''.join(numbers))
    return None


cursor.execute("SELECT COUNT(*) FROM books_list")
db_count = cursor.fetchone()[0]
print(f"Current number of records in database: {db_count}")

check_duplicate_sql = """
SELECT 1 FROM books_list 
WHERE title = %s AND author = %s AND price = %s
LIMIT 1
"""

insert_sql = """
INSERT INTO books_list (title, author, price) 
VALUES (%s, %s, %s)
"""

total_processed = 0
total_inserted = 0

while True:
    url = base_url if page_num == 1 else f"{base_url}?page={page_num}"
    
    print(f"Scraping page {page_num}...")

    page = requests.get(url)
    soup = bs(page.text, 'html.parser')

    book_containers = soup.find_all('div', class_='brief')
    print(f"Found {len(book_containers)} book containers on page {page_num}")

    if not book_containers:
        print("No more books found. Ending scraping process.")
        break
        
    page_processed = 0
    page_inserted = 0
    

    for book in book_containers:
        title_div = book.find('div', class_='nazva')
        title = title_div.text.strip() if title_div else "None"
    
        author_div = book.find('div', class_='avtor')
        author = author_div.text.strip() if author_div else "None"

        price_div = book.find('div', class_='briefPrice')
        price = None
        if price_div:
            price_text = price_div.text.strip()
            price = extract_price(price_text)

        page_processed += 1
        total_processed += 1
        cursor.execute(check_duplicate_sql, (title, author, price))
        if cursor.fetchone() is None:
            # Book doesn't exist, insert it
            cursor.execute(insert_sql, (title, author, price))
            total_inserted += 1
            page_inserted += 1
            
        else:
            print(f"Duplicate found: {title} by {author} at price {price}")

    print(f"Page {page_num} summary:")
    print(f"  Processed: {page_processed}")
    print(f"  Inserted: {page_inserted}")
    mydb.commit()
    time.sleep(2)

    page_num += 1

cursor.execute("SELECT COUNT(*) FROM books_list")
total_rows = cursor.fetchone()[0]


cursor.close()
mydb.close()

print(f"\nOverall summary:")
print(f"Total processed: {total_processed}")
print(f"Total inserted: {total_inserted}")
print(f"Inserted {total_inserted} records into the database.")
print(f"Total rows in the table: {total_rows}")
print("Scraping completed.")
    