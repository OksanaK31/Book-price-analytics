import mysql.connector
import csv

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

cursor.execute("SELECT * FROM books_list")
rows = cursor.fetchall()
column_names = [i[0] for i in cursor.description]

with open('Bookss_Data.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(column_names)  
    csvwriter.writerows(rows)  

cursor.close()
mydb.close()

print("Data exported successfully.")