import csv
from .connect_to_db import get_db_connection


mydb = get_db_connection()
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