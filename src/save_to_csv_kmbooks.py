import csv
from .connect_to_db import get_db_connection


connection = get_db_connection()
cursor = connection.cursor(buffered=True)

cursor.execute("SELECT * FROM kmbooks")
rows = cursor.fetchall()
column_names = [i[0] for i in cursor.description]

with open('KMBooks_Data.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(column_names)  
    csvwriter.writerows(rows)  

cursor.close()
connection.close()

print("Data exported successfully.")