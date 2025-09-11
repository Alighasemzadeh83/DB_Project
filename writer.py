import csv
import psycopg2
from psycopg2 import sql


DB_HOST = "127.0.0.1"
DB_PORT = 5434
DB_NAME = "azuredata"
DB_USER = "azureuser"
DB_PASSWORD = "azurepass"


conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD
)
print("connected!")
cur = conn.cursor()


csv_file = "./data/fact_invocations_minutely_sparse_pg.csv"
i = 0
with open(csv_file, newline='') as f:
    reader = csv.reader(f)
    headers = next(reader) 

    for row in reader:
        i+=1
        if(i%10000 == 0):
            print(f"reached {i}")
        function_id = int(row[0])
        day = int(row[1])
        usage_array = row[2].strip("{}").split(",") 

        for minute_index, count_str in enumerate(usage_array):
            minute = minute_index + 1 
            count = int(count_str)

            if count > 0:

                cur.execute(
                    sql.SQL(
                        "INSERT INTO raw_invocations(function_id, day, minute, count) VALUES (%s, %s, %s, %s)"
                    ),
                    (function_id, day, minute, count)
                )


conn.commit()


cur.close()
conn.close()

print("CSV data inserted into raw_invocations successfully.")
