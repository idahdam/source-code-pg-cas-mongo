import psycopg2
import time
import logging

# Configure logging
logging.basicConfig(filename='app_nonjoin_pg.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="localhost",
    database="dbname",
    user="dbuser",
    password="insert-ur-password-here"
)

# Open a cursor to perform database operations
cur = conn.cursor()

# Define a list to store the elapsed times
elapsed_times = []

loop_count = 100

# Loop as much as loop_count times
for i in range(loop_count):
    start_time = time.time()
    # Query the data from a table
    cur.execute("SELECT MIN(value) FROM trading_data")


    # Fetch all the rows from the result set
    rows = cur.fetchall()

    # Write the rows to a file
    with open(f"pg_nonjoin/output_{i}.txt", "w") as f:
        for row in rows:
            f.write(str(row) + "\n")

    # Record the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_times.append(elapsed_time)

# Calculate the total elapsed time and log it
total_time = (sum(elapsed_times) * 1000) / loop_count
logging.info(f"All loops completed - Total time elapsed per operation: {total_time:.6f} milliseconds")
print(f"All loops completed - Total time elapsed per operation: {total_time:.6f} milliseconds")

# Close the cursor and the database connection
cur.close()
conn.close()
