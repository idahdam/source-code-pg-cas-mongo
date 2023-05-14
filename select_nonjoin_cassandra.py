from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import time
import logging
#
# Configure logging
logging.basicConfig(filename='app_nonjoin_cass.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

loop_count = 100

# Set up authentication credentials
auth_provider = PlainTextAuthProvider(username='username', password='password')

# Connect to the Cassandra cluster
cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
session = cluster.connect()

# Define a list to store the elapsed times
elapsed_times = []

# Query the data from a table
query = SimpleStatement("SELECT country, hs9, country_name from my_keyspace.trading_data_combined  WHERE ym = '198801' AND country > 103 ORDER BY country DESC LIMIT 1000 ALLOW FILTERING;", fetch_size=1000)

# Loop as much as loop_count times
for i in range(loop_count):
    start_time = time.time()

    # Fetch all the rows from the result set
    rows = session.execute(query)

    # Write the rows to a file
    with open(f"cass_nonjoin/output_{i}.txt", "w") as f:
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

# Close the session and the cluster connection
session.shutdown()
cluster.shutdown()
