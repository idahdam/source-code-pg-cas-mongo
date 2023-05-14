from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
import time
import logging

# Configure logging
logging.basicConfig(filename='app_paginate_cass.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

loop_count = 100

# Set up authentication credentials
auth_provider = PlainTextAuthProvider(username='username', password='password')

# Connect to the Cassandra cluster
cluster = Cluster(['localhost'], port=9042, auth_provider=auth_provider)
session = cluster.connect()
session.default_timeout = 6000

# Define a list to store the elapsed times
elapsed_times = []

limit = 1000
fetch_size = 3500

# Query the data from a table
query = SimpleStatement("select country from my_keyspace.trading_data", fetch_size=fetch_size)
overhead = session.execute(query)
paging_state = overhead.paging_state # this will store the paging state

# Loop as much as loop_count times
for i in range(loop_count):
    start_time = time.time()

    mini_query = SimpleStatement("select country from my_keyspace.trading_data LIMIT " + str(limit), fetch_size=limit)

    # Fetch all the rows from the result set
    rows = session.execute(mini_query, paging_state=paging_state)

    # Write the rows to a file
    with open(f"cass_paginate/output_{i}.txt", "w") as f:
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
