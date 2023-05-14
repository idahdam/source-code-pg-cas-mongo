import pymongo
import time
import logging

# Configure logging
logging.basicConfig(filename='app_nonjoin_mongo.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["db"]
collection = db["trading_data"]

# Define a list to store the elapsed times
elapsed_times = []
rows_count = 0

loop_count = 10

# Loop as much as loop_count times
for i in range(loop_count):
    # Query the data from a collection
    start_time = time.time()
    rows = collection.aggregate([{ "$group" : { "_id": 1, "sum": { "$sum" : "$value" }}}])
    rows_count = rows.count()

    # Write the rows to a file
    with open(f"mongo_nonjoin/output_{i}.txt", "w") as f:
        f.write(str(row) + "\n")

    # Record the elapsed time
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_times.append(elapsed_time)

# Calculate the total elapsed time and log it
total_time = (sum(elapsed_times) * 1000) / loop_count
#print(rows_count)
logging.info(f"All loops completed - Total time elapsed per operation: {total_time:.6f} milliseconds")
print(f"All loops completed - Total time elapsed per operation: {total_time:.6f} milliseconds")

# Close the database connection
client.close()
