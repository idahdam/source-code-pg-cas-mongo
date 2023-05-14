import pymongo
import time
import logging

# Configure logging
logging.basicConfig(filename='app_nonjoin_mongo.log', filemode='w', format='%(asctime)s - %(message)s', level=logging.INFO)

# Connect to the MongoDB database
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["db"]
collection = db["trading_data_combined"]

# Define a list to store the elapsed times
elapsed_times = []

loop_count = 100

# Loop as much as loop_count times
for i in range(loop_count):
    # Query the data from a collection
    start_time = time.time()
    rows = collection.find({"ym": "198801", "code": {"$gt": 103}}, {"code": 1, "hs9": 1, "country_name": 1, "_id": 0}).sort([{"code", -1}]).limit(1000)

    # Write the rows to a file
    with open(f"mongo_nonjoin/output_{i}.txt", "w") as f:
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

# Close the database connection
client.close()
