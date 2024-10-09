from db_conductor.adapters.mongodb_adapter import MongoDBAdapter

# Step 1: Connect to the MongoDB database
mongo_adapter = MongoDBAdapter(
    host="localhost",
    port=27017,
    database="test_db",
    collection="test_collection"
)
mongo_adapter.connect()

# Step 2: Insert toy data into the collection
toy_data = {"name": "John Doe", "age": 30, "occupation": "Software Developer"}
mongo_adapter.save(toy_data)
print("Inserted a single record into MongoDB.")

# Step 3: Insert multiple records (batch save)
batch_data = [
    {"name": "Alice", "age": 25, "occupation": "Data Scientist"},
    {"name": "Bob", "age": 32, "occupation": "System Administrator"}
]
mongo_adapter.save_batch(batch_data)
print("Inserted multiple records into MongoDB.")

# Step 4: Fetch the inserted data
query = {"name": "John Doe"}
retrieved_record = mongo_adapter.get(query)
print(f"Retrieved Record: {retrieved_record}")

# Step 5: Disconnect from the MongoDB database
mongo_adapter.disconnect()
