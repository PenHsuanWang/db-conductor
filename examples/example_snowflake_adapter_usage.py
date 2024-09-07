from db_conductor.adapters.snowflake_adapter import SnowflakeAdapter

# Step 1: Connect to the Snowflake database
snowflake_adapter = SnowflakeAdapter(
    user="your_user",
    password="your_password",
    account="your_account",
    role="your_role",
    warehouse="your_warehouse",
    database="your_database",
    schema="your_schema",
    table="test_table"
)
snowflake_adapter.connect()

# Step 2: Insert toy data into the Snowflake table
toy_data = {"id": 1, "name": "John Doe", "age": 30, "occupation": "Software Developer"}
snowflake_adapter.save(toy_data)
print("Inserted a single record into Snowflake.")

# Step 3: Insert multiple records (batch save)
batch_data = [
    {"id": 2, "name": "Alice", "age": 25, "occupation": "Data Scientist"},
    {"id": 3, "name": "Bob", "age": 32, "occupation": "System Administrator"}
]
snowflake_adapter.save_batch(batch_data)
print("Inserted multiple records into Snowflake.")

# Step 4: Fetch the inserted data
query = {"id": 1}
retrieved_record = snowflake_adapter.get(query)
print(f"Retrieved Record: {retrieved_record}")

# Step 5: Disconnect from the Snowflake database
snowflake_adapter.disconnect()
