import pyodbc
import pandas as pd

# Function to connect to the SQL Server database and extract data in chunks
def extract_large_data_from_sql_server(server, database, username, password, query, chunk_size=100000):
    try:
        # Connection string for SQL Server
        conn = pyodbc.connect(
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
        )
        
        print("Connection established successfully.")
        
        # Iterate over the query results in chunks
        chunks_processed = 0
        for chunk in pd.read_sql_query(query, conn, chunksize=chunk_size):
            # Process each chunk here (e.g., save to a file, process data)
            # Example: Append chunk to a CSV file
            chunk.to_csv('output_large_dataset.csv', mode='a', index=False, header=not chunks_processed)
            chunks_processed += 1
            print(f"Chunk {chunks_processed} processed.")
        
        # Close the connection
        conn.close()
        print("Data extraction completed successfully.")
    
    except Exception as e:
        print("An error occurred while connecting to the database or extracting data.")
        print(e)

# Parameters for database connection
server = 'your_server_name'
database = 'your_database_name'
username = 'your_username'
password = 'your_password'
query = 'SELECT * FROM your_large_table_name'

# Extract data with chunking
extract_large_data_from_sql_server(server, database, username, password, query)
