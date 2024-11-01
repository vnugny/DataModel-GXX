import pyodbc
import pandas as pd
from datetime import datetime, timedelta

# Function to connect to the SQL Server and extract data in chunks by date range
def extract_large_data_by_date(server, database, username, password, table_name, start_date, end_date, chunk_size=100000):
    try:
        # Create a connection string
        connection = pyodbc.connect(
            f'DRIVER={{SQL Server}};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password}'
        )
        print("Connection successful")

        current_date = start_date

        while current_date < end_date:
            next_date = current_date + timedelta(days=1)  # Adjust to weeks/months if needed
            query = f"""
                SELECT * 
                FROM {table_name} 
                WHERE date_column >= '{current_date.strftime('%Y-%m-%d')}' 
                AND date_column < '{next_date.strftime('%Y-%m-%d')}'
            """
            
            chunk_count = 0
            for chunk in pd.read_sql_query(query, connection, chunksize=chunk_size):
                chunk_count += 1
                print(f"Processing chunk {chunk_count} for date {current_date.strftime('%Y-%m-%d')} with {len(chunk)} rows")
                # Save each chunk to a file
                chunk.to_csv(f'data_{current_date.strftime("%Y-%m-%d")}_chunk_{chunk_count}.csv', index=False)
                print(f"Chunk {chunk_count} for date {current_date.strftime('%Y-%m-%d')} saved")

            current_date = next_date

        # Close the connection
        connection.close()
        print("All data processed successfully")

    except pyodbc.Error as e:
        print("Error while connecting to SQL Server:", e)
    except Exception as e:
        print("An error occurred:", e)

# Main function
if __name__ == "__main__":
    server = 'your_server_name'
    database = 'your_database_name'
    username = 'your_username'
    password = 'your_password'
    table_name = 'your_table_name'

    # Define start and end dates
    start_date = datetime(2020, 1, 1)
    end_date = datetime.now()  # Adjust as needed

    # Extract data by looping through dates
    extract_large_data_by_date(server, database, username, password, table_name, start_date, end_date, chunk_size=100000)
