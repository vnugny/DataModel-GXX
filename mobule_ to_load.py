import psycopg2
from psycopg2 import sql, extras

def write_dict_list_to_postgres(dict_list, table_name, db_config):
    """
    Writes a list of dictionaries to a PostgreSQL table.

    :param dict_list: List of dictionaries to insert. Each dict represents a row.
    :param table_name: Target PostgreSQL table name.
    :param db_config: Dictionary with keys 'host', 'port', 'database', 'user', 'password'.
    """
    if not dict_list:
        print("Empty list provided. Nothing to insert.")
        return

    try:
        # Extract columns from first dict
        columns = dict_list[0].keys()

        # Establish connection
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()

        # Prepare insert query dynamically
        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        # Prepare values
        values = [tuple(d[col] for col in columns) for d in dict_list]

        # Use execute_batch for efficiency
        extras.execute_batch(cursor, insert_query, values)

        # Commit transaction
        conn.commit()
        print(f"Inserted {len(values)} records into {table_name}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Example usage
if __name__ == "__main__":
    data = [
        {'name': 'apple', 'color': 'red'},
        {'name': 'banana', 'color': 'yellow'},
        {'name': 'grape', 'color': 'purple'}
    ]
    table = 'fruits'

    db_config = {
        'host': 'localhost',
        'port': 5432,
        'database': 'your_database',
        'user': 'your_user',
        'password': 'your_password'
    }

    write_dict_list_to_postgres(data, table, db_config)