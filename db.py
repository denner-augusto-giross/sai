import os
import pandas as pd
from peewee import MySQLDatabase
from dotenv import load_dotenv
from query import query_region

def read_data_from_db(query: list):
    """
    Connects to the database using peewee, executes the query,
    and returns the data as a pandas DataFrame.
    """
    # Load environment variables from .env file
    load_dotenv()
    host = os.getenv('HOST_2')
    user = os.getenv('USER_2')
    password = os.getenv('PASSWORD_2')
    port = int(os.getenv('PORT_2'))
    database = os.getenv('DATABASE_2')
    
    # --- Troubleshooting Step ---
    # This will print the host being used, to make sure it's correct.
    print(f"Attempting to connect to host: {host}")

    # Create the database connection object
    db = MySQLDatabase(
        database=database,
        user=user,
        password=password,
        host=host,
        port=port
    )

    # query = query_region(cities_ids)

    try:
        db.connect()
        print("Successfully connected to the database!")

        # Use pandas to read the data from the database
        # The db.connection() method gets the underlying DB-API 2 connection
        df = pd.read_sql_query(query, db.connection())
        return df

    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        # Check if the connection is open before trying to close it
        if not db.is_closed():
            db.close()
            print("Database connection closed.")