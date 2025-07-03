from db import read_data_from_db
from query import query_stuck_orders # Import the new function

if __name__ == "__main__":
    
    # 1. Get the SQL query for stuck orders
    stuck_orders_query = query_stuck_orders()

    # 2. Pass the query to the function that reads from the database
    data_df = read_data_from_db(stuck_orders_query)

    # 3. Print the results
    if data_df is not None:
        print("\nResults for Stuck Orders:")
        print(data_df)