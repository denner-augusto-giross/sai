import pandas as pd
from geopy.distance import geodesic
from db import read_data_from_db
from query import query_stuck_orders, query_available_providers

if __name__ == "__main__":

    # --- Step 1: Get the stuck orders ---
    print("Fetching stuck orders...")
    stuck_orders_query = query_stuck_orders()
    stuck_orders_df = read_data_from_db(stuck_orders_query)

    # --- Step 2: Get the available providers ---
    print("\nFetching available providers...")
    providers_query = query_available_providers()
    providers_df = read_data_from_db(providers_query)

    # --- Step 3: Implement the Matching Logic ---

    # Proceed only if we have both orders and providers
    if stuck_orders_df is not None and not stuck_orders_df.empty and providers_df is not None and not providers_df.empty:
        print("\n" + "-"*50)
        print("Calculating best providers for stuck orders...")

        # A. Clean up data: Ensure lat/lon are numeric and not missing
        stuck_orders_df.dropna(subset=['store_latitude', 'store_longitude'], inplace=True)
        providers_df.dropna(subset=['latitude', 'longitude'], inplace=True)

        stuck_orders_df['store_latitude'] = pd.to_numeric(stuck_orders_df['store_latitude'])
        stuck_orders_df['store_longitude'] = pd.to_numeric(stuck_orders_df['store_longitude'])
        providers_df['latitude'] = pd.to_numeric(providers_df['latitude'])
        providers_df['longitude'] = pd.to_numeric(providers_df['longitude'])

        # B. Create every possible combination of order-to-provider
        # This is a highly efficient "cross join" done with pandas
        stuck_orders_df['key'] = 1
        providers_df['key'] = 1
        all_combinations_df = pd.merge(stuck_orders_df, providers_df, on='key').drop('key', axis=1)

        # C. Calculate the distance for each combination
        # The .apply() method goes through each row and performs a calculation
        def calculate_distance(row):
            order_coords = (row['store_latitude'], row['store_longitude'])
            provider_coords = (row['latitude'], row['longitude'])
            return geodesic(order_coords, provider_coords).kilometers

        all_combinations_df['distance_km'] = all_combinations_df.apply(calculate_distance, axis=1)

        # D. Sort the results to find the closest provider for each order
        # We sort by order_id first, then by the criteria you defined:
        # distance (closest), total_releases (fewest), and score (highest)
        all_combinations_df.sort_values(
            by=['order_id', 'distance_km', 'total_releases', 'score'],
            ascending=[True, True, True, False],
            inplace=True
        )

        # E. Select only the best (top) provider for each order
        best_matches_df = all_combinations_df.groupby('order_id').first().reset_index()
        
        # F. Display the final results
        print("\n--- Best Provider for Each Order ---")
        # Show the most important columns for the final decision
        final_columns = [
            'order_id',
            'user_name',
            'provider_id',
            'provider_name',
            'distance_km',
            'total_releases',
            'score'
        ]
        print(best_matches_df[final_columns])

    else:
        print("\nCould not perform matching because there were no stuck orders or no available providers.")