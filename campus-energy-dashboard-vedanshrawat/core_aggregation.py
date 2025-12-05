import pandas as pd
import os
import re

# --- Data Ingestion (Simplified and Robust) ---

def ingest_data_for_aggregation(data_directory='data'):
    """
    Automatically reads all clean CSV files, combines them, and prepares the DataFrame 
    for aggregation tasks. This uses the robust logic developed previously.
    """
    all_dfs = []
    
    print("--- Running Ingestion to Create Master DataFrame ---")
    
    try:
        for filename in os.listdir(data_directory):
            if filename.endswith('.csv'):
                file_path = os.path.join(data_directory, filename)
                
                try:
                    df = pd.read_csv(
                        file_path,
                        dtype={'kwh': float},
                        on_bad_lines='skip' 
                    )
                    
                    if len(df) == 0:
                        continue

                    # Robust Timestamp Conversion and Cleanup
                    if 'timestamp' in df.columns:
                        original_len = len(df)
                        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                        df.dropna(subset=['timestamp'], inplace=True)
                        if len(df) < original_len:
                            print(f"  -- LOG: Dropped {original_len - len(df)} invalid timestamp row(s) in {filename}.")
                    else:
                        continue # Skip files without a timestamp column
                    
                    # Add Metadata
                    match = re.search(r'building_([A-Za-z]+)_', filename)
                    building_id = match.group(1).upper() if match else 'UNKNOWN'
                    df['building'] = f'Building {building_id}'
                    
                    all_dfs.append(df[['building', 'timestamp', 'kwh']].dropna(subset=['kwh']))

                except Exception as e:
                    print(f"  -- ERROR: Could not read {filename}. Reason: {e}")
                    continue

    except FileNotFoundError:
        print(f"FATAL ERROR: The data directory '{data_directory}' was not found.")
        return pd.DataFrame()

    if all_dfs:
        df_combined = pd.concat(all_dfs, ignore_index=True)
        # CRITICAL STEP: Set 'timestamp' as the index for resampling
        df_combined = df_combined.set_index('timestamp').sort_index()
        print("--- Master DataFrame Created and Indexed ---")
        return df_combined
    else:
        return pd.DataFrame()


# -------------------------------------------------------------------
# --- Task 2: Core Aggregation Logic ---
# -------------------------------------------------------------------

def calculate_daily_totals(df):
    """
    Calculates the total daily electricity consumption (kWh) per building.
    Uses resample('D') to achieve daily granularity.
    """
    if df.empty:
        return pd.DataFrame()
    
    print("\n--- Calculating Daily Totals ---")
    
    # 1. Group by building
    # 2. Resample the time index by Day ('D')
    # 3. Sum the 'kwh' values
    df_daily = df.groupby('building')['kwh'].resample('D').sum().reset_index()
    
    # Filter out NaNs that may result from days with no data
    df_daily.dropna(subset=['kwh'], inplace=True)
    
    return df_daily

def calculate_weekly_aggregates(df):
    """
    Calculates the total weekly electricity consumption (kWh) per building.
    Uses resample('W') to achieve weekly granularity.
    """
    if df.empty:
        return pd.DataFrame()
        
    print("\n--- Calculating Weekly Totals ---")

    # 1. Group by building
    # 2. Resample the time index by Week ('W')
    # 3. Sum the 'kwh' values
    df_weekly = df.groupby('building')['kwh'].resample('W').sum().reset_index()
    
    df_weekly.rename(columns={'timestamp': 'week_ending'}, inplace=True)
    df_weekly.dropna(subset=['kwh'], inplace=True)
    
    return df_weekly

def building_wise_summary(df):
    """
    Calculates a summary table (mean, min, max, total) for electricity consumption
    across the entire period for each building.
    """
    if df.empty:
        return {}
        
    print("\n--- Generating Building-Wise Summary ---")
    
    # Use .groupby() to group by building and then .agg() to apply multiple statistical functions
    summary_df = df.groupby('building')['kwh'].agg(
        total_kwh='sum',
        mean_kwh='mean',
        min_kwh='min',
        max_kwh='max'
    ).reset_index()
    
    # Convert the resulting DataFrame into a dictionary for storage/reporting
    # Use 'records' format for a list of dictionaries, easier to iterate
    summary_dict = summary_df.to_dict('records')
    
    return summary_dict

# --- Execution ---
if __name__ == "__main__":
    
    # Step 1: Ingest Data and create the master DataFrame
    master_df = ingest_data_for_aggregation('data')

    if master_df.empty:
        print("\nCannot proceed with aggregation. Master DataFrame is empty.")
    else:
        # Step 2: Calculate Daily Totals
        daily_df = calculate_daily_totals(master_df.copy())
        
        # Step 3: Calculate Weekly Totals
        weekly_df = calculate_weekly_aggregates(master_df.copy())
        
        # Step 4: Calculate Building-Wise Summary
        summary_results = building_wise_summary(master_df.copy())

        # --- FINAL OUTPUT ---
        
        print("\n" + "="*70)
        print("FINAL REPORT: Aggregation Results")
        print("="*70)
        
        ## Daily Totals Output
        print("\n### Daily Totals (df_daily) ###")
        print(daily_df.to_string(index=False))

        ## Weekly Totals Output
        print("\n### Weekly Totals (df_weekly) ###")
        print(weekly_df.to_string(index=False))

        ## Summary Table Output
        print("\n### Building-Wise Summary (Stored as List of Dictionaries) ###")
        print(pd.DataFrame(summary_results).to_string(index=False))
        
        print("\n--- Task Complete ---")