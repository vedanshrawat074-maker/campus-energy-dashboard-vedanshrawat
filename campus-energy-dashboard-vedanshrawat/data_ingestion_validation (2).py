import pandas as pd
import os
import re # Used for robust filename parsing

def ingest_and_validate_data(data_directory='data'):
    """
    Automatically reads all CSV files in a specified directory, 
    validates the data, adds metadata, and combines them into one DataFrame.
    """
    all_dfs = []
    
    print(f"--- Starting Data Ingestion from /{data_directory}/ ---")
    
    # Use os.walk or os.listdir to loop through the directory
    try:
        # Get a list of all items in the 'data' directory
        for filename in os.listdir(data_directory):
            # Check if the file is a CSV file
            if filename.endswith('.csv'):
                file_path = os.path.join(data_directory, filename)
                print(f"\nProcessing file: {filename}")
                
                # 1. Handle File Reading Exceptions
                try:
                    # Use pandas.read_csv to read the file
                    # Handle corrupt data by skipping bad lines
                    df = pd.read_csv(
                        file_path,
                        dtype={'kwh': float},
                        # Note: We rely on manual conversion below, so remove parse_dates here
                        on_bad_lines='skip' 
                    )
                    
                    if len(df) == 0:
                        print("  -- WARNING: File skipped due to reading errors or being empty.")
                        continue
                        
                except Exception as e:
                    print(f"  -- ERROR: Could not read file {filename} completely. Skipping. Reason: {e}")
                    continue
                    
                
                # 2. Data Type Validation and Conversion (Robustness Fix)
                if 'timestamp' in df.columns:
                    # Attempt to force the column to datetime type, coercing errors to NaT (Not a Time)
                    original_len = len(df)
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                    
                    # Drop rows where the timestamp conversion failed (i.e., NaT)
                    df.dropna(subset=['timestamp'], inplace=True)
                    
                    # Log the number of invalid rows dropped
                    if len(df) < original_len:
                        print(f"  -- VALIDATION: Dropped {original_len - len(df)} row(s) with invalid timestamp data.")
                else:
                    print("  -- WARNING: 'timestamp' column is missing from file. Skipping.")
                    continue
                
                
                # 3. Add Metadata (Building Name and Month)
                # Extract building name (e.g., 'A' from 'building_A_sep.csv')
                match = re.search(r'building_([A-Za-z]+)_', filename)
                building_id = match.group(1).upper() if match else 'UNKNOWN'
                df['building'] = f'Building {building_id}'
                
                # Extract month (now safe because 'timestamp' is confirmed datetime)
                df['month'] = df['timestamp'].dt.strftime('%B')

                # Basic Validation: Check for required columns after loading (redundant check, but safe)
                if 'kwh' not in df.columns:
                    print("  -- WARNING: 'kwh' column is missing after processing. Skipping.")
                    continue
                
                # Final check and cleanup
                df_clean = df[['building', 'month', 'timestamp', 'kwh']].dropna(subset=['kwh'])
                all_dfs.append(df_clean)
                print(f"  -- SUCCESS: {len(df_clean)} records ingested and added to master list.")

    except FileNotFoundError:
        print(f"\nFATAL ERROR: The data directory '{data_directory}' was not found.")
        print("Please ensure the directory exists and contains the CSV files.")
        return pd.DataFrame()
    except Exception as e:
        # Catch unexpected errors during directory traversal
        print(f"\nAn unexpected error occurred during file traversal: {e}")
        return pd.DataFrame()


    # 4. Combine all DataFrames into one clean DataFrame
    if all_dfs:
        df_combined = pd.concat(all_dfs, ignore_index=True)
        # Sort for clean presentation
        df_combined = df_combined.sort_values(by=['building', 'timestamp']).reset_index(drop=True)
        return df_combined
    else:
        print("\nNo valid data frames were combined.")
        return pd.DataFrame()


# --- Execution ---
if __name__ == "__main__":
    
    # Execute the ingestion and validation function
    df_combined = ingest_and_validate_data('data')
    
    print("\n" + "="*70)
    print("Expected Output 1: Single Merged DataFrame (df_combined)")
    print("="*70)
    
    # Expected Output: A single merged DataFrame
    if not df_combined.empty:
        print(df_combined.to_string(index=False))
        print(f"\nTotal records in combined DataFrame: {len(df_combined)}")
    else:
        print("DataFrame is empty.")