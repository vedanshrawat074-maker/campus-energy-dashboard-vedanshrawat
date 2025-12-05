import pandas as pd
import os
import re

# --- 1. MeterReading Class ---
# Represents a single monthly usage record
class MeterReading:
    """Represents a single meter reading with a timestamp and kWh usage."""
    def __init__(self, timestamp, kwh):
        # Ensure kwh is a float and timestamp is a datetime object
        self.timestamp = pd.to_datetime(timestamp)
        self.kwh = float(kwh)
    
    def __repr__(self):
        return f"MeterReading(timestamp='{self.timestamp.strftime('%Y-%m-%d')}', kwh={self.kwh})"

# --- 2. Building Class ---
# Manages a building's name and its list of meter readings
class Building:
    """Represents a building and manages its meter readings."""
    def __init__(self, name):
        self.name = name
        # Stores a list of MeterReading objects
        self.meter_readings = []
        # Store data in a DataFrame for efficient aggregation (Task 2 logic)
        self._df = pd.DataFrame() 

    # Method 1: Add Reading (from Task 3 requirements)
    def add_reading(self, reading):
        """Adds a MeterReading object to the building's list of readings."""
        if isinstance(reading, MeterReading):
            self.meter_readings.append(reading)
        else:
            raise TypeError("Reading must be a MeterReading instance.")
    
    # Internal method to update the DataFrame used for aggregation
    def _update_dataframe(self):
        """Converts meter_readings list into a pandas DataFrame for aggregation."""
        data = [{'timestamp': r.timestamp, 'kwh': r.kwh} for r in self.meter_readings]
        if data:
            self._df = pd.DataFrame(data).set_index('timestamp').sort_index()
            
    # Method 2: Calculate Total Consumption (from Task 3 requirements)
    def calculate_total_consumption(self):
        """Calculates the total kWh consumption for the building."""
        self._update_dataframe()
        if self._df.empty:
            return 0.0
        return round(self._df['kwh'].sum(), 2)

    # Method 3: Generate Report (from Task 3 requirements)
    def generate_report(self):
        """Generates a summary report for the building (Task 3 Expected Output)."""
        total_kwh = self.calculate_total_consumption()
        
        # Calculate summary statistics (Task 2 logic)
        summary = self._df['kwh'].agg(mean_kwh='mean', min_kwh='min', max_kwh='max')
        
        report = (
            f"\n--- Report for Building: {self.name} ---\n"
            f"Total Months of Data: {len(self.meter_readings)}\n"
            f"Total Consumption (kWh): {total_kwh}\n"
            f"Mean Consumption (kWh): {summary['mean_kwh']:.2f}\n"
            f"Min Consumption (kWh): {summary['min_kwh']:.2f}\n"
            f"Max Consumption (kWh): {summary['max_kwh']:.2f}\n"
            "Monthly Readings:\n"
        )
        for reading in sorted(self.meter_readings, key=lambda r: r.timestamp):
            report += f"  - {reading.timestamp.strftime('%Y-%m')}: {reading.kwh} kWh\n"
        return report

# --- 3. BuildingManager Class ---
# Manages all Building objects and handles data ingestion
class BuildingManager:
    """Manages all Building objects and handles the data ingestion process."""
    def __init__(self):
        # Dictionary to store Building objects: {ID: Building_instance}
        self.buildings = {}
        self.combined_df = pd.DataFrame()

    # Method 4: Ingest Data (incorporates Task 1 logic)
    def ingest_data(self, data_directory='data'):
        """Reads multiple CSV files and populates the Building objects."""
        all_dfs = []
        
        for filename in os.listdir(data_directory):
            if filename.endswith('.csv'):
                file_path = os.path.join(data_directory, filename)
                
                try:
                    df = pd.read_csv(file_path, dtype={'kwh': float}, on_bad_lines='skip')
                    
                    # Robust Timestamp Conversion (Task 1 logic)
                    if 'timestamp' in df.columns:
                        original_len = len(df)
                        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                        df.dropna(subset=['timestamp', 'kwh'], inplace=True)
                        if len(df) < original_len:
                            print(f"  -- LOG: Dropped {original_len - len(df)} bad row(s) in {filename}.")
                    else:
                        continue 
                    
                    # Extract building ID
                    match = re.search(r'building_([A-Za-z]+)_', filename)
                    building_id = match.group(1).upper() if match else 'UNKNOWN'
                    building_name = f'Building {building_id}'
                    df['building'] = building_name
                    
                    all_dfs.append(df[['building', 'timestamp', 'kwh']])
                    
                    # Create or GET the existing Building object
                    if building_id not in self.buildings:
                        self.buildings[building_id] = Building(building_name)
                    building = self.buildings[building_id]

                    # Populate the Building object with MeterReading instances
                    for index, row in df.iterrows():
                        reading = MeterReading(row['timestamp'], row['kwh'])
                        building.add_reading(reading)

                except Exception as e:
                    print(f"  -- ERROR: Could not process {filename}. Reason: {e}")
                    continue

        if all_dfs:
            self.combined_df = pd.concat(all_dfs, ignore_index=True)
            self.combined_df = self.combined_df.set_index('timestamp').sort_index()

    # Aggregation method (Task 2 logic moved to Manager for central control)
    def get_daily_aggregates(self):
        """Calculates and returns the total daily electricity consumption per building."""
        if self.combined_df.empty: return pd.DataFrame()
        return self.combined_df.groupby('building')['kwh'].resample('D').sum().reset_index().dropna(subset=['kwh'])


# --- Execution ---
if __name__ == "__main__":
    
    # Setup for demonstration
    data_folder = 'data'
    print("--- Starting Task 3: Object-Oriented Modeling & Reporting ---")

    # Initialize the Manager (Task 3 requirement)
    manager = BuildingManager()

    # 1. Ingest Data and Populate Objects (Task 1 requirement)
    manager.ingest_data(data_folder)
    
    if manager.combined_df.empty:
        print("\nFATAL: No data ingested. Cannot generate reports.")
    else:
        # 2. Generate Aggregated Reports (Task 3 Expected Output)
        print("\n" + "="*70)
        print("Expected Output: Instances of Buildings with Aggregated Reports")
        print("="*70)
        
        # Iterate through the managed Building objects
        for building_id in sorted(manager.buildings.keys()):
            building = manager.buildings[building_id]
            # Uses calculate_total_consumption() and generate_report() methods
            print(building.generate_report())
            
        print("\n" + "="*70)
        print("Verification: Daily Aggregates (from BuildingManager)")
        print("="*70)
        # Display central DataFrame for full Task 1 verification
        daily_report = manager.get_daily_aggregates()
        print(daily_report.to_string(index=False))
        print(f"\nTotal records in combined DataFrame: {len(manager.combined_df)}")