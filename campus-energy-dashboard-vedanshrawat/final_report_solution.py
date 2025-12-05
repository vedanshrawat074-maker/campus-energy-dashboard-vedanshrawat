import pandas as pd
import os
import re
import matplotlib.pyplot as plt

# --- OOP Classes (MeterReading and Building are unchanged from Task 3) ---
class MeterReading:
    def __init__(self, timestamp, kwh):
        self.timestamp = pd.to_datetime(timestamp)
        self.kwh = float(kwh)
    
class Building:
    def __init__(self, name):
        self.name = name
        self.meter_readings = []
        self._df = pd.DataFrame() 

    def add_reading(self, reading):
        if isinstance(reading, MeterReading):
            self.meter_readings.append(reading)
        else:
            raise TypeError("Reading must be a MeterReading instance.")
    
    def _update_dataframe(self):
        data = [{'timestamp': r.timestamp, 'kwh': r.kwh} for r in self.meter_readings]
        if data:
            self._df = pd.DataFrame(data).set_index('timestamp').sort_index()
            
    def calculate_total_consumption(self):
        self._update_dataframe()
        return round(self._df['kwh'].sum(), 2)

    def generate_report(self):
        self._update_dataframe()
        total_kwh = self.calculate_total_consumption()
        summary = self._df['kwh'].agg(mean_kwh='mean', min_kwh='min', max_kwh='max')
        
        report = (
            f"\n--- Report for Building: {self.name} ---\n"
            f"Total Consumption (kWh): {total_kwh}\n"
            f"Mean Consumption (kWh): {summary['mean_kwh']:.2f}\n"
            f"Min Consumption (kWh): {summary['min_kwh']:.2f}\n"
            f"Max Consumption (kWh): {summary['max_kwh']:.2f}\n"
        )
        return report

# --- BuildingManager Class (Updated for Task 5) ---
class BuildingManager:
    def __init__(self):
        self.buildings = {}
        self.combined_df = pd.DataFrame()
        self.output_dir = 'output' # Define output directory

    # Data Ingestion (Task 1) - Logic omitted for brevity, assumes success.
    def ingest_data(self, data_directory='data'):
        # ... (Ingestion logic from previous step)
        all_dfs = []
        for filename in os.listdir(data_directory):
            if filename.endswith('.csv'):
                file_path = os.path.join(data_directory, filename)
                try:
                    df = pd.read_csv(file_path, dtype={'kwh': float}, on_bad_lines='skip')
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                        df.dropna(subset=['timestamp', 'kwh'], inplace=True)
                    else: continue
                    match = re.search(r'building_([A-Za-z]+)_', filename)
                    building_id = match.group(1).upper() if match else 'UNKNOWN'
                    building_name = f'Building {building_id}'
                    df['building'] = building_name
                    all_dfs.append(df[['building', 'timestamp', 'kwh']])
                    
                    if building_id not in self.buildings:
                        self.buildings[building_id] = Building(building_name)
                    building = self.buildings[building_id]
                    for index, row in df.iterrows():
                        reading = MeterReading(row['timestamp'], row['kwh'])
                        building.add_reading(reading)
                except Exception as e:
                    print(f"  -- ERROR: Could not process {filename}. Reason: {e}")
                    continue

        if all_dfs:
            self.combined_df = pd.concat(all_dfs, ignore_index=True)
            self.combined_df = self.combined_df.set_index('timestamp').sort_index()

    # Aggregation Methods (Task 2)
    def get_daily_aggregates(self):
        if self.combined_df.empty: return pd.DataFrame()
        return self.combined_df.groupby('building')['kwh'].resample('D').sum().reset_index().dropna(subset=['kwh'])

    def get_summary_stats(self):
        """Calculates campus-wide summary stats (Task 2 & 5)."""
        if self.combined_df.empty: return pd.DataFrame()
        
        # Calculate summary per building
        summary_df = self.combined_df.groupby('building')['kwh'].agg(
            total_kwh='sum',
            mean_kwh='mean',
            min_kwh='min',
            max_kwh='max',
            data_points='count'
        ).reset_index()
        
        return summary_df
    
    # --- NEW METHOD 1: Export Data (Task 5 requirement) ---
    def export_data(self):
        """Exports the cleaned dataset and the summary statistics."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        # 1. Export Final Processed Dataset (cleaned_energy_data.csv)
        cleaned_data_export = self.combined_df.reset_index()
        cleaned_data_export['month'] = cleaned_data_export['timestamp'].dt.strftime('%B')
        cleaned_data_export.to_csv(os.path.join(self.output_dir, 'cleaned_energy_data.csv'), index=False)
        print(f"  -- EXPORT: Final processed dataset saved to {self.output_dir}/cleaned_energy_data.csv")

        # 2. Export Summary Stats (building_summary.csv)
        summary_stats = self.get_summary_stats()
        summary_stats.to_csv(os.path.join(self.output_dir, 'building_summary.csv'), index=False)
        print(f"  -- EXPORT: Summary statistics saved to {self.output_dir}/building_summary.csv")
        
        return summary_stats # Return for use in summary report

    # --- NEW METHOD 2: Create Summary Report (Task 5 requirement) ---
    def create_summary_report(self, summary_df):
        """Generates a short, concise summary report (summary.txt)."""
        if self.combined_df.empty or summary_df.empty:
            report = "No data available to generate a summary report."
            print(report)
            return
            
        # 1. Total Campus Consumption
        total_campus_kwh = summary_df['total_kwh'].sum()
        
        # 2. Highest-Consuming Building
        highest_consumer = summary_df.loc[summary_df['total_kwh'].idxmax()]
        
        # 3. Peak Load Time (Approximate using the single highest KWh reading)
        peak_reading = self.combined_df['kwh'].max()
        peak_timestamp = self.combined_df['kwh'].idxmax()
        
        # 4. Weekly/Daily Trends (e.g., Average Daily Consumption)
        daily_avg_df = self.combined_df.groupby('building')['kwh'].resample('D').sum().groupby('building').mean()
        
        report = "="*40 + "\n"
        report += "EXECUTIVE ENERGY CONSUMPTION SUMMARY\n"
        report += "="*40 + "\n"
        report += f"1. Total Campus Consumption: {total_campus_kwh:,.2f} kWh\n"
        report += f"2. Highest-Consuming Building: {highest_consumer['building']} ({highest_consumer['total_kwh']:,.2f} kWh)\n"
        report += f"3. Peak Load Event: {peak_reading:,.2f} kWh at {peak_timestamp.strftime('%Y-%m-%d')}\n\n"
        report += "4. Daily Consumption Trends (Average per Day with Data):\n"
        for building, avg_kwh in daily_avg_df.items():
             report += f"   - {building}: {avg_kwh:,.2f} kWh/day (Avg)\n"
        report += "="*40 + "\n"
        
        # Optionally, print this summary to the console (Task 5 optional step)
        print(report)
        
        # Save to file
        with open(os.path.join(self.output_dir, 'summary.txt'), 'w') as f:
            f.write(report)
        print(f"  -- EXPORT: Executive summary saved to {self.output_dir}/summary.txt")


# --- Execution ---
if __name__ == "__main__":
    
    data_folder = 'data'
    print("--- Starting Task 5: Persistence and Executive Summary ---")

    manager = BuildingManager()

    # 1. Ingest Data (Task 1)
    manager.ingest_data(data_folder)
    
    if manager.combined_df.empty:
        print("\nFATAL: No data ingested. Cannot generate reports.")
    else:
        # 2. Export Data (Task 5)
        print("\n[Step 2: Data Export]")
        summary_stats_df = manager.export_data()
        
        # 3. Create Summary Report (Task 5)
        print("\n[Step 3: Executive Summary]")
        manager.create_summary_report(summary_stats_df)
        
        # 4. Generate Dashboard (Task 4 - Optional inclusion for completeness)
        # Note: This is an optional step if you want to regenerate the dashboard.
        # manager.generate_dashboard('dashboard.png') 
        
        print("\n--- Project Fully Complete ---")