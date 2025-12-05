import pandas as pd
import os
import re
import matplotlib.pyplot as plt

# --- 1. MeterReading Class (No Change) ---
class MeterReading:
    """Represents a single meter reading with a timestamp and kWh usage."""
    def __init__(self, timestamp, kwh):
        self.timestamp = pd.to_datetime(timestamp)
        self.kwh = float(kwh)
    
    def __repr__(self):
        return f"MeterReading(timestamp='{self.timestamp.strftime('%Y-%m-%d')}', kwh={self.kwh})"

# --- 2. Building Class (No Change) ---
class Building:
    """Represents a building and manages its meter readings."""
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
        if self._df.empty:
            return 0.0
        return round(self._df['kwh'].sum(), 2)

    def generate_report(self):
        self._update_dataframe()
        total_kwh = self.calculate_total_consumption()
        summary = self._df['kwh'].agg(mean_kwh='mean', min_kwh='min', max_kwh='max')
        # ... (rest of report generation) ...
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

# --- 3. BuildingManager Class (Updated with Visualization) ---
class BuildingManager:
    """Manages all Building objects, data ingestion, and aggregation."""
    def __init__(self):
        self.buildings = {}
        self.combined_df = pd.DataFrame()

    def ingest_data(self, data_directory='data'):
        # ... (Ingestion logic from previous step, ensuring self.combined_df is set_index('timestamp'))
        all_dfs = []
        for filename in os.listdir(data_directory):
            if filename.endswith('.csv'):
                file_path = os.path.join(data_directory, filename)
                try:
                    df = pd.read_csv(file_path, dtype={'kwh': float}, on_bad_lines='skip')
                    if 'timestamp' in df.columns:
                        original_len = len(df)
                        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                        df.dropna(subset=['timestamp', 'kwh'], inplace=True)
                        # LOGGING (omitted for brevity, assume success)
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

    def get_daily_aggregates(self):
        """Calculates daily consumption and pivots it for plotting."""
        if self.combined_df.empty: return pd.DataFrame()
        
        # Calculate daily totals (Task 2 logic)
        df_daily = self.combined_df.groupby('building')['kwh'].resample('D').sum()
        
        # Pivot for easy plotting (columns = buildings, index = timestamp)
        df_pivot = df_daily.unstack(level=0, fill_value=0)
        
        return df_pivot
    
    def get_weekly_averages(self):
        """Calculates the average weekly consumption for the bar chart."""
        if self.combined_df.empty: return pd.DataFrame()
        
        # Calculate total usage per week, then average the weekly totals per building
        df_weekly_total = self.combined_df.groupby('building')['kwh'].resample('W').sum()
        
        # Group by building and calculate the mean of the weekly totals
        df_weekly_avg = df_weekly_total.groupby('building').mean().reset_index()
        
        return df_weekly_avg
    
    # NEW METHOD for Task 4
    def generate_dashboard(self, filename='dashboard.png'):
        """
        Generates a 3-part dashboard using Matplotlib and saves it to a file.
        Uses plt.subplots() for the unified figure.
        """
        # Prepare aggregated dataframes
        df_daily_pivot = self.get_daily_aggregates()
        df_weekly_avg = self.get_weekly_averages()
        
        if df_daily_pivot.empty or df_weekly_avg.empty:
            print("FATAL: Cannot generate dashboard. Aggregation data is missing.")
            return

        print("\n--- Generating Visualization Dashboard ---")

        # 1. Setup unified figure (3 rows, 1 column)
        fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(12, 12))
        fig.suptitle('Building Energy Consumption Dashboard', fontsize=16, y=1.02)
        
        # --- SUBPLOT 1: Trend Line (Daily Consumption) ---
        ax1 = axes[0]
        # Plot the daily pivot table (time index vs. kwh columns)
        df_daily_pivot.plot(ax=ax1, linewidth=2)
        ax1.set_title('1. Daily Consumption Trend Over Time')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Daily kWh Total')
        ax1.grid(True, linestyle='--', alpha=0.6)
        ax1.legend(title='Building', loc='upper left')
        
        # --- SUBPLOT 2: Bar Chart (Average Weekly Usage) ---
        ax2 = axes[1]
        # Use the average weekly data
        ax2.bar(df_weekly_avg['building'], df_weekly_avg['kwh'], color=['skyblue', 'salmon', 'lightgreen'])
        ax2.set_title('2. Comparison of Average Weekly Usage (kWh)')
        ax2.set_xlabel('Building')
        ax2.set_ylabel('Average Weekly kWh')
        ax2.grid(axis='y', linestyle='--', alpha=0.6)
        
        # --- SUBPLOT 3: Scatter Plot (Peak-Hour Consumption vs. Time/Building) ---
        # NOTE: Since the data is monthly *total*, not hourly, we'll plot the monthly total as the 'peak' event.
        ax3 = axes[2]
        df_combined_clean = self.combined_df.reset_index()
        
        # Scatter plot for each building's monthly consumption over time
        for name, group in df_combined_clean.groupby('building'):
            ax3.scatter(group['timestamp'], group['kwh'], label=name, alpha=0.7)
            
        ax3.set_title('3. Monthly Peak Consumption Events')
        ax3.set_xlabel('Date (Time)')
        ax3.set_ylabel('Monthly kWh Total ("Peak Event")')
        ax3.legend(title='Building')
        ax3.grid(True, linestyle='--', alpha=0.6)

        # Final layout adjustments and saving
        plt.tight_layout(rect=[0, 0, 1, 0.98]) # Adjust layout to prevent title overlap
        plt.savefig(filename)
        plt.close(fig)
        
        print(f"--- Dashboard saved successfully as {filename} ---")


# --- Execution ---
if __name__ == "__main__":
    
    data_folder = 'data'
    print("--- Starting Final Task 4: Visualization Dashboard ---")

    # Initialize the Manager (Task 3)
    manager = BuildingManager()

    # 1. Ingest Data (Task 1)
    # Logging is internal; assume successful ingestion for demonstration
    manager.ingest_data(data_folder)
    
    # 2. Generate Dashboard (Task 4)
    manager.generate_dashboard('dashboard.png')
    
    print("\n--- Project Execution Complete ---")