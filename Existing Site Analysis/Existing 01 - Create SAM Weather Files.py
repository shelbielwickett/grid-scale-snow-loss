#!/usr/bin/env python3

"""
This script uses the saved .parquet NSRDB files and combines them with
snow depth data from the NSIDC to create SAM weather files for each PV site.
Updated on 8/18/25 to use portable file paths for GitHub sharing
@author: shelbielwickett
"""
####################### Library Imports #############################
from pathlib import Path
import asyncio
import pandas as pd
import numpy as np
import xarray as xr
import aiofiles
import re
import time
import nest_asyncio

####################### User Defined Constants #############################
# === EDIT YEAR ===
year = 2020

# === CHOOSE BASE DIRECTORY ===
BASE_DIR = Path("/Volumes/Wickett SSD/Snow_Loss_Project")

####################### OTHER Constants #############################
# INPUTS
folder_path = BASE_DIR / f"NSRDB_parquet/{year}_NSRDB_parquet"  # Input Parquet directory
nc_file_path = Path("Data/Snow Data") / f"4km_SWE_Depth_WY{year}_v01.nc"  # NetCDF stays local

# OUTPUTS
output_parquet_dir = BASE_DIR / f"NSRDB_parquet/{year}_NSRDB_parquet"  # Same as input
output_csv_dir = BASE_DIR / f"SAM_Weather_Files/{year} Weather Files"
output_csv_dir.mkdir(parents=True, exist_ok=True)

####################### Define All Functions #############################

# Function to save a DataFrame to a CSV file with metadata included
async def save_to_csv_with_metadata_from_parquet(data_df, metadata_df, output_file_path):

    # Define headers for the main data rows in the CSV
    data_headers = [
        'Year', 'Month', 'Day', 'Hour', 'Minute', 'DHI', 'DNI', 'GHI', 'Dew Point',
        'Temperature', 'Pressure', 'Wind Direction', 'Wind Speed', 'Surface Albedo','snow depth'
    ]

    # Split the timestamp into separate columns for year, month, day, hour, and minute
    data_df['Year'] = data_df['timestamp'].dt.year
    data_df['Month'] = data_df['timestamp'].dt.month
    data_df['Day'] = data_df['timestamp'].dt.day
    data_df['Hour'] = data_df['timestamp'].dt.hour
    data_df['Minute'] = data_df['timestamp'].dt.minute
    print(data_df.head(50))

    # Ensure required columns are present, filling any missing ones with NaN
    weather_data_columns = [
        'Year', 'Month', 'Day', 'Hour', 'Minute', 'dhi', 'dni', 'ghi', 'dew_point',
        'temp_air', 'surface_pressure', 'wind_direction','wind_speed','surface_albedo','snow_depth'
    ]
    for col in weather_data_columns:
        if col not in data_df.columns:
            data_df[col] = np.nan

    # Select and rename columns
    data_df = data_df[weather_data_columns]
    data_df.columns = data_headers

    # Asynchronously write metadata and headers to the file
    async with aiofiles.open(output_file_path, 'w') as f:
        await f.write(','.join(map(str, metadata_df.columns)) + '\n')
        await f.write(','.join(map(str, metadata_df.iloc[0])) + '\n')
        await f.write(','.join(data_headers) + '\n')

    # Append the data rows (without headers) to complete the .csv file
    await asyncio.to_thread(data_df.to_csv, output_file_path, mode='a', index=False, header=False)
    print(f"File saved to {output_file_path}")

# Retrieve snow depth at specific lat, lon, and timestamp from the NetCDF dataset
# Also, convert daily data to hourly
async def get_snow_depth(timestamp, lat, lon, nc_data):
    try:
        # Get the date (without time) for daily data and select the closest day in the NetCDF file
        daily_date = timestamp.normalize()
        nc_slice = nc_data.sel(time=daily_date, method='nearest')
        
        # Select depth for the latitude and longitude
        depth_value = nc_slice.sel(lat=lat, lon=lon, method='nearest')['DEPTH'].values

        # Convert to a scalar (if necessary)
        if isinstance(depth_value, (list, np.ndarray)):
            depth_value = depth_value.item()

        # Convert mm to cm
        depth_value_cm = depth_value / 10
        
        # Return the same depth value for each hour within the day
        return depth_value_cm

    except Exception as e:
        print(f"Error fetching snow depth for {timestamp}, lat {lat}, lon {lon}: {e}")
        return np.nan  # Return NaN if failure occurs to prevent infinite retries

# Processes .parquet file and saves new SAM files
async def process_file(file_path, nc_data, folder_path):
    print(f"Processing file: {file_path}")

    # Extract latitude and longitude from the file name
    lat_lon_match = re.search(r'([-+]?\d*\.\d+)_([-+]?\d*\.\d+)', file_path.name)
    if lat_lon_match:
        lat = float(lat_lon_match.group(1))
        lon = float(lat_lon_match.group(2))
        print(f"Extracted Latitude: {lat}, Longitude: {lon}")
    else:
        raise ValueError("Could not extract lat and lon from file name.")

    # Check if the output .parquet file already exists
    updated_parquet_file_path = output_parquet_dir / f"{lat}_{lon}_snow.parquet"
    if Path(updated_parquet_file_path).exists():
        print(f"Skipping file: {updated_parquet_file_path} already exists")
        return

    # Load the .parquet file as a dataframe
    parquet_df = pd.read_parquet(file_path)

    # Convert timestamp columns to timezone-naive format
    parquet_df['timestamp'] = pd.to_datetime(parquet_df['timestamp']).dt.tz_localize(None)
    nc_data['time'] = pd.to_datetime(nc_data['time'].values).tz_localize(None)

    # Gather snow depth data asynchronously in batches
    batch_size = 50  # If issues arise, may need to adjust batch size.
    snow_depths = []
    for i in range(0, len(parquet_df), batch_size):
        batch = parquet_df.iloc[i:i + batch_size]
        depth_tasks = [get_snow_depth(row['timestamp'], lat, lon, nc_data) for _, row in batch.iterrows()]
        batch_depths = await asyncio.gather(*depth_tasks)
        snow_depths.extend(batch_depths)

    # Add snow depth data to the dataframe
    parquet_df['snow_depth'] = snow_depths

    # Save the updated .parquet file
    parquet_df.to_parquet(updated_parquet_file_path, engine='pyarrow', index=False)
    print(f"Updated Parquet file saved: {updated_parquet_file_path}")

    # Check for a corresponding metadata file and save with metadata if it exists
    metadata_file_path = folder_path / f"{lat}_{lon}_metadata.parquet"
    if metadata_file_path.exists():
        metadata_df = pd.read_parquet(metadata_file_path)
        output_file_path = output_csv_dir / f"{lat}_{lon}_SAM_final.csv"
        await save_to_csv_with_metadata_from_parquet(parquet_df, metadata_df, output_file_path)

    print(f"Processed and saved data for: {lat}, {lon}")

# Main function with semaphore, progress, and time tracking
async def main():
    print("Starting main function")

    # Set the maximum number of concurrent tasks
    max_concurrent_tasks = 20 
    semaphore = asyncio.Semaphore(max_concurrent_tasks)

    # Open the NetCDF dataset once to pass to each processing task
    nc_data = xr.open_dataset(nc_file_path)

    # Prepare for progress and time tracking
    matching_files = [
        file for file in folder_path.glob("*.parquet")
        if re.match(r"[-+]?\d*\.\d+_[-+]?\d*\.\d+\.parquet$", file.name)
    ]
    total_files = len(matching_files)
    print(f"Total files to process: {total_files}")

    counter = 0
    counter_lock = asyncio.Lock()
    start_time = time.time()

    # Wrap `process_file` with semaphore and progress tracking
    async def sem_task(file_path):
        nonlocal counter
        async with semaphore:
            await process_file(file_path, nc_data, folder_path)

            # Update and print progress
            async with counter_lock:
                counter += 1
                elapsed_time = time.time() - start_time
                progress = (counter / total_files) * 100
                print(f"Progress: {counter}/{total_files} files processed ({progress:.2f}%), Elapsed Time: {elapsed_time:.2f} seconds")

    # Launch all tasks
    tasks = [sem_task(file) for file in matching_files]
    await asyncio.gather(*tasks)

    print("Main function completed")

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Wrapper function to run the async main function
def run_async_main():
    asyncio.run(main())

# Run the async main function
run_async_main()
