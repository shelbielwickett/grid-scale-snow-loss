#!/usr/bin/env python3

"""
Created on Tue Nov 19 22:09:55 2024
This script downloads NSRDB data for the PV sites via API and saves them as .parquet
files.  
Updated on 4/30/25 to include extra day at beginning and end of year for UTC alignment
Updated on 7/24/25 to use portable file paths for GitHub sharing
@author: shelbielwickett
"""
####################### Library Imports #############################
import pvlib
import pandas as pd
import asyncio
import time
import requests
import os
import aiohttp
import nest_asyncio
from pathlib import Path

####################### User Defined Constants #############################

# === CHOOSE BASE DIRECTORY ===
BASE_DIR = Path("/Volumes/Wickett SSD/Snow_Loss_Project")  # Set this to your external drive or data folder

# === EDIT YEAR ===
YEAR = 2020

# === ENTER EMAIL ===
email = "shelbied@mtu.edu"

# === ENTER NREL API ===
api = 'MMnlHTRA1FIWpFCH5JJAlLUFK16QmhzGiPQICAem'

####################### OTHER Constants #############################

# Define all file/folder paths relative to BASE_DIR
SITE_DATA_FILE = Path("Data/Site Data/2024_utility-scale_solar_data_update.csv")
NSRDB_DIR = BASE_DIR / f"NSRDB_parquet/{YEAR}_NSRDB_parquet"
LOG_FILE = BASE_DIR / "Logs/missing_existing_site_NSRDB_data_log.txt"

####################### Define All Functions #############################

# Log writing function
def write_to_log(message):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG_FILE, "a") as log:
        log.write(f"{message}\n")

# Loads and preprocesses existing site data. 
# Preprocessing includes filtering for NERC subregions that are in the Eastern Interconnect
def load_site_data(file_path, year):
    try:
        site_data = pd.read_csv(file_path)
        site_data = site_data.iloc[:, :-19]
        regions_to_keep = ['PJM', 'MISO', 'ISO-NE', 'NYISO', 'SPP', 'Southeast (non-ISO)']
        site_data = site_data[
            site_data['Region'].str.lower().isin([region.lower() for region in regions_to_keep])
        ]
        return site_data
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        write_to_log(f"Error: File {file_path} not found.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred while loading site data: {e}")
        write_to_log(f"An error occurred while loading site data: {e}")
        return pd.DataFrame()

# Create a site dictionary from the dataframe
def create_site_dictionary(site_df):
    return site_df.to_dict(orient='index')

# Save DataFrame to Parquet with metadata
def save_to_parquet(df, filename, metadata):
    folder = filename.parent
    folder.mkdir(parents=True, exist_ok=True)

    df.to_parquet(filename, engine='pyarrow', index=False)

    metadata_df = pd.DataFrame([metadata])
    metadata_filename = filename.with_name(filename.stem + "_metadata.parquet")
    metadata_df.to_parquet(metadata_filename, engine='pyarrow', index=False)

    print(f"Data saved to {filename} and metadata saved to {metadata_filename}")

# Restructure NSRDB, lat/lon, and site data
def restructure_data(psm3_data, lat, lon, site_number):
    structured_data = {'site_number': site_number, 'latitude': lat, 'longitude': lon, 'data': []}
    timestamps = psm3_data.get('Year', {}).keys()
    for timestamp in timestamps:
        timestamp_data = {'timestamp': str(timestamp)}
        for variable, values in psm3_data.items():
            timestamp_data[variable] = values.get(timestamp, None)
        structured_data['data'].append(timestamp_data)
    return structured_data

# Worker function for async download
async def worker(year, n, site_dict, semaphore, total_requests, session, request_times, lock, email, api):
    lat, lon = site_dict[n]['Latitude'], site_dict[n]['Longitude']
    project_name = site_dict[n].get('Project Name', 'Unknown Project')
    api_key = api
    site_file = NSRDB_DIR / f"{lat}_{lon}.parquet"

    async with lock:
        if site_file.exists():
            print(f"Data for site {n} already saved. Skipping...")
            return None

    async with semaphore:
        try:
            # Fetch data using pvlib
            psm3_data, metadata = await asyncio.to_thread(
                pvlib.iotools.get_psm3,
                map_variables=True,
                latitude=lat, longitude=lon,
                api_key=api_key,
                email=email,
                names=year,
                attributes=[
                    'dhi', 'dni', 'air_temperature', 'ghi',
                    'dew_point', 'surface_pressure',
                    'wind_direction', 'wind_speed', 'surface_albedo'
                ],
                leap_day=False
            )

            # Restructure and save data
            structured_data = restructure_data(psm3_data.to_dict(), lat, lon, site_number=n)
            df = pd.DataFrame(structured_data['data'])
            async with lock:
                save_to_parquet(df, site_file, metadata)
            return structured_data

        except requests.HTTPError as e:
            if "OVER_RATE_LIMIT" in str(e):
                print(f"Rate limit exceeded for project '{project_name}' (lat: {lat}, lon: {lon}). Pausing...")
                await asyncio.sleep(60)
                return await worker(year, n, site_dict, semaphore, total_requests, session, request_times, lock)
            else:
                print(f"HTTP error for project '{project_name}' (lat: {lat}, lon: {lon}): {e}")
                await asyncio.sleep(10)
                return None
        except Exception as e:
            print(f"Unexpected error for project '{project_name}' (lat: {lat}, lon: {lon}): {e}")
            return None

# Main function for async execution
async def main(year, site_dict):
    semaphore = asyncio.Semaphore(10)
    total_requests = [0]
    lock = asyncio.Lock()
    async with aiohttp.ClientSession() as session:
        tasks = [
            worker(year, n, site_dict, semaphore, total_requests, session, {}, lock, email, api)
            for n in site_dict
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

# Synchronous entry point
def run_async_main():
    asyncio.run(main(YEAR, site_dict))

####################### Run Script #############################

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Track the execution time
start_time = time.time()

# Load LBNL existing utility solar site data that has been filtered to only include pv sites
unfiltered_site_df = load_site_data(SITE_DATA_FILE, YEAR)

# Create site dictionary
site_dict = create_site_dictionary(unfiltered_site_df)

# Run the async main function
run_async_main()

# Print execution time and the unfitered site dataframe
end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")
print(unfiltered_site_df)
