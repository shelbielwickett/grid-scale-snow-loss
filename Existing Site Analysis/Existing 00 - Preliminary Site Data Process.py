 #!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 22:09:55 2024

@author: shelbiedavis1

Updated on 4/30/25 to include extra day at beginning and end of year for UTC alignment
"""

import pvlib
import pandas as pd
import asyncio
import time
import requests
import os
import aiohttp
import nest_asyncio

####################### Define All Functions #############################

# Log writing function
def write_to_log(message):
    log_file = "missing_existing_site_NSRDB_data_log.txt"
    with open(log_file, "a") as log:
        log.write(f"{message}\n")
        
# load_site_data function loads and preprocesses existing site data. 
# Preprocessing includes filtering out HI and AK data and only includeing
# NERC subregions that are in the Eastern Interconnect
def load_site_data(file_path, year):
    try:
        site_data = pd.read_csv(file_path)
        #filter out Hawaii and Alaska data
        #site_data = site_data[site_data['State'] != 'HI'].reset_index(drop=True)
        #site_data = site_data[site_data['State'] != 'AK'].reset_index(drop=True)
        site_data = site_data.iloc[:, :-19]
        #keep subregions that are in Eastern Interconnect
        regions_to_keep = ['PJM', 'MISO', 'ISO-NE', 'NYISO', 'SPP', 'Southeast (non-ISO)']
        site_data = site_data[
            site_data['Region'].str.lower().isin([region.lower() for region in regions_to_keep])
        ]
        #site_data = site_data[site_data['Solar COD Year'] == year]
        return site_data
    except FileNotFoundError:
        # Print error if the file path cannot be found
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
    folder = os.path.dirname(filename)
    if not os.path.exists(folder):
        os.makedirs(folder)
    df.to_parquet(filename, engine='pyarrow', index=False)
    metadata_df = pd.DataFrame([metadata])
    metadata_filename = filename.replace(".parquet", "_metadata.parquet")
    metadata_df.to_parquet(metadata_filename, engine='pyarrow', index=False)
    print(f"Data saved to {filename} and metadata saved to {metadata_filename}")

# Read Parquet file
def read_parquet_file(file_path):
    return pd.read_parquet(file_path, engine='pyarrow')

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

async def worker(year, n, site_dict, semaphore, total_requests, session, request_times, lock):
    lat, lon = site_dict[n]['Latitude'], site_dict[n]['Longitude']
    project_name = site_dict[n].get('Project Name', 'Unknown Project')
    api_key = 'MMnlHTRA1FIWpFCH5JJAlLUFK16QmhzGiPQICAem'
    site_file = f'/Users/shelbiedavis1/Multi-State Simulation/NSRDB_parquet/{year}_NSRDB_parquet/{lat}_{lon}.parquet'

    # Check if the file already exists
    async with lock:
        if os.path.exists(site_file):
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
                email='shelbied@mtu.edu',
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
                print(f"Rate limit exceeded for project '{project_name}' (lat: {lat}, lon: {lon}). Pausing for 1 minute...")
                await asyncio.sleep(60)  # Wait for 2 minutes before retrying
                return await worker(year, n, site_dict, semaphore, total_requests, session, request_times, lock)
            else:
                print(f"HTTP error for project '{project_name}' (lat: {lat}, lon: {lon}): {e}. Retrying...")
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
            worker(year, n, site_dict, semaphore, total_requests, session, {}, lock)
            for n in site_dict
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

# Synchronous entry point
def run_async_main():
    asyncio.run(main(year, site_dict))

####################### Primary Code #############################

# Apply nest_asyncio to allow nested event loops
nest_asyncio.apply()

# Track the execution time
start_time = time.time()

year = 2022
# loads LBNL existing utility solar site data that has been filtered to only
# include pv sites
site_data_file = '/Users/shelbiedavis1/Multi-State Simulation/Site Data/2024_utility-scale_solar_data_update.csv'
unfiltered_site_df = load_site_data(site_data_file, year)

# Create site dictionary
site_dict = create_site_dictionary(unfiltered_site_df)


# Run the async main function
run_async_main()

end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")

data = pd.read_csv(site_data_file)
print(data)

print(unfiltered_site_df)

# Save site dictionary to JSON
#output_json_path = 'Project json files/all_existing_sites.json'
#try:
#    with open(output_json_path, 'w') as f:
#        json.dump(site_dict, f)
#    print(f"Site dictionary saved to {output_json_path}")
#except Exception as e:
#    print(f"An error occurred while saving site dictionary to JSON: {e}")

# End timing
#end_time = time.time()
#print(f"Execution time: {end_time - start_time:.2f} seconds")


####################### Visualization Code #############################

####################### Visualization Code #############################

# Select a site from Michigan and one from Georgia

#michigan_site = unfiltered_site_df[unfiltered_site_df['State'] == 'MI'].iloc[1]
#georgia_site = unfiltered_site_df[unfiltered_site_df['State'] == 'GA'].iloc[1]


# Function to create separate figures for Michigan and Georgia showing irradiance and air temperature
#def plot_july_average_day_separate(site_row, state_name, month_number):
#    def load_and_process(site_row, month_number):
#        lat = site_row['Latitude']
#        lon = site_row['Longitude']
#        site_file = f'NSRDB_parquet/{year}_NSRDB_parquet/{lat}_{lon}_existing.parquet'
#        df = read_parquet_file(site_file)
#        
#        # Ensure data columns are formatted correctly
#        df['timestamp'] = pd.to_datetime(df['timestamp'])
#        df['month'] = df['timestamp'].dt.month
#        df['hour'] = df['timestamp'].dt.hour

        # Filter for January data and group by hour to get the average daily irradiance profile
#        january_data = df[df['month'] == month_number]
#        hourly_avg = january_data.groupby('hour')[['dhi', 'dni', 'ghi', 'temp_air']].mean()
        
#        return hourly_avg, lat, lon

    # Load and process data for the selected site
#    hourly_data, lat, lon = load_and_process(site_row, month_number)
    
    # Create a figure with two stacked subplots
#    fig, (ax_irradiance, ax_temp) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

    # Plot for Irradiance
#    ax_irradiance.plot(hourly_data.index, hourly_data['dhi'], label='DHI', marker='o')
#    ax_irradiance.plot(hourly_data.index, hourly_data['dni'], label='DNI', marker='o')
#    ax_irradiance.plot(hourly_data.index, hourly_data['ghi'], label='GHI', marker='o')
#    ax_irradiance.set_ylabel('Irradiance (W/m²)')
#    ax_irradiance.set_title(f'Average Hourly Irradiance in Month {month_number} for {state_name} ({lat}, {lon})')
#    ax_irradiance.legend(loc="upper left")
#    ax_irradiance.grid(True)

    # Plot for Air Temperature
#    ax_temp.plot(hourly_data.index, hourly_data['temp_air'], label='Air Temperature', color='tab:red', marker='o')
#    ax_temp.set_ylabel('Air Temperature (°C)')
#    ax_temp.set_title(f'Average Hourly Air Temperature in Month {month_number} for {state_name} ({lat}, {lon})')
#    ax_temp.legend(loc="upper left")
#    ax_temp.grid(True)

    # Common x-axis label
#    plt.xlabel('Hour of the Day')
#    plt.xticks(range(0, 24))  # Ensure all hours from 0 to 23 are displayed
#    plt.tight_layout()
#    plt.show()

# Plot for Michigan
#plot_july_average_day_separate(michigan_site, 'Michigan', 1)

# Plot for Georgia
#plot_july_average_day_separate(georgia_site, 'Georgia', 1)
