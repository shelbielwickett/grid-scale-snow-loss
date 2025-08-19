"""
Created on Tuesday, April 15, 2025 at 4:56 PM

@author: shelbiedavis1

This code is an updated version of a previous code. 
This code outputs a PySAM file that now includes both local time and UTC.

Updated on 8/19/25 to use portable file paths for GitHub sharing
@author: shelbielwickett
"""

# Import all necessary packages
import PySAM.Pvwattsv8 as pv
import pandas as pd
import json
import os
from timezonefinder import TimezoneFinder
import pytz
from pathlib import Path

# === EDIT THIS PATH ===
BASE_DIR = Path("/Volumes/Wickett SSD/Snow_Loss_Project")

# === EDIT YEAR ===
year = 2022

# === EDIT PROJECT SITE DICTIONARY FILE NAME ===
file = f"{year} Eastern Interconnect Analysis.json"

# %% Snow Loss Simulation

# Initialize PySAM PVWatts model
system_model = pv.new()

# load site dictionary from json
JSON_DIR = BASE_DIR / "Project json files"
with open(JSON_DIR / file) as f:
    site_dict = json.load(f)

# Set up the PVWatts System Model with no financial analysis
system_model = pv.default('PVWattsNone')

# Activate Snow Model
system_model.SystemDesign.en_snowloss = 1

# Initialize variables for loop
i = 0

log_path = BASE_DIR / 'log_files'
folder = log_path.parent
folder.mkdir(parents=True, exist_ok=True)
log_file_path = log_path / 'missing_snow_depth_log.txt'

# Open log file to add generated error messages
with open(log_file_path, 'a') as log_file:
    
    # Loop through each site in site_dict
    for i, site_key in enumerate(site_dict):
        print(f"Testing site {site_key} at index {i}")
        site_data = site_dict[site_key]
        
        # Define output path
        output_path = BASE_DIR / f'PySAM_Results_UTC/Existing_Sites_Results/Roof_Slide_Coeff/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv'

        # Skip if file already exists
        if os.path.exists(output_path):
            print(f"Skipping site {site_key} (index {i}) — file already exists.")
            continue
                
        try:
            # Assign variables for the current site
            old_system_capacity = system_model.SystemDesign.system_capacity #get current PySAM capacity
            new_system_capacity = site_data['Solar Capacity MW-DC'] * 1000 #PySAM requires capacity in kW
            system_model.value('system_capacity', new_system_capacity) #assign new capacity
            
            #skip sites that have no listed capacity and write error in log file
            if pd.isna(site_data['Solar Capacity MW-DC']):
                log_file.write(f"Error at site {site_key} (index {i}, {site_data['Project Name']}): No MW-DC Value\n")
                continue
            
            # Other site-specific configurations
            if site_data['Tracking Type'] == 'Fixed Tilt':
                old_tracking_type = system_model.SystemDesign.array_type #get current PySAM array_type
                new_tracking_type = 0 #create new array_type
                system_model.value('array_type', new_tracking_type) #assign new array_type
                
                old_tilt = system_model.SystemDesign.tilt #get current PySAM tilt
                if site_data['Tilt'] == ' ':
                    new_tilt = 30 #if the tilt is blank in the csv, assign 30 degree tilt
                else:
                    new_tilt = float(site_data['Tilt']) #create new tilt from tilt in csv
                system_model.value('tilt', new_tilt) #assign new tilt

            elif site_data['Tracking Type'] == 'Single Axis':
                old_tracking_type = system_model.SystemDesign.array_type #get current PySAM array_type
                new_tracking_type = 2 #create new array_type
                system_model.value('array_type', new_tracking_type) #assign new array_type
                
                old_tilt = system_model.SystemDesign.tilt #get current PySAM tilt
                new_tilt = 0 #create new tilt
                system_model.value('tilt', new_tilt) #assign new tilt                
            
            lat = site_data['Latitude']
            lon = site_data['Longitude']
            #assign weather file path
            weather_file = BASE_DIR / f'SAM_Weather_Files/{year} Weather Files/{lat}_{lon}_SAM_final.csv'
            system_model.value('solar_resource_file', str(weather_file)) #assign weather file
            
            weather_df = pd.read_csv(weather_file, skiprows=1) #skip first two rows
            weather_df.columns = weather_df.iloc[0]  # Set columns
            weather_df = weather_df.drop(weather_df.index[0]) #drops extra header row
            weather_df.reset_index(drop=True, inplace=True) #resets index
            
            # Run the model for this site
            system_model.execute(0)
            
            #initialize empty lists
            ac_list = []
            dc_list = []
            dcsnowderate_list = []
            gen_list = []
            snow_list = []
            poa_list = []
            tamb_list = []
            
            ac = system_model.Outputs.ac
            ac_list.append(ac)

            dc = system_model.Outputs.dc
            dc_list.append(dc)
    
            dcsnowderate = system_model.Outputs.dcsnowderate
            dcsnowderate_list.append(dcsnowderate_list)
        
            gen = system_model.Outputs.gen
            gen_list.append(gen)
        
            snow = system_model.Outputs.snow
            snow_list.append(snow)
            
            poa = system_model.Outputs.poa
            poa_list.append(poa)
            
            tamb = system_model.Outputs.tamb
            tamb_list.append(tamb)
            
            
            
            dictionary = {'AC Inverter Output Power [W]': ac, 'DC Inverter Input Power [W]':dc, 'DC Power Loss Due to Snow [%]': dcsnowderate, 'System Power Generated [kW]': gen, 'Weather File Snow Depth': snow, 'poa':poa, 'tamb': tamb}
            dataframe = pd.DataFrame(dictionary)
            

            #change the columns from the weather file dataframe to local time timestamps
            naive_dt = pd.to_datetime(weather_df[['Year', 'Month', 'Day', 'Hour', 'Minute']])

            # Get timezone from lat/lon
            tf = TimezoneFinder()
            timezone_str = tf.timezone_at(lat=lat, lng=lon)

            if timezone_str is None:
                raise ValueError(f"Could not determine timezone for site {site_key} at lat {lat}, lon {lon}")
                log_file.write(f"Could not determine timezone for site {site_key} at lat {lat}, lon {lon}")
                
            # Localize and convert
            local_tz = pytz.timezone(timezone_str) #identifies timezone (example: America/New York)
            localized_dt = naive_dt.dt.tz_localize(local_tz, ambiguous=True, nonexistent='shift_forward') #attaches timezone to timestamp
            utc_dt = localized_dt.dt.tz_convert(pytz.UTC) #converts local time to UTC

            # Assign to dataframe
            dataframe.index = utc_dt
            #Format the localized datetime as strings
            local_dt_strings = localized_dt.dt.strftime('%Y-%m-%d %H:%M:%S %z')
            dataframe['Local Datetime'] = pd.Series(local_dt_strings.values, index=dataframe.index)
            
            
            #Save output results csv
            directory = BASE_DIR / f'PySAM_Results_UTC/Existing_Sites_Results/Roof_Slide_Coeff/{year} SAM Results/'
            os.makedirs(directory, exist_ok=True)
            output_path = BASE_DIR / f'PySAM_Results_UTC/Existing_Sites_Results/Roof_Slide_Coeff/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv'
            dataframe.to_csv(output_path)
                       
            print(f"Site {site_key} executed successfully with Snow Loss.")
            
    
        except Exception as e:
            print(f"Error at site {site_key} (index {i}): {e}")
            log_file.write(f"Error at site {site_key} (index {i}, {site_data['Project Name']}): {e}\n")
#            break  # Stop at the failing site for closer inspection

# %% No Snow Loss Simulation

# Initialize PySAM PVWatts model
system_model = pv.new()

# load site dictionary from json

with open(JSON_DIR / file) as f:
    site_dict = json.load(f)

# Set up the PVWatts System Model with no financial analysis
system_model = pv.default('PVWattsNone')

# Turn Off Snow Model
system_model.SystemDesign.en_snowloss = 0

# Initialize variables for loop
i = 0

# Open log file to add generated error messages
with open(log_file_path, 'a') as log_file:
    
    # Loop through each site in site_dict
    for i, site_key in enumerate(site_dict):
        print(f"Testing site {site_key} at index {i}")
        site_data = site_dict[site_key]
                
        # Define output path
        output_path = BASE_DIR / f'PySAM_Results_UTC/Existing_Sites_Results/No_Snow/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv'

        # Skip if file already exists
        if os.path.exists(output_path):
            print(f"Skipping site {site_key} (index {i}) — file already exists.")
            continue
        
        try:
            # Assign variables for the current site
            old_system_capacity = system_model.SystemDesign.system_capacity
            new_system_capacity = site_data['Solar Capacity MW-DC'] * 1000
            system_model.value('system_capacity', new_system_capacity)
            
            # Other site-specific configurations
            if site_data['Tracking Type'] == 'Fixed Tilt':
                old_tracking_type = system_model.SystemDesign.array_type
                new_tracking_type = 0
                system_model.value('array_type', new_tracking_type)
                
                old_tilt = system_model.SystemDesign.tilt
                if site_data['Tilt'] == ' ':
                    new_tilt = 30
                else:
                    new_tilt = float(site_data['Tilt'])
                system_model.value('tilt', new_tilt)
            
            elif site_data['Tracking Type'] == 'Single Axis':
                old_tracking_type = system_model.SystemDesign.array_type
                new_tracking_type = 2
                system_model.value('array_type', new_tracking_type)
                
                old_tilt = system_model.SystemDesign.tilt
                new_tilt = 0
                system_model.value('tilt', new_tilt)
                
            lat = site_data['Latitude']
            lon = site_data['Longitude']
            # Example weather file (change as needed)
            weather_file = BASE_DIR / f'SAM_Weather_Files/{year} Weather Files/{lat}_{lon}_SAM_final.csv'
            system_model.value('solar_resource_file', str(weather_file))
            
            weather_df = pd.read_csv(weather_file, skiprows=1)
            weather_df.columns = weather_df.iloc[0]
            weather_df = weather_df.drop(weather_df.index[0])
            weather_df.reset_index(drop=True, inplace=True)
            
            # Run the model for this site
            system_model.execute(0)
            
            ac_list = []
            dc_list = []
            dcsnowderate_list = []
            gen_list = []
            snow_list = []
            poa_list = []
            tamb_list = []
            
            ac = system_model.Outputs.ac
            ac_list.append(ac)

            dc = system_model.Outputs.dc
            dc_list.append(dc)
    
            dcsnowderate = system_model.Outputs.dcsnowderate
            dcsnowderate_list.append(dcsnowderate_list)
        
            gen = system_model.Outputs.gen
            gen_list.append(gen)
        
            snow = system_model.Outputs.snow
            snow_list.append(snow)
            
            poa = system_model.Outputs.poa
            poa_list.append(poa)
            
            tamb = system_model.Outputs.tamb
            tamb_list.append(tamb)
            
            
            dictionary = {'AC Inverter Output Power [W]': ac, 'DC Inverter Input Power [W]':dc, 'DC Power Loss Due to Snow [%]': dcsnowderate, 'System Power Generated [kW]': gen, 'Weather File Snow Depth': snow, 'poa':poa, 'tamb':tamb}
            dataframe = pd.DataFrame(dictionary)
            
            
            #change the columns from the weather file dataframe to local time timestamps
            naive_dt = pd.to_datetime(weather_df[['Year', 'Month', 'Day', 'Hour', 'Minute']])

            # Get timezone from lat/lon
            tf = TimezoneFinder()
            timezone_str = tf.timezone_at(lat=lat, lng=lon)

            if timezone_str is None:
                raise ValueError(f"Could not determine timezone for site {site_key} at lat {lat}, lon {lon}")
                log_file.write(f"Could not determine timezone for site {site_key} at lat {lat}, lon {lon}")
                
            # Localize and convert
            local_tz = pytz.timezone(timezone_str) #identifies timezone (example: America/New York)
            localized_dt = naive_dt.dt.tz_localize(local_tz, ambiguous=True, nonexistent='shift_forward') #attaches timezone to timestamp
            utc_dt = localized_dt.dt.tz_convert(pytz.UTC) #converts local time to UTC

            # Assign to dataframe
            dataframe.index = utc_dt
            #Format the localized datetime as strings
            local_dt_strings = localized_dt.dt.strftime('%Y-%m-%d %H:%M:%S %z')
            dataframe['Local Datetime'] = pd.Series(local_dt_strings.values, index=dataframe.index)
            
            #Save output results csv
            directory = BASE_DIR / f'PySAM_Results_UTC/Existing_Sites_Results/No_Snow/{year} SAM Results/'
            os.makedirs(directory, exist_ok=True)
            output_path = BASE_DIR / f'PySAM_Results_UTC/Existing_Sites_Results/No_Snow/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv'
            dataframe.to_csv(output_path)
                       
            print(f"Site {site_key} executed successfully with NO Snow Loss.")
             
    
        except Exception as e:
            print(f"Error at site {site_key} (index {i}): {e}")
            log_file.write(f"Error at site {site_key} (index {i}), {site_data['Project Name']}: {e}\n")
#            break  # Stop at the failing site for closer inspection
