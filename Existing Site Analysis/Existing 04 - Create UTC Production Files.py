#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 17:55:21 2025

@author: shelbiedavis1
This code changes the local time PySAM output to full UTC year output
"""

# import packages
import pandas as pd
import json
import os
from datetime import datetime

# Enter Analysis Year
year = 2022

# Enter Project File Name without .json ending
file = f"{year} Eastern Interconnect Analysis"

# load site dictionary from json
with open('/Users/shelbiedavis1/Multi-State Simulation/Project json files/' + file + '.json') as f:
    site_dict = json.load(f)

# %% Snow Data
# initialize loop variables
i = 0    
log_file_path = 'missing_local_time_file_log.txt'
year_early = year-1
year_late = year+1

# Open log file to add generated error messages
with open(log_file_path, 'a') as log_file:
    
    # Loop through each site in site_dict
    for i, site_key in enumerate(site_dict):
        print(f"Testing site {site_key} at index {i}")
        site_data = site_dict[site_key]
        
        try:            
            file_early = '/Users/shelbiedavis1/Multi-State Simulation/PySAM_Results_UTC/Existing_Sites_Results/Roof_Slide_Coeff/'+str(year_early)+' SAM Results/'+site_data['Project Name']+'_'+str(year_early)+'_Results.csv'
            file_name = '/Users/shelbiedavis1/Multi-State Simulation/PySAM_Results_UTC/Existing_Sites_Results/Roof_Slide_Coeff/'+str(year)+' SAM Results/'+site_data['Project Name']+'_'+str(year)+'_Results.csv'
            file_late = '/Users/shelbiedavis1/Multi-State Simulation/PySAM_Results_UTC/Existing_Sites_Results/Roof_Slide_Coeff/'+str(year_late)+' SAM Results/'+site_data['Project Name']+'_'+str(year_late)+'_Results.csv'
            
            # Load CSVs into a DataFrames
            df_early = pd.read_csv(file_early)
            df = pd.read_csv(file_name)
            try:
                df_late = pd.read_csv(file_late)
            except FileNotFoundError:
                print(f"⚠️ 2023 data not found for site {site_key}, truncating end of 2022 only")
                df_late = pd.DataFrame(columns=df.columns)  # Empty frame with same columns
            
            # Rename the UTC column
            for df_ in [df_early, df, df_late]:
                df_.rename(columns={df_.columns[0]: 'UTC'}, inplace=True)
            
            # Concatenate early, current, and late year DataFrames
            df_all = pd.concat([df_early, df, df_late], ignore_index=True)
            
            # Ensure UTC column is parsed and timezone-aware
            df_all['UTC'] = pd.to_datetime(df_all['UTC'], utc=True)
            
            # Set UTC as index for filtering
            df_all.set_index('UTC', inplace=True)
            
            # Define exact UTC time window
            start_utc = pd.Timestamp(f"{year}-01-01 00:00:00", tz='UTC')
            end_utc = pd.Timestamp(f"{year + 1}-01-01 00:00:00", tz='UTC')
            
            # Slice for the desired UTC year range
            df_year = df_all.loc[start_utc:end_utc - pd.Timedelta(seconds=1)]
    
            expected_hours = 8760  # Because SAM always outputs 8760 hours regardless of leap year
            if len(df_year) != expected_hours:
                print(f"⚠️ Warning: Got {len(df_year)} hours for site {site_key}, expected 8760 from SAM")
            if pd.Timestamp(f"{year}-01-01").is_leap_year:
                print(f"ℹ️ Note: {year} is a leap year, but SAM omits Feb 29. Expecting 8760 hours.")
            
            # Define output directory (create it if needed)
            output_dir = '/Users/shelbiedavis1/Multi-State Simulation/PySAM_Results/Existing_Sites_Results_Full_UTC/Roof_Slide_Coeff/'+str(year)+' SAM Results/'
            os.makedirs(output_dir, exist_ok=True)
    
            # Construct output filename
            output_filename = f"{site_data['Project Name']}_{year}_Results.csv"
            output_path = os.path.join(output_dir, output_filename)
            
            # Ensure you're working on a copy
            df_year = df_year.copy()
    
            # Format 'Local Datetime' with full offset like '-05:00'
            if 'Local Datetime' in df_year.columns: #check if local time column exists
                # converts string to datetime
                df_year['Local Datetime'] = df_year['Local Datetime'].apply(
                    lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S %z') if pd.notnull(x) else None
                    )
                # reformats datetime
                df_year['Local Datetime'] = df_year['Local Datetime'].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S%z')[:-2] + ':' + x.strftime('%Y-%m-%d %H:%M:%S%z')[-2:]
                    if pd.notnull(x) else ''
                    )
    
            # Save to CSV
            df_year.to_csv(output_path)
    
            print(f"Saved: {output_path}")
        except Exception as e:
            print(f"Error at site {site_key} (index {i}): {e}")
            log_file.write(f"Error at site {site_key} (index {i}, {site_data['Project Name']}): {e}\n")
#            break  # Stop at the failing site for closer inspection
        
# %% No Snow Data
# initialize loop variables
i = 0    
log_file_path = 'missing_local_time_file_log.txt'
year_early = year-1
year_late = year+1

# Open log file to add generated error messages
with open(log_file_path, 'a') as log_file:
    
    # Loop through each site in site_dict
    for i, site_key in enumerate(site_dict):
        print(f"Testing site {site_key} at index {i}")
        site_data = site_dict[site_key]
        
        try:            
            file_early = '/Users/shelbiedavis1/Multi-State Simulation/PySAM_Results_UTC/Existing_Sites_Results/No_Snow/'+str(year_early)+' SAM Results/'+site_data['Project Name']+'_'+str(year_early)+'_Results.csv'
            file_name = '/Users/shelbiedavis1/Multi-State Simulation/PySAM_Results_UTC/Existing_Sites_Results/No_Snow/'+str(year)+' SAM Results/'+site_data['Project Name']+'_'+str(year)+'_Results.csv'
            file_late = '/Users/shelbiedavis1/Multi-State Simulation/PySAM_Results_UTC/Existing_Sites_Results/No_Snow/'+str(year_late)+' SAM Results/'+site_data['Project Name']+'_'+str(year_late)+'_Results.csv'
            
            # Load CSVs into a DataFrames
            df_early = pd.read_csv(file_early)
            df = pd.read_csv(file_name)
            try:
                df_late = pd.read_csv(file_late)
            except FileNotFoundError:
                print(f"⚠️ 2023 data not found for site {site_key}, truncating end of 2022 only")
                df_late = pd.DataFrame(columns=df.columns)  # Empty frame with same columns
            
            # Rename the UTC column
            for df_ in [df_early, df, df_late]:
                df_.rename(columns={df_.columns[0]: 'UTC'}, inplace=True)
            
            # Concatenate early, current, and late year DataFrames
            df_all = pd.concat([df_early, df, df_late], ignore_index=True)
            
            # Ensure UTC column is parsed and timezone-aware
            df_all['UTC'] = pd.to_datetime(df_all['UTC'], utc=True)
            
            # Set UTC as index for filtering
            df_all.set_index('UTC', inplace=True)
            
            # Define exact UTC time window
            start_utc = pd.Timestamp(f"{year}-01-01 00:00:00", tz='UTC')
            end_utc = pd.Timestamp(f"{year + 1}-01-01 00:00:00", tz='UTC')
            
            # Slice for the desired UTC year range
            df_year = df_all.loc[start_utc:end_utc - pd.Timedelta(seconds=1)]
    
            expected_hours = 8760  # Because SAM always outputs 8760 hours regardless of leap year
            if len(df_year) != expected_hours:
                print(f"⚠️ Warning: Got {len(df_year)} hours for site {site_key}, expected 8760 from SAM")
            if pd.Timestamp(f"{year}-01-01").is_leap_year:
                print(f"ℹ️ Note: {year} is a leap year, but SAM omits Feb 29. Expecting 8760 hours.")
            
            # Define output directory (create it if needed)
            output_dir = '/Users/shelbiedavis1/Multi-State Simulation/PySAM_Results/Existing_Sites_Results_Full_UTC/No_Snow/'+str(year)+' SAM Results/'
            os.makedirs(output_dir, exist_ok=True)
    
            # Construct output filename
            output_filename = f"{site_data['Project Name']}_{year}_Results.csv"
            output_path = os.path.join(output_dir, output_filename)
            
            # Ensure you're working on a copy
            df_year = df_year.copy()
    
            # Format 'Local Datetime' with full offset like '-05:00'
            if 'Local Datetime' in df_year.columns: #check if local time column exists
                # converts string to datetime
                df_year['Local Datetime'] = df_year['Local Datetime'].apply(
                    lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S %z') if pd.notnull(x) else None
                    )
                # reformats datetime
                df_year['Local Datetime'] = df_year['Local Datetime'].apply(
                    lambda x: x.strftime('%Y-%m-%d %H:%M:%S%z')[:-2] + ':' + x.strftime('%Y-%m-%d %H:%M:%S%z')[-2:]
                    if pd.notnull(x) else ''
                    )
    
            # Save to CSV
            df_year.to_csv(output_path)
    
            print(f"Saved: {output_path}")
        except Exception as e:
            print(f"Error at site {site_key} (index {i}): {e}")
            log_file.write(f"Error at site {site_key} (index {i}, {site_data['Project Name']}): {e}\n")
#            break  # Stop at the failing site for closer inspection