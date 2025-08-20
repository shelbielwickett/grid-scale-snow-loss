#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 30 17:55:21 2025

@author: shelbiedavis1
This code changes the local time PySAM output to full UTC year output
Updated on 8/19/25 to use portable file paths for GitHub sharing

"""

# import packages
import pandas as pd
import json
import os
from datetime import datetime
from pathlib import Path

# === EDIT THIS PATH ===
BASE_DIR = Path("/Volumes/Wickett SSD/Snow_Loss_Project")

# === EDIT YEAR ===
year = 2022

# === EDIT PROJECT SITE DICTIONARY FILE NAME ===
file = f"{year} Eastern Interconnect Analysis.json"

# load site dictionary from json
JSON_DIR = BASE_DIR / "Project json files"
with open(JSON_DIR / file) as f:
    site_dict = json.load(f)

# ensure log_file exists
log_path = BASE_DIR / 'log_files'
log_path.mkdir(parents=True, exist_ok=True)
log_file_path = log_path / 'missing_local_time_file_log.txt'

year_early = year-1
year_late = year+1

# Helper Function - Safely read CSVs
def read_csv_or_empty(path: Path, template_cols, label: str, site_key: str, log_file):
    try:
        #rename 'UTC' column
        df = pd.read_csv(path)
        df.rename(columns={df.columns[0]: "UTC"}, inplace=True)
        return df
    except FileNotFoundError:
        msg = f"{label} file missing for site {site_key}: {path}"
        print(msg)
        log_file.write(msg + "\n")
        # empty frame with same columns as current-year df to keep concat
        return pd.DataFrame(columns=template_cols)
    
# Helper Function - Process scenarios
def process_scenario(scenario_folder_name: str):
    out_scenario = "Roof_Slide_Coeff" if "Roof_Slide_Coeff" in scenario_folder_name else "No_Snow"

    with open(log_file_path, "a") as log_file:
        for i, site_key in enumerate(site_dict):
            print(f"[{out_scenario}] Testing site {site_key} at index {i}")
            site_data = site_dict[site_key]

            try:
                #Build paths
                file_curr = BASE_DIR / f'PySAM_Results/Existing_Sites_Results/{scenario_folder_name}/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv'
                file_prev = BASE_DIR / f'PySAM_Results/Existing_Sites_Results/{scenario_folder_name}/{year_early} SAM Results/{site_data["Project Name"]}_{year_early}_Results.csv'
                file_next = BASE_DIR / f'PySAM_Results/Existing_Sites_Results/{scenario_folder_name}/{year_late} SAM Results/{site_data["Project Name"]}_{year_late}_Results.csv'

                #Read current year
                try:
                    df_curr = pd.read_csv(file_curr)
                except FileNotFoundError:
                    msg = f"{year} data missing for site {site_key}: {file_curr} - skipping site."
                    print(msg)
                    log_file.write(msg + "\n")
                    continue
                
                #Normalize column 0 to "UTC" on current year first
                df_curr.rename(columns={df_curr.columns[0]: "UTC"}, inplace = True)
                template_cols = df_curr.columns

                # Read previous and next years
                df_prev = read_csv_or_empty(file_prev, template_cols, str(year_early), site_key, log_file)
                df_next = read_csv_or_empty(file_next, template_cols, str(year_late), site_key, log_file)

                #Filter out empty or all-NA dataframes
                frames =[]
                if df_prev is not None and not df_prev.empty and not df_prev.dropna(how="all").empty:
                    frames.append(df_prev)
                
                #Current year is required and should not be empty
                frames.append(df_curr)

                if df_next is not None and not df_next.empty and not df_next.dropna(how="all").empty:
                    frames.append(df_next)

                df_all = pd.concat(frames, ignore_index=True)

                #Make sure UTC column is parsed and timezone-aware
                df_all["UTC"] = pd.to_datetime(df_all["UTC"], utc=True, errors="coerce")
                bad = df_all["UTC"].isna().sum()
                if bad:
                    warn = f"{bad} rows with invalid UTC dropped for site {site_key}"
                    print(warn)
                    log_file.write(warn + "\n")
                    df_all = df_all.dropna(subset = ["UTC"])
                
                df_all.set_index("UTC", inplace=True)
                
                #Define exact UTC time window
                start_utc = pd.Timestamp(f"{year}-01-01 00:00:00", tz = "UTC")
                end_utc = pd.Timestamp(f"{year + 1}-01-01 00:00:00", tz="UTC")

                #Slice UTC year range
                df_year = df_all.loc[start_utc:end_utc - pd.Timedelta(seconds=1)].copy()
                
                expected_hours = 8760
                if len(df_year) != expected_hours:
                       print(f"Got {len(df_year)} hours for site {site_key}, expected 8760.")

                # Format 'Local Datetime' with full offset e.g. '-05:00'
                if "local Datetime" in df_year.columns:
                    def _fmt_local(x):
                        if pd.isna(x) or x == "":
                            return ""
                        # Try with and without colon in %z
                        for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S%z"):
                            try:
                                dt = datetime.strptime(str(x), fmt)
                                s = dt.strftime("%Y-%m-%d %H:%M:%S%z")
                                return s[:-2] + ":" + s[-2:]
                            except Exception:
                                pass
                        #If parsing fails, leave as-is
                        return str(x)
                    df_year["Local Datetime"] = df_year["Local Datetime"].apply(_fmt_local)

                #Output directory & save
                output_dir = BASE_DIR / f"PySAM_Results_UTC/Existing_Sites_Results_UTC/{out_scenario}/{year} SAM Results/"
                output_dir.mkdir(parents=True, exist_ok=True)

                output_filename = f"{site_data['Project Name']}_{year}_Results.csv"
                output_path = output_dir / output_filename
                df_year.to_csv(output_path)
                print(f"Saved: {output_path}")
            except Exception as e:
                msg = f"Error at site {site_key} (index {i}, {site_data['Project Name']}): {e}"
                print(msg)
                log_file.write(msg + "\n")

#Run both scenarios
process_scenario("Roof_Slide_Coeff")
process_scenario("No_Snow")
""" 
# Open log file to add generated error messages
with open(log_file_path, 'a') as log_file:
    
    # Loop through each site in site_dict
    for i, site_key in enumerate(site_dict):
        print(f"Testing site {site_key} at index {i}")
        site_data = site_dict[site_key]
        
        try:            
            file_early = BASE_DIR / f'PySAM_Results/Existing_Sites_Results/Roof_Slide_Coeff/{year_early} SAM Results/{site_data["Project Name"]}_{year_early}_Results.csv'
            file_name = BASE_DIR / f'PySAM_Results/Existing_Sites_Results/Roof_Slide_Coeff/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv'
            file_late = BASE_DIR / f'PySAM_Results/Existing_Sites_Results/Roof_Slide_Coeff/{year_late} SAM Results/{site_data["Project Name"]}_{year_late}_Results.csv'
            
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
            output_dir = BASE_DIR / f'PySAM_Results_UTC/Existing_Sites_Results_UTC/Roof_Slide_Coeff/{year} SAM Results/'
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
#log_file_path = 'missing_local_time_file_log.txt'
year_early = year-1
year_late = year+1

# Open log file to add generated error messages
with open(log_file_path, 'a') as log_file:
    
    # Loop through each site in site_dict
    for i, site_key in enumerate(site_dict):
        print(f"Testing site {site_key} at index {i}")
        site_data = site_dict[site_key]
        
        try:            
            file_early = BASE_DIR / f'PySAM_Results/Existing_Sites_Results/No_Snow/{year_early} SAM Results/{site_data["Project Name"]}_{year_early}_Results.csv'
            file_name = BASE_DIR / f'PySAM_Results/Existing_Sites_Results/No_Snow/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv'
            file_late = BASE_DIR / f'PySAM_Results/Existing_Sites_Results/No_Snow/{year_late} SAM Results/{site_data["Project Name"]}_{year_late}_Results.csv'
            
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
            output_dir = BASE_DIR / f'PySAM_Results_UTC/Existing_Sites_Results_UTC/No_Snow/{year} SAM Results/'
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
#            break  # Stop at the failing site for closer inspection """