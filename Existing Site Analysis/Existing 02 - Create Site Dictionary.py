#!/usr/bin/env python3

"""
This script creates a PV site dictionary for other scripts to easily access site metadata.
Updated on 8/19/25 to use portable file paths for GitHub sharing
@author: shelbielwickett
"""
####################### Library Imports #############################
import pandas as pd
import json
from pathlib import Path

####################### User Defined Constants #############################
# === EDIT YEAR ===
year = 2020

# === EDIT TRACKING TYPE ===
tracking_type = ['All']

# === EDIT ELECTRIC REGION ===
electric_region = ['MISO','ISO-NE', 'NYISO', 'PJM','Southeast (non-ISO)', 'SPP']

# === EDIT STATE ===
state = ['All']

# === EDIT TITLE ===
title = 'Eastern Interconnect'

# === CHOOSE BASE DIRECTORY ===
BASE_DIR = Path("/Volumes/Wickett SSD/Snow_Loss_Project")

####################### OTHER Constants #############################
analysis_title = f'{year} {title}'

# Load the site data
site_data = pd.read_csv('Data/Site Data/2024_utility-scale_solar_data_update.csv')
unfiltered_site_df = pd.DataFrame(site_data)
unfiltered_site_df = unfiltered_site_df.drop(unfiltered_site_df.columns[-19:], axis=1)

# Output site metadata files
csv_output_path = BASE_DIR / f'Existing Site Metadata Files/{year}_PV_existing_site_metadata.csv'
folder = csv_output_path.parent
folder.mkdir(parents=True, exist_ok=True)

json_file_path = BASE_DIR / f'Project json files/{analysis_title} Analysis.json'
folder = json_file_path.parent
folder.mkdir(parents=True, exist_ok=True)

####################### Define All Functions #############################

# Create a site dictionary that includes the closest corresponding NSIDC datapoint latitudes and longitudes
def create_site_dictionary(year, site_df):
    # Changes dataframe of sites into a dictionary
    site_dict = site_df.to_dict(orient='index')
    return site_dict

# This function allows the user to filter the type of sites they want to analyze
def filter_site_df(tracking_type, year, electric_region, state, site_df):
    filtered_site_df = site_df[site_df['Solar COD Year'] <= year[0]]
    #uncomment next line to not filter installation year
    #filtered_site_df = site_df

    if 'All' in tracking_type:
        filtered_site_df = filtered_site_df
    else:
        filtered_site_df = filtered_site_df[filtered_site_df['Tracking Type'].isin(tracking_type)]
    
    if 'All' in electric_region:
        filtered_site_df = filtered_site_df
    else:
        #filtered_site_df = filtered_site_df[filtered_site_df['Region'].isin(electric_region)]
        #regions_to_keep = ['PJM', 'MISO', 'ISO-NE', 'NYISO', 'SPP', 'Southeast (non-ISO)']
        filtered_site_df = filtered_site_df[
            filtered_site_df['Region'].str.lower().isin([region.lower() for region in electric_region])
        ]
    if 'All' in state:
        filtered_site_df = filtered_site_df
    else:
        filtered_site_df = filtered_site_df[filtered_site_df['State'].isin(state)]
        
    return filtered_site_df

def main() -> int:
    # Filter site data
    year_series = [year]
    site_df = filter_site_df(tracking_type, year_series, electric_region, state, unfiltered_site_df)
    site_df = site_df.reset_index(drop=True)
    print(site_df)

    # Create the site dictionary
    site_dict = create_site_dictionary(year, site_df)

    site_df.to_csv(csv_output_path, index=False)
    print(f"Site info CSV saved to {csv_output_path}")


    # Save the dictionary to a JSON file
    with open(json_file_path, 'w') as json_file:
        json.dump(site_dict, json_file, indent=4)
    print(f"Site dictionary saved to {json_file_path}")
    
    return 0

if __name__ == "__main__":
    raise SystemExit(main())