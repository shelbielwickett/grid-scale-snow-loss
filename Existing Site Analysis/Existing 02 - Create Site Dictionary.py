# Import all necessary packages
import pandas as pd
import json

# Entered Variables
# Choose a year of data from your downloaded NSIDC Data
year = 2022
tracking_type = ['All']
#electric_region = ['MISO']
electric_region = ['MISO','ISO-NE', 'NYISO', 'PJM','Southeast (non-ISO)', 'SPP']
state = ['All']
title = 'Eastern Interconnect'
analysis_title = f'{year} {title}'

# This function creates a site dictionary that includes the closest corresponding NSIDC datapoint latitudes and longitudes
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
        regions_to_keep = ['PJM', 'MISO', 'ISO-NE', 'NYISO', 'SPP', 'Southeast (non-ISO)']
        filtered_site_df = filtered_site_df[
            filtered_site_df['Region'].str.lower().isin([region.lower() for region in regions_to_keep])
        ]
    if 'All' in state:
        filtered_site_df = filtered_site_df
    else:
        filtered_site_df = filtered_site_df[filtered_site_df['State'].isin(state)]
        
    return filtered_site_df


# Load the site data
site_data = pd.read_csv('/Users/shelbiedavis1/Multi-State Simulation/Site Data/2024_utility-scale_solar_data_update.csv')
unfiltered_site_df = pd.DataFrame(site_data)
unfiltered_site_df = unfiltered_site_df.drop(unfiltered_site_df.columns[-19:], axis=1)
print(unfiltered_site_df['Solar COD Year'].dtype)

# Specifically filter out FL
# Get a list of all states in site_df, excluding Florida

# Filter site data
year_series = [year]

site_df = filter_site_df(tracking_type, year_series, electric_region, state, unfiltered_site_df)
site_df = site_df.reset_index(drop=True)

print(site_df)

# Display first 50 rows of the filtered DataFrame
print(site_df.iloc[:, 1:3])

# Create the site dictionary
site_dict = create_site_dictionary(year, site_df)
#print(site_dict)

# Output site metadata files

csv_output_path = f'/Users/shelbiedavis1/Multi-State Simulation/Existing Site Metadata Files/{year}_PV_existing_site_metadata.csv'
site_df.to_csv(csv_output_path, index=False)
print(f"Site info CSV saved to {csv_output_path}")


# Save the dictionary to a JSON file
json_file_path = f'/Users/shelbiedavis1/Multi-State Simulation/Project json files/{analysis_title} Analysis.json'
with open(json_file_path, 'w') as json_file:
    json.dump(site_dict, json_file, indent=4)

print(f"Site dictionary saved to {json_file_path}")
