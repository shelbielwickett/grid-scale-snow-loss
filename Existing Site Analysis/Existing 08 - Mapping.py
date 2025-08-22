#!/usr/bin/env python3
"""
Created on Sun Dec  1 17:52:13 2024
@author: shelbiedavis1
This code plots the site snow loss and snow depth in the Eastern Interconnect during any specified hour. 
Updated on 8/21/25 to use portable file paths for GitHub sharing.
"""
####################### Library Imports #############################
import geopandas as gpd
import matplotlib.pyplot as plt
import xarray as xr
import rioxarray
import numpy as np
import pandas as pd
from matplotlib.colors import LogNorm, LinearSegmentedColormap
from matplotlib.ticker import ScalarFormatter
import json
from mpl_toolkits.axes_grid1 import make_axes_locatable
import os
import matplotlib.ticker as mticker
from pathlib import Path

####################### User Defined Constants #############################
start_time = '2022-01-31' #day before the maximum loss hour
end_time = '2022-02-01' #day of the maximum loss hour
timestamp = '2022-02-01 18:30:00+00:00' #exact timestamp of hour of interest
timestamp = pd.Timestamp(timestamp) #change timestamp into datetime format
#Do you want to include sites that were installed after the chosen hour?
sites_post_analysis_hour = True

#Identify if you want the map to show the new snow since previous day or the total snow depth on the ground.
#Choose 'New Snow' or 'Total Snow'
snow_metric = 'Total Snow'

# === CHOOSE BASE DIRECTORY ===
BASE_DIR = Path("/Volumes/Wickett SSD/Snow_Loss_Project")

####################### Other Constants #############################
# define analysis year
year = timestamp.year

# load NERC region data
GEO_DIR = Path("Data/Geo Data/jurisdiction_nerc_subregion_v1")
geo_file_path = GEO_DIR / "jurisdiction_nerc_subregion_v1.gpkg"

# load NSIDC snow data
NC_DIR = Path('Data/Snow Data')
nc_file_path = NC_DIR / f'4km_SWE_Depth_WY{year}_v01.nc'

# load json project file path
JSON_DIR = BASE_DIR / "Project json files"
json_project_file_path = JSON_DIR / f'{year} Eastern Interconnect Analysis.json'

####################### Define All Functions #############################
def read_production_data(path: Path, snow_scenario: str, site_data: dict, year: int):
    """Reads in production data produced through PySAM simulations."""
    SAM_data = pd.read_csv(
        path / f'PySAM_Results_UTC/Existing_Sites_Results_UTC/{snow_scenario}/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv'
        )
    # Convert 'Timestamp' to datetime and find selected timestamp
    SAM_data['UTC'] = pd.to_datetime(SAM_data['UTC'])
    hour_data = SAM_data[SAM_data['UTC'] == pd.Timestamp(timestamp)]
    #W to MW
    hour_data = hour_data['DC Inverter Input Power [W]']/1000000000
    return hour_data

def extract_float(val):
    """Flattens series."""
    if isinstance(val, pd.Series):
        return val.values[0] if val.values.size > 0 else np.nan
    return float(val) if pd.notna(val) else np.nan

def process_snow_depth(geo_file_path, nc_file_path, start_time, end_time, snow_metric):
    """Processes snow data and sets up mapping for plot."""
    # Load the GeoPackage
    gdf = gpd.read_file(geo_file_path)

    # Filter for desired NERC subregions
    desired_names = [
        'FLORIDA RELIABILITY COORDINATING COUNCIL (FRCC)',
        'NEW ENGLAND (NEWE)',
        'SOUTHEASTERN (SRSE)',
        'VIRGINIA-CAROLINA (SRVC)',
        'EAST (MROE)',
        'LONG ISLAND (NYLI)',
        'UPSTATE NEW YORK (NYUP)',
        'MICHIGAN (RFCM)',
        'EAST (RFCE)',
        'NYC - WESTCHESTER (NYCW)',
        'SOUTH (SPSO)',
        'WEST (RFCW)',
        'WEST (MROW)',
        'NORTH (SPNO)',
        'GATEWAY (SRGW)',
        'SOUTH (SPSO)',
        'DELTA (SRDA)',
        'CENTRAL (SRCE)',
        'EAST (MROE)'
    ]
    filtered_gdf = gdf[gdf['subname'].isin(desired_names)]

    # Ensure GeoDataFrame CRS matches the NetCDF file's CRS
    if filtered_gdf.crs != "EPSG:4326":
        filtered_gdf = filtered_gdf.to_crs("EPSG:4326")

    # dissolve polygons into a single geometry
    combined_geometry = filtered_gdf.geometry.union_all()

    # extract the exterior boundary
    boundary = combined_geometry.boundary

    # create a GeoSeries and set the CRS
    boundary_gs = gpd.GeoSeries([boundary], crs=filtered_gdf.crs)

    # load the NetCDF dataset
    nc_data = xr.open_dataset(nc_file_path)

    # select specific start and end times for the snow data
    depth_start = nc_data['DEPTH'].sel(time=start_time)
    depth_end = nc_data['DEPTH'].sel(time=end_time)

    # determine snow metric
    if snow_metric == 'New Snow':
        snow_depth_increase = (depth_end - depth_start) / 10  # Convert from mm to cm
    else:
        snow_depth_increase = depth_end/10

    # set all negative snow values to 0
    snow_depth_increase = snow_depth_increase.clip(min=0.001)

    # set spatial dimensions and CRS for NetCDF data
    snow_depth_increase = snow_depth_increase.rio.set_spatial_dims(x_dim="lon", y_dim="lat")
    snow_depth_increase = snow_depth_increase.rio.write_crs("EPSG:4326")  # Ensure CRS is set

    # clip the NetCDF data to the shapefile area
    clipped_depth = snow_depth_increase.rio.clip(filtered_gdf.geometry, filtered_gdf.crs)

    # Define a custom colormap
    colors = [
        (1, 0.9, 0.9),
        (0.5, 0.5, 1),
        (0, 0, 0.2)
    ]
    custom_cmap = LinearSegmentedColormap.from_list("CustomMap", colors, N=256)
    
    return clipped_depth, boundary_gs, custom_cmap

####################### Main Code #############################

with open(json_project_file_path) as f:
    site_dict = json.load(f)

no_snow_gen_values_list = []
snow_gen_values_list = []
mount_type_list = []
lat_list = []
lon_list = []
names_list = []
cod_list = []
gen_loss_list = []
hour_no_snow_sum_series= pd.Series(dtype=float)
hour_snow_sum_series = pd.Series(dtype=float)

for i, site_key in enumerate(site_dict):
    #skip sites with no snow data
    if site_dict[site_key]['Project Name'] == 'FPL Space Coast Next Generation Solar Energy Center':
        continue  
    if site_dict[site_key]['Project Name'] == 'Monroe County Sites C  D  & E':
        continue
    if site_dict[site_key]['Project Name'] == 'Old Bridge Solar Farm':
        continue
    if site_dict[site_key]['Project Name'] == 'IFF Union Beach Solar Project':
        continue
    if site_dict[site_key]['Project Name'] == 'Trask East Solar':
        continue
    if site_dict[site_key]['Project Name'] == 'FPL Discovery Solar Center':
        continue
    if site_dict[site_key]['Project Name'] == 'Tank Farm 4':
        continue
    if site_dict[site_key]['Project Name'] == 'Lumberton Solar':
        continue
    if site_dict[site_key]['Project Name'] == 'Live Oak Solar':
        continue
    
    site_data = site_dict[site_key]

    hour_no_snow = read_production_data(
        BASE_DIR, 
        'No_Snow', 
        site_data,
        year
        )
    no_snow_gen_values_list.append(hour_no_snow)
    
    hour_snow = read_production_data(
        BASE_DIR, 
        'Roof_Slide_Coeff', 
        site_data,
        year
        )
    snow_gen_values_list.append(hour_snow)

    # Get mount type
    mount_type = site_data['Mount']
    mount_type_list.append(mount_type)
    
    #get lat
    lat = site_data['Latitude']
    lat_list.append(lat)
    
    #get lon
    lon = site_data['Longitude']
    lon_list.append(lon)
    
    #get project names
    names = site_data['Project Name']
    names_list.append(names)
    
    #get Solar COD
    cod = site_data['Solar COD']
    cod_list.append(cod)
    
    #find gen loss
    gen_loss = (hour_no_snow-hour_snow)/hour_no_snow*100
    
    gen_loss_list.append(gen_loss)
    
    hour_no_snow_sum_series = pd.concat([hour_no_snow_sum_series, pd.Series([hour_no_snow])], ignore_index=True)
    hour_snow_sum_series= pd.concat([hour_snow_sum_series, pd.Series([hour_snow])], ignore_index=True)


# Creating the DataFrame
existing_sites_data = pd.DataFrame({
    'project_names': names_list,
    'no_snow_gen_values': no_snow_gen_values_list,
    'snow_gen_values': snow_gen_values_list,
    'mount_type': mount_type_list,
    'latitude': lat_list,
    'longitude': lon_list,
    'Solar COD': cod_list,
    'gen_loss': gen_loss_list
})

# Apply to existing sites
existing_sites_data['gen_loss'] = existing_sites_data['gen_loss'].apply(extract_float)
previous_sites = existing_sites_data[existing_sites_data['Solar COD'] <= end_time]
current_sites = existing_sites_data[existing_sites_data['Solar COD'] > end_time]
print(current_sites['gen_loss'])

####################### Create Plot #############################

fig, ax = plt.subplots(figsize=(12, 8))

clipped_depth, boundary_gs, custom_cmap = process_snow_depth(
    geo_file_path=geo_file_path,
    nc_file_path=nc_file_path,
    start_time="2022-01-01",
    end_time="2022-01-02",
    snow_metric=snow_metric
)

# Plot the exterior boundary
boundary_gs.plot(ax=ax, edgecolor='grey', linewidth=0.5, zorder = 1)

# Overlay the clipped snow depth data
lat = clipped_depth['lat'].values
lon = clipped_depth['lon'].values
mesh = ax.pcolormesh(
    lon,
    lat,
    clipped_depth,
    cmap=custom_cmap,
    norm=LogNorm(vmin=0.1, vmax=np.nanmax(clipped_depth)),
    shading='auto',
    alpha=0.6,
    zorder = 5
)

# Overlay the data points pre-start_time
scatter = ax.scatter(
    previous_sites['longitude'],
    previous_sites['latitude'],
    c = previous_sites['gen_loss'],
    cmap= 'winter_r',
    edgecolors='black',  # Color of the borders
    linewidth=0.5,        # Thickness of the borders
    s=15,  # Size of the points
    alpha = 1,
    zorder = 5
)
value = 'before'
text = f'Existing Sites ({year})'

if sites_post_analysis_hour == True:
    #Overlay the data points post-start_time
    scatter = ax.scatter(
        current_sites['longitude'],
        current_sites['latitude'],
        c = current_sites['gen_loss'],
        cmap='winter_r',
        edgecolors='black',  # Color of the borders
        linewidth=0.5,        # Thickness of the borders
        s=15,  # Size of the points
        alpha = 1,
        zorder = 5
    )
    value = 'after'
    text = 'Existing Sites (2022)'


# Use make_axes_locatable to create and position the colorbars
divider = make_axes_locatable(ax)

# Add colorbar for snow depth
cax1 = divider.append_axes("right", size="2%", 
#                           pad = -.2
                           pad=-.08
                           )  # Adjust pad for spacing
cbar = fig.colorbar(mesh, cax=cax1)
cbar.mappable.set_clim(vmin=.1, vmax=10)
# Move label & ticks to the left
cbar.ax.yaxis.set_label_position('left')  # Move label to the left
cbar.ax.yaxis.tick_left()  # Move ticks to the left
cbar.ax.yaxis.set_label_coords(-1.75, 0.5)  # Adjust x, y position

cbar.set_label(f'{snow_metric} (cm)', 
#               fontsize=16
                fontsize = 26
               )  

cbar.ax.yaxis.set_major_formatter(ScalarFormatter(useMathText=False))  # Format numbers
cbar.ax.tick_params(
#    labelsize=16
    labelsize = 26
    )

# Force whole number ticks
cbar.ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
cbar.set_ticks([0.1, 1, 10])  # Set .1, 1, and 10 as ticks
cbar.ax.set_yticklabels(["0", "1", "10+"])  # Label Ticks

# Add colorbar for scatter plot
cax2 = divider.append_axes("right", size="2%", pad=.25)  # Adjust pad for proper spacing
scatter_cbar = fig.colorbar(scatter, cax=cax2)
scatter_cbar.set_label('Loss [%] of Power', 
#                       fontsize=16
                       fontsize = 26
                       )  
scatter_cbar.ax.yaxis.set_major_formatter(ScalarFormatter(useMathText=False))  # Format numbers
scatter_cbar.ax.tick_params(
#    labelsize=16
    labelsize = 26
    ) 

# Add axis labels and title
ax.set_xlabel("Longitude", 
#              fontsize = 16
              fontsize = 26
              )
ax.set_ylabel("Latitude", 
#              fontsize = 16
              fontsize = 26
              )
ax.tick_params(axis='x', 
#               labelsize=16
               labelsize = 26
               )  
ax.tick_params(axis='y', 
#               labelsize=16
               labelsize = 26
               )  
ax.yaxis.grid(True, linestyle='-', linewidth=0.5, alpha=0.5)
ax.xaxis.grid(True, linestyle='-', linewidth=0.5, alpha=0.5)
ax.set_axisbelow(True)  # Ensures grid lines are behind bars
ax.set_ylim(24,50)
ax.set_xlim(-109,-65)

formatted = timestamp.strftime('%m/%d/%y %H:%M')

text_str = f'{formatted}'
plt.text(
#    0.75, 0.9,  # x and y in figure-relative coordinates
    0.6, 0.92,
    text_str,
#    fontsize=20,
    fontsize = 26,
    bbox=dict(facecolor='white', alpha=0.5, 
              edgecolor= 'none'
              ),  
    ha='left',  # Align text to the left
    va='bottom',  # Align text to the bottom
    transform=ax.transAxes  # Ensures text stays in bottom-left even if limits change
)

####################### Print and Save Plot #############################
plt.tight_layout()
# Show the plot
fig.savefig(BASE_DIR / f"Figures/Production and Snow Map.pdf", format='pdf', bbox_inches='tight')
plt.show()


