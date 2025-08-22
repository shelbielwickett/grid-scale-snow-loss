#!/usr/bin/env python3
"""
Created on Tue Dec  3 22:22:21 2024
@author: shelbiedavis1
This code plots the growth of utility-scale PV in the US. 
Updated on 8/21/25 to use portable file paths for GitHub sharing.
"""

import geopandas as gpd
import matplotlib.pyplot as plt
import xarray as xr
#import rioxarray as rxr
import numpy as np
import pandas as pd
from matplotlib.colors import LogNorm, LinearSegmentedColormap
from matplotlib.ticker import ScalarFormatter
import json
from mpl_toolkits.axes_grid1 import make_axes_locatable
import os
from pathlib import Path

# === CHOOSE BASE DIRECTORY ===
BASE_DIR = Path("/Volumes/Wickett SSD/Snow_Loss_Project")

####################### Constants #############################
# load NERC region data
GEO_DIR = Path("Data/Geo Data/jurisdiction_nerc_subregion_v1")
geo_file_path = GEO_DIR / "jurisdiction_nerc_subregion_v1.gpkg"
SITE_DIR = Path("Data/Site Data")
existing_site_file_path = SITE_DIR / 'existing_site_data.csv'


####################### Define All Functions #############################
def mapping_parameters(geo_file_path):
    """Sets up mapping for plot."""
    # Load the GeoPackage
    gdf = gpd.read_file(geo_file_path)

    # Filter for desired regions
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

    # Dissolve polygons into a single geometry
    combined_geometry = filtered_gdf.geometry.union_all()

    # Extract the exterior boundary
    boundary = combined_geometry.boundary

    # Create a GeoSeries and set the CRS
    boundary_gs = gpd.GeoSeries([boundary], crs=filtered_gdf.crs)

    return boundary_gs

####################### Main Code #############################
df = pd.read_csv(existing_site_file_path)
print(df.iloc[:,2:5])
print(df.columns)
print(df['Region'].unique())
df['Region'] = df['Region'].str.strip()  # Remove extra spaces

region_names = [
    'ISO-NE',
    'MISO',
    'NYISO',
    'Southeast (non-ISO)',
    'PJM',
    'SPP'
]

filtered_df = df[df['Region'].isin(region_names)]
print(filtered_df)


####################### Create Plot #############################
fig, ax = plt.subplots(figsize=(12, 8))

boundary_gs = mapping_parameters(geo_file_path)

boundary_gs.plot(ax=ax, edgecolor='blue', color='gray', linewidth=0.5)

scatter = ax.scatter(
   filtered_df['Longitude'],
   filtered_df['Latitude'],
   c = filtered_df['Solar COD Year'],
   cmap='cividis_r',
   edgecolors='black',
   linewidth =.75,
   s=40,  # Size of the points
   alpha = .5
)

# Use make_axes_locatable to create and position the colorbars
divider = make_axes_locatable(ax)

# Add colorbar for scatter plot
cax2 = divider.append_axes("right", size="2%", pad=-0.75)  # Adjust pad for proper spacing
scatter_cbar = fig.colorbar(scatter, cax=cax2)
scatter_cbar.ax.yaxis.set_major_formatter(ScalarFormatter(useMathText=False))  # Format numbers
scatter_cbar.ax.tick_params(labelsize=16)  # Change tick label font size
scatter.set_clim(2009, 2023)
years = filtered_df['Solar COD Year']
scatter_cbar.set_ticks(years)

# Add axis labels and title
ax.set_xlabel("Longitude", fontsize = 16)
ax.set_ylabel("Latitude", fontsize = 16)
ax.tick_params(axis='y', labelsize=16)
ax.tick_params(axis='x', labelsize=16)
ax.yaxis.grid(True, linestyle='-', linewidth=0.5, alpha=0.5, zorder = 0)
ax.xaxis.grid(True, linestyle='-', linewidth=0.5, alpha=0.5, zorder = 0)
ax.set_axisbelow(True)  # Ensures grid lines are behind bars

####################### Print and Save Plot #############################
plt.tight_layout()
# Save the figure
fig.savefig(BASE_DIR / "Figures/Utility PV Growth", format='eps', bbox_inches="tight")
# Show the plot
plt.show()

