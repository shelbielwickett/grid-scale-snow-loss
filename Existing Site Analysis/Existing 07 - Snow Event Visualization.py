#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 16 22:06:29 2024

@author: shelbiedavis1
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker

#max 2022
#start_date = '2022-01-14 00:30:00' #date and time in yyyy-mm-dd hh:mm:ss
#end_date = '2022-01-23 23:00:00' #date and time in yyyy-mm-dd hh:mm:ss
#max 2018
start_date = '2014-01-10 00:30:00-05:00' #date and time in yyyy-mm-dd hh:mm:ss
end_date = '2014-01-12 23:00:00-05:00' #date and time in yyyy-mm-dd hh:mm:ss
#site_name = 'Indianapolis Motor Speedway (IMS) Solar Farm' #name of solar site
site_name = 'Apple Data Center- PV1'
#site_name = 'Gulf Coast Solar Center 2 (NAS Whiting Field  NOLF Holley)'
not_covered = 'existing_sites_no_snow_model_results'
covered = 'existing_sites_snow_model_results'
batch = 'Existing'

# Create a date range with daily frequency
date_list = pd.date_range(start=start_date, end=end_date, freq='D')
print(date_list)

# Format the dates as 'm/d/yy'
formatted_dates = [date.strftime('%-m/%-d/%y') for date in date_list]


start_date = pd.to_datetime(start_date)
end_date = pd.to_datetime(end_date)
year = start_date.year
file_name = site_name + '_' + str(year) +'_Results.csv'
file_path_no_snow = f'/Users/shelbiedavis1/Multi-State Simulation/PySAM_Results/Existing_Sites_Results_Full_UTC/No_Snow/{year} SAM Results/Apple Data Center- PV1_{year}_Results.csv'
date_range = [start_date, end_date]

df_no_snow = pd.read_csv(file_path_no_snow)
print(df_no_snow.iloc[:,:3])

file_path_snow = f'/Users/shelbiedavis1/Multi-State Simulation/PySAM_Results/Existing_Sites_Results_Full_UTC/Roof_Slide_Coeff/{year} SAM Results/Apple Data Center- PV1_{year}_Results.csv'

df_snow = pd.read_csv(file_path_snow)
print(df_snow.head(50))

# Ensure the Datetime column in both DataFrames is in datetime format
df_no_snow['Local Datetime'] = pd.to_datetime(df_no_snow['Local Datetime'])
df_snow['Local Datetime'] = pd.to_datetime(df_snow['Local Datetime'])

# Filter the dataframes by the given datetime range
df_no_snow_filtered = df_no_snow[(df_no_snow['Local Datetime'] >= start_date) & (df_no_snow['Local Datetime'] <= end_date)]
df_snow_filtered = df_snow[(df_snow['Local Datetime'] >= start_date) & (df_snow['Local Datetime'] <= end_date)]
print(df_snow_filtered.head(50))

# Determine Mount Type
if batch == 'Existing':
    site_data = pd.read_csv('/Users/shelbiedavis1/Multi-State Simulation/Site Data/2023_utility-scale_solar_data_update.csv')
    #site_data_df = pd.to_dataframe(site_data)
    # Find the mounting type using .loc
    mounting_type = site_data.loc[site_data['Project Name'] == site_name, 'Mount'].iloc[0]
    state = site_data.loc[site_data['Project Name'] == site_name, 'State'].iloc[0]
    print(mounting_type)
else:
    site_data = pd.read_csv('/Users/shelbiedavis1/Multi-State Simulation/Site Data/queues_2023_clean_data_r1.csv')


import matplotlib.pyplot as plt
import pandas as pd

# Create a figure with two subplots stacked vertically
fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(14, 12), sharex=True)

# Plot 1: DC Input Power and Weather File Snow Depth
ax1 = axes[0]
ax1.plot(df_no_snow_filtered['Local Datetime'], df_no_snow_filtered['DC Inverter Input Power [W]']/1000,
         label='No Snow Model: DC Input Power [kW]', color='orange', linestyle='-')
ax1.plot(df_snow_filtered['Local Datetime'], df_snow_filtered['DC Inverter Input Power [W]']/1000,
         label='With Snow Model: DC Input Power [kW]', color='blue', linestyle='--')

ax1.set_ylabel('DC Inverter Input Power [kW]', fontsize=14)
ax1.legend(loc='upper left', fontsize=11)
ax1.tick_params(axis='y', labelsize=14)
ax1.set_ylim(-30)

# Secondary y-axis for Weather File Snow Depth
ax2 = ax1.twinx()
ax2.plot(df_snow_filtered['Local Datetime'], df_snow_filtered['Weather File Snow Depth'],
         label='Weather File Snow Depth', color='black', linestyle='-.')

# Set the y-axis label
ax2.set_ylabel('Snow Depth [cm]', fontsize=14)

# Set the y-axis range to start at 0 and end at 15
ax2.set_ylim(0, 30)

# Use whole numbers for the y-axis ticks
ax2.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))

# Set tick parameters
ax2.tick_params(axis='y', labelsize=12, colors='black')

# Add the legend
ax2.legend(loc='upper right', fontsize=11)


ax1.set_title(f'{site_name}\n{mounting_type} Array in {state}\nDC Input Power and Snow Depth Over Time', fontsize=14)

# Add a vertical line to Plot 1
#vline_datetime1 = pd.Timestamp('2022-01-22 14:30:00')
#ax1.axvline(x=vline_datetime1, color='red', linestyle='--')

# Grid
ax1.grid()

# Plot 2: Plane of Array Irradiance and Ambient Temperature
ax3 = axes[1]
ax3.plot(df_no_snow_filtered['Local Datetime'], df_no_snow_filtered['poa'],
         label='Plane of Array Irradiance [W/m^2]', color='green', linestyle='-')

ax3.set_ylabel('Irradiance [W/m^2]', fontsize=14)
ax3.legend(loc='upper left', fontsize=11)
ax3.tick_params(axis='y', labelsize=14)

ax3.set_ylim(0, 1000)

# Secondary y-axis for Ambient Temperature
ax4 = ax3.twinx()
ax4.plot(df_snow_filtered['Local Datetime'], df_snow_filtered['tamb'],
         label='Ambient Temperature [C]', color='teal', linestyle='-.')
ax4.set_ylabel('Temperature [C]', fontsize=14)
ax4.tick_params(axis='y', labelsize=12, colors='black')
ax4.legend(loc='upper right', fontsize=11)

ax4.set_ylim(-30, 10)

# Add a vertical line to Plot 2
#vline_datetime2 = pd.Timestamp('2022-01-22 14:30:00')
#ax3.axvline(x=vline_datetime2, color='red', linestyle='--')

ax3.set_title(f'{site_name}\n{mounting_type} Array in {state}\nPlane of Array Irradiance and Temperature Over Time', fontsize=14)

# Grid
ax3.grid()

# Set shared x-axis label
#axes[-1].set_xlabel('Date', fontsize=14)

# Format x-axis with dates
# Create a date formatter
date_formatter = mdates.DateFormatter('%-m/%-d/%y')

# Apply the date formatter and locator to each axis
# for ax in axes:
#     ax.xaxis.set_major_locator(mdates.DayLocator())  # Set major ticks to daily intervals
#     ax.xaxis.set_major_formatter(date_formatter)
#     ax.tick_params(axis='x', rotation=45, labelsize=14)

plt.tight_layout()

# Save the figure
#fig.savefig(f"Figures/Sites/{site_name}_{start_date}_to_{end_date}_stacked.png", dpi=300, bbox_inches="tight")

# Show the plot
plt.show()