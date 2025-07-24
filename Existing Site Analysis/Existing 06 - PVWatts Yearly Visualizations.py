#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 15:20:52 2024

@author: shelbiedavis1
"""
# %% [1] Import Packages
import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# %% [2] User-Defined Data
# Enter Analysis Start and End Year
start_year = 2013
end_year = 2022
#title = 'MISO'
title = 'Eastern Interconnect'
months = [12, 1, 2] #winter
#months = [3,4,5] #spring
#months = [1,2,3,4,5,6,7,8,9,10,11,12]
timeframe = 'winter'
lat_min = 0

# %% [3] Data Processing

df = pd.DataFrame(columns=[])
    

# Dictionary to store DataFrames for each year
yearly_dfs = {}

for year in range(start_year, end_year + 1):
    total_no_snow_dc_power_list = []
    total_snow_dc_power_list = []
    mount_type_list = []

    file = f"{year} {title} Analysis"
    with open('/Users/shelbiedavis1/Multi-State Simulation/Project json files/' + file + '.json') as f:
        site_dict = json.load(f)
    

    for i, site_key in enumerate(site_dict):
        
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
        
        if site_dict[site_key]['Latitude']>= lat_min:
            site_data = site_dict[site_key]
        
      
        else:
            continue
    
        #site_data = site_dict[site_key]
    
    
        # Load No Snow data using the UTC datetime index
        SAM_no_snow = pd.read_csv(
           f'/Volumes/Wickett SSD/PySAM_Results/Existing_Sites_Results/No_Snow/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv',
           index_col=0,
           parse_dates=True
           )
        SAM_no_snow.index.name = 'UTC'

        # Filter directly using the index (which is in UTC)
        SAM_no_snow = SAM_no_snow[SAM_no_snow.index.month.isin(months)]

        # Aggregate DC input power
        total_no_snow_dc_power = SAM_no_snow['DC Inverter Input Power [W]'].sum()
        total_no_snow_dc_power_list.append(total_no_snow_dc_power)
        
        # Load Snow data
        SAM_snow = pd.read_csv(
            f'/Volumes/Wickett SSD/PySAM_Results/Existing_Sites_Results/Roof_Slide_Coeff/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv',
            index_col=0,
            parse_dates=True
            )
        SAM_snow.index.name = 'UTC'
        SAM_snow = SAM_snow[SAM_snow.index.month.isin(months)]

        total_snow_dc_power = SAM_snow['DC Inverter Input Power [W]'].sum()
        total_snow_dc_power_list.append(total_snow_dc_power)

        # Get mount type
        mount_type = site_data['Mount']
        mount_type_list.append(mount_type)

    
    # Create a DataFrame for the current year
    df_year = pd.DataFrame({
        f'{year} No Snow Total DC Inverter Input Power [W]': total_no_snow_dc_power_list,
        f'{year} Snow Total DC Inverter Input Power [W]': total_snow_dc_power_list,
        f'{year} Total Snow Loss [W]': [
            no_snow - snow for no_snow, snow in zip(total_no_snow_dc_power_list, total_snow_dc_power_list)
        ],
        f'{year} Site Snow Loss [%]': [
            ((no_snow - snow) / no_snow) * 100 if no_snow != 0 else 0
            for no_snow, snow in zip(total_no_snow_dc_power_list, total_snow_dc_power_list)
        ],
        f'{year} Mount': mount_type_list
    })

    # Add the DataFrame to the dictionary
    yearly_dfs[year] = df_year

print(yearly_dfs)


    
# Simulate the structure of yearly_dfs dictionary for demonstration
# yearly_dfs = {
#     2012: {
#         '2012 No Snow Total DC Inverter Input Power [W]': [100, 200, 300],
#         '2012 Snow Total DC Inverter Input Power [W]': [90, 180, 270],
#         '2012 Mount': ['tracking', 'fixed', 'tracking']
#     },
#     2013: {
#         '2013 No Snow Total DC Inverter Input Power [W]': [110, 220, 330],
#         '2013 Snow Total DC Inverter Input Power [W]': [100, 200, 310],
#         '2013 Mount': ['fixed', 'tracking', 'fixed']
#     }
# }

# Initialize data structures for the bar chart
years = sorted(yearly_dfs.keys())
categories = [
    
    
    "Tracking Sites without Snow",
    "Tracking Sites with Snow",
    "Tracking Total Loss",
    "Fixed Sites without Snow",
    "Fixed Sites with Snow",
    "Fixed Total Loss",
    "All Sites without Snow",
    "All Sites with Snow",
    "Total Loss",
]

data = {category: [] for category in categories}

# Aggregate data for each year and category
for year in years:
    df = yearly_dfs[year]
    snow = df[f'{year} Snow Total DC Inverter Input Power [W]']
    no_snow = df[f'{year} No Snow Total DC Inverter Input Power [W]']
    mounts = df[f'{year} Mount']
    
    # Initialize totals for the year
    tracking_snow, tracking_no_snow = 0, 0
    fixed_snow, fixed_no_snow = 0, 0
    
    # Aggregate based on mount type
    for mount, s, ns in zip(mounts, snow, no_snow):
        if mount == 'Tracking':
            tracking_snow += s
            tracking_no_snow += ns
        elif mount == 'Fixed Tilt':
            fixed_snow += s
            fixed_no_snow += ns
    if tracking_no_snow != 0:
        tracking_loss = (tracking_no_snow-tracking_snow)/tracking_no_snow*100
    else:
        tracking_loss = 0
    if fixed_no_snow != 0:
        fixed_loss = (fixed_no_snow-fixed_snow)/fixed_no_snow*100
    else:
        fixed_loss = 0
    if df[f'{year} No Snow Total DC Inverter Input Power [W]'].sum() != 0:
        total_loss = df[f'{year} Total Snow Loss [W]'].sum()/df[f'{year} No Snow Total DC Inverter Input Power [W]'].sum()*100
    else:
        total_loss = 0
    print(f'Total Loss: {total_loss}')
    
    print(tracking_loss)
    print(fixed_loss)    
    # Populate data for the bar chart
    data["Tracking Sites with Snow"].append(tracking_snow/1000000000000)
    data["Tracking Sites without Snow"].append(tracking_no_snow/1000000000000)
    data["Tracking Total Loss"].append(tracking_loss)
    data["Fixed Sites with Snow"].append(fixed_snow/1000000000000)
    data["Fixed Sites without Snow"].append(fixed_no_snow/1000000000000)
    data["Fixed Total Loss"].append(fixed_loss)
    data["All Sites with Snow"].append(sum(snow)/1000000000000)
    data["All Sites without Snow"].append(sum(no_snow)/1000000000000)
    data["Total Loss"].append(total_loss)

# %% [4] Bar Chart (not stacked)
# # Create the bar chart
# x = np.arange(len(years))



# # Define colors for each category
# colors = {

#     "Tracking Sites with Snow": "#66D0B9",
#     "Tracking Sites without Snow": "#009E73",
#     "Fixed Sites with Snow": "#A8DDF5",
#     "Fixed Sites without Snow": "#56B4E9",
#     "All Sites with Snow": "#FFCD66",
#     "All Sites without Snow": "#E69F00"
    
# }

# # Adjust bar positions and plot with colors
# fig, ax = plt.subplots(figsize=(14, 6))

# # Define a consistent spacing for bars
# width = 0.15  # Width of the bars
# spacing = 0.15  # Spacing between bars

# # Adjusted offsets
# all_offsets = [-2 * spacing, -spacing]
# tracking_offsets = [0, spacing]
# fixed_offsets = [2 * spacing, 3 * spacing]

# # Plot bars
# all_without_snow_bars = ax.bar(
#     x + all_offsets[0], 
#     data["All Sites without Snow"], 
#     width, 
#     label="All Sites without Snow", 
#     color=colors["All Sites without Snow"], 
#     alpha=1
# )
# all_with_snow_bars = ax.bar(
#     x + all_offsets[1], 
#     data["All Sites with Snow"], 
#     width, 
#     label="All Sites with Snow", 
#     color=colors["All Sites with Snow"], 
#     alpha=1, 
#     hatch='\\'
# )
# tracking_without_snow_bars = ax.bar(
#     x + tracking_offsets[0], 
#     data["Tracking Sites without Snow"], 
#     width, 
#     label="Tracking Sites without Snow", 
#     color=colors["Tracking Sites without Snow"]
# )
# tracking_with_snow_bars = ax.bar(
#     x + tracking_offsets[1], 
#     data["Tracking Sites with Snow"], 
#     width, 
#     label="Tracking Sites with Snow", 
#     color=colors["Tracking Sites with Snow"], 
#     hatch='\\'
# )
# fixed_without_snow_bars = ax.bar(
#     x + fixed_offsets[0], 
#     data["Fixed Sites without Snow"], 
#     width, 
#     label="Fixed Sites without Snow", 
#     color=colors["Fixed Sites without Snow"]
# )
# fixed_with_snow_bars = ax.bar(
#     x + fixed_offsets[1], 
#     data["Fixed Sites with Snow"], 
#     width, 
#     label="Fixed Sites with Snow", 
#     color=colors["Fixed Sites with Snow"], 
#     hatch='\\'
# )





# # Add text above "Fixed Sites" bars for the loss
# for i, (bar_without_snow, bar_with_snow) in enumerate(zip(fixed_without_snow_bars, fixed_with_snow_bars)):
#     if data["Fixed Sites without Snow"][i] != 0:        
#         loss = data["Fixed Total Loss"][i]
#         x_position = bar_without_snow.get_x() + bar_without_snow.get_width() / 1
#         y_position = bar_without_snow.get_height() +.5  # annual
#         #y_position = bar_without_snow.get_height() +.05  # winter
#         #y_position = bar_without_snow.get_height() +.005  # 40 lat
#         ax.text(x_position, y_position, f'{loss:.1f}%', ha='center', va='bottom', fontsize=9, color='black')
    
#         # # Draw Brackets
#         # x_center = x[i] + fixed_offsets[1] # Centered over the blue bar
#         # bracket_bottom = bar_with_snow.get_height()  # Bottom of the bracket starts at the blue bar height
#         # bracket_top = bar_without_snow.get_height()  # Top of the bracket ends at the orange bar height
#         # mid_bracket = (bracket_bottom + bracket_top) / 2  # Midpoint for the text label
#         # total_loss = bracket_top-bracket_bottom
#         # total_percent_loss = (bracket_top-bracket_bottom)/bracket_top*100
        

#         # # Draw the vertical bracket
#         # ax.plot([x_center, x_center], [bracket_bottom, bracket_top], color='black', lw=1.5)
#         # ax.plot([x_center - 0.05, x_center + 0.05], [bracket_top, bracket_top], color='black', lw=1.5)
#         # ax.plot([x_center - 0.05, x_center + 0.05], [bracket_bottom, bracket_bottom], color='black', lw=1.5)
#     else:
#         continue
    
# # Add text above "Tracking Sites" bars for the loss
# for i, (bar_without_snow, bar_with_snow) in enumerate(zip(tracking_without_snow_bars, tracking_with_snow_bars)):
#     if data["Tracking Sites without Snow"][i] != 0:
#         loss = data["Tracking Total Loss"][i]
#         x_position = bar_without_snow.get_x() + bar_without_snow.get_width() / 1
#         y_position = bar_without_snow.get_height() +.5  # annual
#         #y_position = bar_without_snow.get_height() +.05  # winter
#         #y_position = bar_without_snow.get_height() +.005  # 40 lat
#         ax.text(x_position, y_position, f'{loss:.1f}%', ha='center', va='bottom', fontsize=9, color='black')
        
#         # # Draw Brackets
#         # x_center = x[i] + tracking_offsets[1] # Centered over the blue bar
#         # bracket_bottom = bar_with_snow.get_height()  # Bottom of the bracket starts at the blue bar height
#         # bracket_top = bar_without_snow.get_height()  # Top of the bracket ends at the orange bar height
#         # mid_bracket = (bracket_bottom + bracket_top) / 2  # Midpoint for the text label
#         # total_loss = bracket_top-bracket_bottom
#         # total_percent_loss = (bracket_top-bracket_bottom)/bracket_top*100
        

#         # # Draw the vertical bracket
#         # ax.plot([x_center, x_center], [bracket_bottom, bracket_top], color='black', lw=1.5)
#         # ax.plot([x_center - 0.05, x_center + 0.05], [bracket_top, bracket_top], color='black', lw=1.5)
#         # ax.plot([x_center - 0.05, x_center + 0.05], [bracket_bottom, bracket_bottom], color='black', lw=1.5)
#     else:
#         continue

# # Add text above "All Sites" bars for the loss
# for i, (bar_without_snow, bar_with_snow) in enumerate(zip(all_without_snow_bars, all_with_snow_bars)):
#     if data["All Sites without Snow"][i] != 0:
#         loss = data["Total Loss"][i]
#         x_position = bar_without_snow.get_x() + bar_without_snow.get_width() / 1
#         y_position = bar_without_snow.get_height() +.5  # annual
#         #y_position = bar_without_snow.get_height() +.05  # winter
#         #y_position = bar_without_snow.get_height() +.005  # 40 lat
#         ax.text(x_position, y_position, f'{loss:.1f}%', ha='center', va='bottom', fontsize=9, color='black')
        
#         # # Draw Brackets
#         # x_center = x[i] + all_offsets[1] # Centered over the blue bar
#         # bracket_bottom = bar_with_snow.get_height()  # Bottom of the bracket starts at the blue bar height
#         # bracket_top = bar_without_snow.get_height()  # Top of the bracket ends at the orange bar height
#         # mid_bracket = (bracket_bottom + bracket_top) / 2  # Midpoint for the text label
#         # total_loss = bracket_top-bracket_bottom
#         # total_percent_loss = (bracket_top-bracket_bottom)/bracket_top*100
        

#         # # Draw the vertical bracket
#         # ax.plot([x_center, x_center], [bracket_bottom, bracket_top], color='black', lw=1.5)
#         # ax.plot([x_center - 0.05, x_center + 0.05], [bracket_top, bracket_top], color='black', lw=1.5)
#         # ax.plot([x_center - 0.05, x_center + 0.05], [bracket_bottom, bracket_bottom], color='black', lw=1.5)
    
#     else:
#         continue



# # Formatting the chart
# ax.set_xlabel('Year', fontsize = 14)
# ax.set_ylabel('DC Energy [TWh]', fontsize = 14)
# #ax.set_title('DC Energy Generation of Existing Sites\nAbove 40 Degrees Latitude\nDecember, January, and February', fontsize = 20)
# ax.set_xticks(x)
# ax.set_xticklabels(years, fontsize = 14)
# # Assuming you have an Axes object named ax
# ax.tick_params(axis='y', labelsize=12)
# # Set x-axis limits to match the range of years
# plt.xlim(-.5, 9.6)
# plt.ylim(0,60) #annual
# #plt.ylim(0,12) #winter
# #plt.ylim(0,1.4) #40 lat
# ax.legend(fontsize = 14)
# plt.xticks(rotation=45)
# ax.yaxis.grid(True, linestyle='-', linewidth=0.5, alpha=0.5, zorder=0)
# ax.set_axisbelow(True)  # Ensures grid lines are behind bars

# plt.tight_layout()
# # Save the figure
# fig.savefig(f"Figures/existing_site_{timeframe}_energy_generation_above_{lat_min}_not_stacked.png", dpi=300, bbox_inches="tight")
# plt.show()

# %% [5] Bar Chart (stacked)
# Create the bar chart
x = np.arange(len(years))



# Define colors for each category
colors = {

    "Tracking Sites with Snow": "#66D0B9",
    "Tracking Sites without Snow": "#009E73",
    "Fixed Sites with Snow": "#A8DDF5",
    "Fixed Sites without Snow": "#56B4E9",
    "All Sites with Snow": "#FFCD66",
    "All Sites without Snow": "#E69F00"
    
}

# Adjust bar positions and plot with colors
fig, ax = plt.subplots(figsize=(14, 6))

# Define a consistent spacing for bars
width = 0.15  # Width of the bars
spacing = 0.15  # Spacing between bars

# Adjusted offsets
all_offsets = [-1.5 * spacing, -.5*spacing]
other_offsets = [.5 * spacing, 1.5 * spacing]




# Plot bars
all_without_snow_bars = ax.bar(
    x + all_offsets[0], 
    data["All Sites without Snow"], 
    width, 
    label="All Sites without Snow", 
    color=colors["All Sites without Snow"], 
    alpha=1,
    zorder=1
)
all_with_snow_bars = ax.bar(
    x + all_offsets[1], 
    data["All Sites with Snow"], 
    width, 
    label="All Sites with Snow", 
    color=colors["All Sites with Snow"], 
    alpha=1, 
    hatch='\\',
    zorder=1
)
tracking_without_snow_bars = ax.bar(
    x + other_offsets[0], 
    data["Tracking Sites without Snow"], 
    width, 
    label="Tracking Sites without Snow", 
    color=colors["Tracking Sites without Snow"],
    zorder=1
)
tracking_with_snow_bars = ax.bar(
    x + other_offsets[1], 
    data["Tracking Sites with Snow"], 
    width, 
    label="Tracking Sites with Snow", 
    color=colors["Tracking Sites with Snow"], 
    hatch='\\',
    zorder=1
)
fixed_without_snow_bars = ax.bar(
    x + other_offsets[0], 
    data["Fixed Sites without Snow"], 
    width, 
    label="Fixed Sites without Snow", 
    color=colors["Fixed Sites without Snow"],
    bottom = data['Tracking Sites without Snow'],
    zorder=1
)
fixed_with_snow_bars = ax.bar(
    x + other_offsets[1], 
    data["Fixed Sites with Snow"], 
    width, 
    label="Fixed Sites with Snow", 
    color=colors["Fixed Sites with Snow"], 
    hatch='\\',
    bottom = data['Tracking Sites with Snow'],
    zorder=1
)





# Add text above "Fixed Sites" bars for the loss
for i, (bar_without_snow, bar_with_snow) in enumerate(zip(fixed_without_snow_bars, fixed_with_snow_bars)):
    if data["Fixed Sites without Snow"][i] != 0:        
        loss = data["Fixed Total Loss"][i]
        x_position = x[i]
        #y_position = data['All Sites without Snow'][i] + .50  # annual
        y_position = data['All Sites without Snow'][i] +.1  # winter
        #y_position = data['All Sites without Snow'][i] +.021  # for above 40 degrees
        ax.text(x_position, y_position, f'Fixed:\n{loss:.1f}%', ha='center', va='bottom', fontsize=13, color='#56B4E9', 
                fontweight='bold'
                )
    else:
        continue
    
# Add text above "Tracking Sites" bars for the loss
for i, (bar_without_snow, bar_with_snow) in enumerate(zip(tracking_without_snow_bars, tracking_with_snow_bars)):
    if data["Tracking Sites without Snow"][i] != 0:
        loss = data["Tracking Total Loss"][i]
        x_position = x[i]
        #y_position = data['All Sites without Snow'][i] + 2.5  # annual
        y_position = data['All Sites without Snow'][i] + 1.25  # for winter
        #y_position = data['All Sites without Snow'][i] + .17  # for above 40 degrees
        ax.text(x_position, y_position, f'Tracking:\n{loss:.1f}%', ha='center', va='bottom', fontsize=13, color='#009E73', 
                fontweight='bold'
                )
    else:
        continue

# Add text above "All Sites" bars for the loss
for i, (bar_without_snow, bar_with_snow) in enumerate(zip(all_without_snow_bars, all_with_snow_bars)):
    if data["All Sites without Snow"][i] != 0:
        loss = data["Total Loss"][i]
        x_position = x[i]
        #y_position = bar_without_snow.get_height() + 4.5 # annual
        y_position = bar_without_snow.get_height() + 2.4 # for winter
        #y_position = bar_without_snow.get_height() + .32  # for above 40 degrees
        ax.text(x_position, y_position, f'Losses\nTotal:\n{loss:.1f}%', ha='center', va='bottom', fontsize=13, color='black', 
                fontweight='bold'
                )
    else:
        continue

# Add vertical brackets to label differences for total loss
for i, (bar_without_snow, bar_with_snow) in enumerate(zip(all_without_snow_bars, all_with_snow_bars)):
    x_center = x[i] + all_offsets[1] # Centered over the blue bar
    bracket_bottom = bar_with_snow.get_height()  # Bottom of the bracket starts at the blue bar height
    bracket_top = bar_without_snow.get_height()  # Top of the bracket ends at the orange bar height
    mid_bracket = (bracket_bottom + bracket_top) / 2  # Midpoint for the text label
    total_loss = bracket_top-bracket_bottom
    total_percent_loss = (bracket_top-bracket_bottom)/bracket_top*100
    

    # Draw the vertical bracket
    ax.plot([x_center, x_center], [bracket_bottom, bracket_top], color='black', lw=1.5)
    ax.plot([x_center - 0.05, x_center + 0.05], [bracket_top, bracket_top], color='black', lw=1.5)
    ax.plot([x_center - 0.05, x_center + 0.05], [bracket_bottom, bracket_bottom], color='black', lw=1.5)




# Formatting the chart
ax.set_xlabel('Year', fontsize = 18)
ax.set_ylabel('DC Energy [TWh]', fontsize = 18)
#ax.set_title('DC Energy Generation of Existing Sites\nAbove 40 Degrees Latitude\nDecember, January, and February', fontsize = 20)
ax.set_xticks(x)
ax.set_xticklabels(years, fontsize = 18)
# Assuming you have an Axes object named ax
ax.tick_params(axis='y', labelsize=18)
# Set x-axis limits to match the range of years
plt.xlim(-.5, 9.6)
#plt.ylim(0, 1.8) #above 40 degrees lat
plt.ylim(0,15) #winter
#plt.ylim(0,70) #annual

ax.legend(fontsize = 18)
plt.xticks(rotation=45)
ax.yaxis.grid(True, linestyle='-', linewidth=0.5, alpha=0.5, zorder=0)
ax.set_axisbelow(True)  # Ensures grid lines are behind bars

plt.tight_layout()
# Save the figure
#fig.savefig(f"/volumes/wickett SSD/existing_site_{timeframe}_energy_generation_above_{lat_min}_stacked.png", dpi=300, bbox_inches="tight")
#fig.savefig("Figures/test.pdf", format='pdf', bbox_inches="tight")
#fig.savefig("/Volumes/Wickett SSD/Figures/Fig_2.pdf", format='pdf', bbox_inches='tight')
plt.show()





# %% [6] Bar Chart (not stacked) Different Labels
# # Create the bar chart
# x = np.arange(len(years))



# # Define colors for each category
# colors = {

#     "Tracking Sites with Snow": "#66D0B9",
#     "Tracking Sites without Snow": "#009E73",
#     "Fixed Sites with Snow": "#A8DDF5",
#     "Fixed Sites without Snow": "#56B4E9",
#     "All Sites with Snow": "#FFCD66",
#     "All Sites without Snow": "#E69F00"
    
# }

# # Adjust bar positions and plot with colors
# fig, ax = plt.subplots(figsize=(14, 6))

# # Define a consistent spacing for bars
# width = 0.15  # Width of the bars
# spacing = 0.15  # Spacing between bars

# # Adjusted offsets
# all_offsets = [-2 * spacing, -spacing]
# tracking_offsets = [0, spacing]
# fixed_offsets = [2 * spacing, 3 * spacing]

# # Plot bars
# all_without_snow_bars = ax.bar(
#     x + all_offsets[0], 
#     data["All Sites without Snow"], 
#     width, 
#     label="All Sites without Snow", 
#     color=colors["All Sites without Snow"], 
#     alpha=1
# )
# all_with_snow_bars = ax.bar(
#     x + all_offsets[1], 
#     data["All Sites with Snow"], 
#     width, 
#     label="All Sites with Snow", 
#     color=colors["All Sites with Snow"], 
#     alpha=1, 
#     hatch='\\'
# )
# tracking_without_snow_bars = ax.bar(
#     x + tracking_offsets[0], 
#     data["Tracking Sites without Snow"], 
#     width, 
#     label="Tracking Sites without Snow", 
#     color=colors["Tracking Sites without Snow"]
# )
# tracking_with_snow_bars = ax.bar(
#     x + tracking_offsets[1], 
#     data["Tracking Sites with Snow"], 
#     width, 
#     label="Tracking Sites with Snow", 
#     color=colors["Tracking Sites with Snow"], 
#     hatch='\\'
# )
# fixed_without_snow_bars = ax.bar(
#     x + fixed_offsets[0], 
#     data["Fixed Sites without Snow"], 
#     width, 
#     label="Fixed Sites without Snow", 
#     color=colors["Fixed Sites without Snow"]
# )
# fixed_with_snow_bars = ax.bar(
#     x + fixed_offsets[1], 
#     data["Fixed Sites with Snow"], 
#     width, 
#     label="Fixed Sites with Snow", 
#     color=colors["Fixed Sites with Snow"], 
#     hatch='\\'
# )





# # Add text above "Fixed Sites" bars for the loss
# for i, (bar_without_snow, bar_with_snow) in enumerate(zip(fixed_without_snow_bars, fixed_with_snow_bars)):
#     if data["Fixed Sites without Snow"][i] != 0:        
#         loss = data["Fixed Total Loss"][i]
#         x_position = bar_without_snow.get_x() + bar_without_snow.get_width() / 1
#         y_position = bar_without_snow.get_height() +.5  # Place text 10 units above the bar
#         ax.text(x_position, y_position, f'{loss:.1f}%', ha='center', va='bottom', fontsize=10, color='black')
#     else:
#         continue
    
# # Add text above "Tracking Sites" bars for the loss
# for i, (bar_without_snow, bar_with_snow) in enumerate(zip(tracking_without_snow_bars, tracking_with_snow_bars)):
#     if data["Tracking Sites without Snow"][i] != 0:
#         loss = data["Tracking Total Loss"][i]
#         x_position = bar_without_snow.get_x() + bar_without_snow.get_width() / 1
#         y_position = bar_without_snow.get_height() +.5  # Place text 10 units above the bar
#         ax.text(x_position, y_position, f'{loss:.1f}%', ha='center', va='bottom', fontsize=10, color='black')
#     else:
#         continue

# # Add text above "All Sites" bars for the loss
# for i, (bar_without_snow, bar_with_snow) in enumerate(zip(all_without_snow_bars, all_with_snow_bars)):
#     if data["All Sites without Snow"][i] != 0:
#         loss = data["Total Loss"][i]
#         x_position = bar_without_snow.get_x() + bar_without_snow.get_width() / 1
#         y_position = bar_without_snow.get_height() +.5  # Place text 10 units above the bar
#         ax.text(x_position, y_position, f'{loss:.1f}%', ha='center', va='bottom', fontsize=10, color='black')
#     else:
#         continue

# # Formatting the chart
# ax.set_xlabel('Year', fontsize = 14)
# ax.set_ylabel('DC Energy [GWh]', fontsize = 14)
# ax.set_title('DC Energy Generation of Existing Sites\nAbove 40 Degrees Latitude\nDecember, January, and February', fontsize = 20)
# ax.set_xticks(x)
# ax.set_xticklabels(years, fontsize = 14)
# # Assuming you have an Axes object named ax
# ax.tick_params(axis='y', labelsize=12)
# # Set x-axis limits to match the range of years
# plt.xlim(-.5, 9.6)
# ax.legend(fontsize = 14)
# plt.xticks(rotation=45)

# plt.tight_layout()
# # Save the figure
# #fig.savefig("Figures/existing_site_winter_energy_generation_above_40lat.png", dpi=300, bbox_inches="tight")
# plt.show()