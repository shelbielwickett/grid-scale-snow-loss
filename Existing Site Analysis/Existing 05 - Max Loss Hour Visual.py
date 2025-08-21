"""
@author: shelbiedavis1
This code plots the Maximum Loss Hour for each year or season for all sites. It
also plots the tracking and fixed tilt systems separately.
The axes need to be updated to adapt to the input data. Right now, the axes are
configured for plotting all years 2013-2022.
Updated on 8/19/25 to use portable file paths for GitHub sharing
"""
####################### Library Imports #############################
import pandas as pd
import json
import matplotlib.pyplot as plt
import numpy as np
import os
from pathlib import Path

####################### User Defined Constants #############################
start_year = 2021 #first analysis year
end_year = 2022 #last analysis year
title = 'Eastern Interconnect Analysis.json'
#months = [12, 1, 2]  # Winter months
months = [1,2,3,4,5,6,7,8,9,10,11,12] #All months


# === CHOOSE BASE DIRECTORY ===
BASE_DIR = Path("/Volumes/Wickett SSD/Snow_Loss_Project")

####################### OTHER Constants #############################

yearly_summary = []  # To store yearly summary results

####################### Main Code #############################

# Iterate through each year
for year in range(start_year, end_year + 1):
    JSON_DIR = BASE_DIR / "Project json files"
    file = f"{year} {title}"
    with open(JSON_DIR / file) as f:
        site_dict = json.load(f)

    # Temporary storage for all site losses
    all_losses = pd.DataFrame()

    # Iterate through each site in the dictionary
    for i, site_key in enumerate(site_dict):
        print(f"{i} and {site_key}")
        
        # exclude sites with no snow data
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

        # Load No Snow data
        try:
            SAM_no_snow = pd.read_csv(
                BASE_DIR / f'PySAM_Results_UTC/Existing_Sites_Results_UTC/No_Snow/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv'
                )
        except Exception as e:
            print(f"Error: {e}")

        SAM_no_snow['UTC'] = pd.to_datetime(SAM_no_snow['UTC'])
        SAM_no_snow = SAM_no_snow[SAM_no_snow['UTC'].dt.month.isin(months)]
        SAM_no_snow['DC no snow Power [W]'] = SAM_no_snow['DC Inverter Input Power [W]']
        # Load Snow data
        try:
            SAM_snow = pd.read_csv(
                BASE_DIR / f'PySAM_Results_UTC/Existing_Sites_Results_UTC/Roof_Slide_Coeff/{year} SAM Results/{site_data["Project Name"]}_{year}_Results.csv'
                )
        except Exception as e:
            print(f"Error: {e}")
        SAM_snow['UTC'] = pd.to_datetime(SAM_snow['UTC'])
        SAM_snow = SAM_snow[SAM_snow['UTC'].dt.month.isin(months)]
        
        try:            
            # Calculate loss
            SAM_snow['Loss'] = SAM_no_snow['DC Inverter Input Power [W]'] - SAM_snow['DC Inverter Input Power [W]']
            SAM_snow['Mount'] = site_data['Mount']
            SAM_snow['Site'] = site_data['Project Name']
            SAM_snow['DC no snow Power [W]'] = SAM_no_snow['DC Inverter Input Power [W]']
            SAM_snow['DC snow Power [W]'] = SAM_snow['DC Inverter Input Power [W]']
            
            
            # Calculate loss and annotate
            loss_series = SAM_no_snow['DC no snow Power [W]'] - SAM_snow['DC snow Power [W]']
            combined = pd.DataFrame({
               'UTC': SAM_snow['UTC'],
               'Loss': loss_series,
               'Mount': site_data['Mount'],
               'Site': site_data['Project Name'],
               'DC no snow Power [W]': SAM_no_snow['DC no snow Power [W]'],
               'DC snow Power [W]': SAM_snow['DC snow Power [W]']
               }, index=SAM_no_snow.index)

            all_losses = pd.concat([all_losses, combined], ignore_index=False)
        except Exception as e:
                print(f"Error for site {site_data['Project Name']} ({year}): {e}")

    print(all_losses)    
    # Find the row with the maximum total loss across all sites
    max_loss_row = all_losses.groupby(all_losses.index)['Loss'].sum().idxmax()
    total_max_loss = all_losses.groupby(all_losses.index)['Loss'].sum().loc[max_loss_row]
    total_generation_at_max = all_losses.groupby(all_losses.index)['DC no snow Power [W]'].sum().loc[max_loss_row]
    total_generation_at_max_snow = all_losses.groupby(all_losses.index)['DC snow Power [W]'].sum().loc[max_loss_row]
    max_loss_timestamp = all_losses['UTC'].iloc[max_loss_row]
    print(max_loss_timestamp)


    # Filter losses for fixed tilt and tracking at the max-loss timestamp
    losses_at_max = all_losses.loc[[max_loss_row]]
    total_fixed_loss = losses_at_max[losses_at_max['Mount'].str.lower() == 'fixed tilt']['Loss'].sum()
    total_fixed_gen = losses_at_max[losses_at_max['Mount'].str.lower() == 'fixed tilt']['DC no snow Power [W]'].sum()
    total_fixed_gen_snow = losses_at_max[losses_at_max['Mount'].str.lower() == 'fixed tilt']['DC snow Power [W]'].sum()
    
    total_tracking_loss = losses_at_max[losses_at_max['Mount'].str.lower() == 'tracking']['Loss'].sum()
    total_tracking_gen = losses_at_max[losses_at_max['Mount'].str.lower() == 'tracking']['DC no snow Power [W]'].sum()
    total_tracking_gen_snow = losses_at_max[losses_at_max['Mount'].str.lower() == 'tracking']['DC snow Power [W]'].sum()
    
    # Append yearly summary
    yearly_summary.append({
        'Year': year,
        'Max Loss Timestamp': max_loss_timestamp,
        'Total Max Loss [W]': total_max_loss,
        'Total Power Generation at Max Loss [W]': total_generation_at_max,
        'Total Power Generation at Max Loss with snow [W]': total_generation_at_max_snow,
        
        'Total Fixed Tilt Loss at Max': total_fixed_loss,
        'Total Fixed Tilt Power Generation at Max Loss [W]': total_fixed_gen,
        'Total Fixed Tilt Power Generation at Max Loss with snow [W]': total_fixed_gen_snow,
        
        'Total Tracking Loss at Max': total_tracking_loss,
        'Total Tracking Power Generation at Max Loss [W]': total_tracking_gen,
        'Total Tracking Power Generation at Max Loss with snow [W]': total_tracking_gen_snow

    })

# Convert yearly summary to DataFrame
summary_df = pd.DataFrame(yearly_summary)

# Display the yearly summary
print(summary_df.iloc[:,1:4])


max_loss_dates = summary_df['Max Loss Timestamp']
print(max_loss_dates)
total_losses = summary_df['Total Max Loss [W]']/1000000000
total_gen = summary_df['Total Power Generation at Max Loss [W]']/1000000000
total_gen_snow = summary_df['Total Power Generation at Max Loss with snow [W]']/1000000000

tracking_losses = summary_df['Total Tracking Loss at Max']/1000000000  # Total tracking losses at max
tracking_gen = summary_df['Total Tracking Power Generation at Max Loss [W]']/1000000000
tracking_gen_snow = summary_df['Total Tracking Power Generation at Max Loss with snow [W]']/1000000000

fixed_tilt_losses = summary_df['Total Fixed Tilt Loss at Max']/1000000000  # Total fixed tilt losses at max
fixed_gen = summary_df['Total Fixed Tilt Power Generation at Max Loss [W]']/1000000000
fixed_gen_snow = summary_df['Total Fixed Tilt Power Generation at Max Loss with snow [W]']/1000000000

####################### Plot Properties #############################
years = range(start_year, end_year + 1)
# Bar positions
x = np.arange(len(years))
print(x)
width = 0.15

# Offset for each bar group
offsets = [-2.5*width, -1.5 * width, -0.5 * width, 0.5 * width, 1.5 * width, 2.5 * width]

####################### Create Plot #############################
# Create the bar plot
fig1, ax = plt.subplots(figsize=(14, 6))
bars_total = ax.bar(x + offsets[0], total_gen, width, label='Total Power', color='#E69F00')
bars_total_snow = ax.bar(x + offsets[1], total_gen_snow, width, label = 'Total Power w/ Snow Cover', color = '#FFCD66', hatch='\\')
bars_tracking = ax.bar(x + offsets[2], tracking_gen, width, label='Tracking Power', color='#009E73')
bars_tracking_snow = ax.bar(x + offsets[3], tracking_gen_snow, width, label = 'Tracking Power w/ Snow Cover', color = '#66D0B9', hatch='\\')
bars_fixed = ax.bar(x + offsets[4], fixed_gen, width, label='Fixed Tilt Power', color='#56B4E9')
bars_fixed_snow = ax.bar(x + offsets[5], fixed_gen_snow,width, label = 'Fixed Tilt Power w/ Snow Cover', color = '#A8DDF5', hatch='\\' )

# Customize the plot
ax.set_xlabel('Year', fontsize = 14)
ax.set_ylabel('DC Power Output [GW]', fontsize=14)
#ax.set_title('Maximum Power Loss from Snow in 1 Hour\nEastern Interconnect', fontsize = 20)
ax.set_xticks(x)
ax.set_xticklabels(years, fontsize = 14)
ax.tick_params(axis='y', labelsize=12)
ax.legend(fontsize = 12)
plt.xticks(rotation=45)
ax.yaxis.grid(True, linestyle='-', linewidth=0.5, alpha=0.5, zorder = 0)
ax.set_axisbelow(True)  # Ensures grid lines are behind bars
ax.set_ylim(0, 22.5)  # Adjust y-axis limits for visibility

# Add vertical brackets to label differences, centered over the blue bars
for i, (bar_without_snow, bar_with_snow) in enumerate(zip(bars_total, bars_total_snow)):
    x_center = x[i] + offsets[1] # Centered over the blue bar
    bracket_bottom = bar_with_snow.get_height()  # Bottom of the bracket starts at the blue bar height
    bracket_top = bar_without_snow.get_height()  # Top of the bracket ends at the orange bar height
    mid_bracket = (bracket_bottom + bracket_top) / 2  # Midpoint for the text label
    total_loss = bracket_top-bracket_bottom
    total_percent_loss = (bracket_top-bracket_bottom)/bracket_top*100
    formatted_date = max_loss_dates[i].strftime('%m/%d')
    formatted_time = max_loss_dates[i].strftime('%H:%M')

    # Draw the vertical bracket
    ax.plot([x_center, x_center], [bracket_bottom, bracket_top], color='black', lw=1.5)
    ax.plot([x_center - 0.05, x_center + 0.05], [bracket_top, bracket_top], color='black', lw=1.5)
    ax.plot([x_center - 0.05, x_center + 0.05], [bracket_bottom, bracket_bottom], color='black', lw=1.5)

    # Annotate the difference
    ax.text(x_center, bracket_top+.1, f'Max Loss\nHour:\n{formatted_date} {formatted_time}\nTotal Loss:\n{total_loss:.1f} GW\n({total_percent_loss:.1f}%)', ha='center', va='bottom', fontsize=10, color='black')

####################### Print and Save Plot #############################
fig_dir= BASE_DIR / 'Figures'
folder = fig_dir.parent
folder.mkdir(parents=True, exist_ok=True)

# Show the plot
plt.tight_layout()
fig1.savefig(BASE_DIR / 'Figures/Max Loss Hour Figure 1.pdf', format='pdf', bbox_inches='tight')
#plt.show()

####################### Stacked Plot Properties #############################
years = range(start_year, end_year + 1)

# Bar positions
x = np.arange(len(years))
print(x)
width = 0.15
spacing = 0.15  # Spacing between bars

# Adjusted offsets
all_offsets = [-1.5 * spacing, -.5*spacing]
other_offsets = [.5 * spacing, 1.5 * spacing]

####################### Create Stacked Plot #############################
# Create the bar plot with updated date formatting
fig2, ax = plt.subplots(figsize=(14, 6))
bars_total = ax.bar(
    x + all_offsets[0], 
    total_gen, 
    width, 
    label='Total Power', 
    color='#E69F00'
    )

bars_total_snow = ax.bar(
    x + all_offsets[1], 
    total_gen_snow, width, 
    label = 'Total Power w/ Snow Cover', 
    color = '#FFCD66', 
    hatch='\\'
    )

bars_tracking = ax.bar(
    x + other_offsets[0], 
    tracking_gen, 
    width, 
    label='Tracking Power', 
    color='#009E73'
    )

bars_tracking_snow = ax.bar(
    x + other_offsets[1], 
    tracking_gen_snow, 
    width, 
    label = 'Tracking Power w/ Snow Cover', 
    color = '#66D0B9', 
    hatch='\\'
    )

bars_fixed = ax.bar(
    x + other_offsets[0], 
    fixed_gen, 
    width, 
    label='Fixed Tilt Power', 
    color='#56B4E9',
    bottom = tracking_gen
    )

bars_fixed_snow = ax.bar(
    x + other_offsets[1], 
    fixed_gen_snow,
    width, 
    label = 'Fixed Tilt Power w/ Snow Cover', 
    color = '#A8DDF5', 
    hatch='\\',
    bottom = tracking_gen_snow
    )


# Customize the plot
ax.set_xlabel('Year', fontsize = 16)
ax.set_ylabel('DC Power Output [GW]', fontsize=16)
#ax.set_title('Maximum Power Loss from Snow in 1 Hour\nEastern Interconnect', fontsize = 20)
ax.set_xticks(x)
ax.set_xticklabels(years, fontsize = 16)
ax.tick_params(axis='y', labelsize=16)
ax.legend(fontsize = 16)
plt.xticks(rotation=45)
ax.yaxis.grid(True, linestyle='-', linewidth=0.5, alpha=0.5, zorder = 0)
ax.set_axisbelow(True)  # Ensures grid lines are behind bars
ax.set_ylim(0, 22.5)  # Adjust y-axis limits for visibility

# Add vertical brackets to label differences, centered over the blue bars
for i, (bar_without_snow, bar_with_snow) in enumerate(zip(bars_total, bars_total_snow)):
    x_center = x[i] + all_offsets[1] # Centered over the blue bar
    bracket_bottom = bar_with_snow.get_height()  # Bottom of the bracket starts at the blue bar height
    bracket_top = bar_without_snow.get_height()  # Top of the bracket ends at the orange bar height
    mid_bracket = (bracket_bottom + bracket_top) / 2  # Midpoint for the text label
    total_loss = bracket_top-bracket_bottom
    total_percent_loss = (bracket_top-bracket_bottom)/bracket_top*100
    formatted_date = max_loss_dates[i].strftime('%m/%d')
    formatted_time = max_loss_dates[i].strftime('%H:%M')

    # Draw the vertical bracket
    ax.plot([x_center, x_center], [bracket_bottom, bracket_top], color='black', lw=1.5)
    ax.plot([x_center - 0.05, x_center + 0.05], [bracket_top, bracket_top], color='black', lw=1.5)
    ax.plot([x_center - 0.05, x_center + 0.05], [bracket_bottom, bracket_bottom], color='black', lw=1.5)

    # Annotate the difference
    ax.text(x_center, bracket_top+.1, f'Max Loss\nHour:\n{formatted_date} {formatted_time}\nTotal Loss:\n{total_loss:.1f} GW\n({total_percent_loss:.1f}%)', ha='center', va='bottom', fontsize=13, color='black')

####################### Print and Save Plot #############################
# Show the plot
plt.tight_layout()
fig2.savefig(BASE_DIR / 'Figures/Max Loss Hour Figure 2.pdf', format='pdf', bbox_inches='tight')
plt.show()