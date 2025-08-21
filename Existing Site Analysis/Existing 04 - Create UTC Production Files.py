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
    
# Helper Function - Make years full 8760 hours
def pad_utc_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Ensures the df has every hourly UTC timestamp (8760 hours).
    Missing hours are filled with 0's if numeric columns and '' if string columns.
    Drops duplicate timestamps (keeps first timestamp) and sorts by index.
    """
    df = df.copy()

    # Make sure there is UTC datetime index named 'UTC'
    if "UTC" in df.columns and df.index.name != "UTC": #If there is no index named "UTC," create one and insert as index.
        df["UTC"] = pd.to_datetime(df["UTC"], utc=True, errors="coerce")
        df = df.dropna(subset=["UTC"]).set_index("UTC")
    else: #If there is an index named "UTC," make sure it is in datetime/UTC format and has no Nan values
        df.index = pd.to_datetime(df.index, utc=True, errors="coerce")
        df = df[~df.index.isna()]
        df.index.name = "UTC"

    # Make tz-aware UTC
    if df.index.tz is None: 
        df.index = df.index.tz_localize("UTC") #attaches timezone to timestamp without shifting clock time
    else:
        df.index = df.index.tz_convert("UTC") #converts already tz-aware timestamp to UTC, shifting the clock

    # add 30 mins to each time stamp
    df.index = df.index.floor("h") + pd.Timedelta(minutes=30)

    # remove duplicate timestamps (if any) and sort
    if df.index.has_duplicates:
        df = df[~df.index.duplicated(keep="first")]
    df = df.sort_index()

    # build full UTC index
    start_utc = pd.Timestamp(f"{year}-01-01 00:30:00", tz="UTC")
    end_utc = pd.Timestamp(f"{year+1}-01-01 00:30:00", tz="UTC")
    full_idx = pd.date_range(start=start_utc, end=end_utc - pd.Timedelta(hours=1), freq="h", tz="UTC")

    # eliminate leap day from leap years
    if pd.Timestamp(f"{year}-01-01").is_leap_year:
        full_idx = full_idx[~((full_idx.month == 2) & (full_idx.day == 29))]

    # re-index df to have full_idx
    df_out = df.reindex(full_idx)

    # Fill Nans with zero or ''
    num_cols = df_out.select_dtypes(include="number").columns
    obj_cols = df_out.columns.difference(num_cols)
    if len(num_cols):
        df_out[num_cols] = df_out[num_cols].fillna(0)
    if len(obj_cols):
        df_out[obj_cols] = df_out[obj_cols].fillna("")
    
    df_out.index.name = 'UTC'

    return df_out

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
                df_year = pad_utc_year(df_year,year)
                
                print(f"[{out_scenario}] {site_key} -> {len(df_year)} rows (expected 8760)")
                
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
def main() -> int:
    process_scenario("Roof_Slide_Coeff")
    process_scenario("No_Snow")

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
