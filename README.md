# Grid-Scale Snow Loss
This repository allows the user to perform PySAM PVWattsNone snow loss simulations on existing utility-scale solar sites in the Eastern Interconnect. The geographic area can be expanded by modifying the code.

To run the analysis, snow data from the National Snow and Ice Data Center (NSIDC) must be downloaded as .nc files from the NSIDC website (https://nsidc.org/data/nsidc-0719/versions/1). The NSIDC .nc files must be placed in the Data/Snow Data folder. 

The site data must be downloaded from the Lawrence Berkeley National Laboratory (LBNL) website (https://emp.lbl.gov/publications/utility-scale-solar-2024-edition). The downloadable data file is in .xlsx format with multiple sheets. Save the sheet with the raw site data as a .csv titled "existing_site_data.csv" Concentrating solar sites must be filtered out of the dataset. It is advised to clean the data to only include sites that you wish to simulate. 

The NERC subregion geometry must be downloaded from https://gem.anl.gov/tool. The unzipped folder "jurisdiction_nerc_subregion_v1" should be placed in the Data/Geo Data folder.

In the Existing Site Anslysis folder, run the scrips from 00->04 to generate simulated production data and then run 05 through 09 (in any order) for visuals.

Data Sources:

J. Seel, J.M. Kemp, A. Cheyette, D. Millstein, W. Gorman, S. Jeong, D. Robson, R.
Setiawan, M. Bolinger, Utility-Scale Solar, 2024 Edition: Analysis of Empirical Plant-level
Data from U.S. Ground-mounted PV, PV+battery, and CSP Plants (exceeding 5 MWAC),
Lawrence Berkeley National Lab (LBNL), 2024. https://dx.doi.org/10.25984/2460457

P. Broxton, X. Zeng, N. Dawson, Daily 4 km Gridded SWE and Snow Depth from
Assimilated In-Situ and Modeled Data over the Conterminous US, NASA National Snow and
Ice Data Center Distributed Active Archive Center, v1, 2019.
https://doi.org/10.5067/0GGPB220EX6A.

National Renewable Energy Laboratory (NREL), National Solar Radiation Data Base
(NSRDB), PSM v3, 2023. https://nsrdb.nrel.gov 

Argonne National Laboratory, North American Electric Reliability Corporation (NERC) Subregions, Geospatial Energy Mapper (GEM), U.S. Department of Energy, v1, 2018. https://gem.anl.gov/

