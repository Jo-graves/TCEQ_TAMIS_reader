# GeoTAM Processor

## Purpose
GeoTAM processor converts comma-delimited text (.txt) reports from TCEQ's Texas Air Monitoring Information System (TAMIS) into .csv and parquet (.gzip) formats.
Data in these formats is much easier to work with.

## Installation
This repository is constructed as a python package so it can be cloned to a local filesystem and then installed into a virtual environment 
(similar to using conda or pip install to install packages directly from online package channels).
After the package is installed, the main modules can be imported and used in any scripts running in the virtual environment, just like any other package.

### To install the package:
1. Clone the repository into any location on your local filesystem using `git clone clone_path`
2. Activate a virtual environment (e.g., `conda activate my_env_name`)
3. Install conda to the virtual environment if it is not installed already `conda install anaconda::pip`
4. Run `pip install local_path/to/cloned/repo/TCEQ_geotam_reader` (e.g., if this repo is cloned to /Usr/bin run `/Usr/bin/TCEQ_geotam_reader`, pip just needs the filepath to the directory holding the setup.py and *.toml files) 
That's it!

## Data Access
Data can be accessed at the TCEQ TAMIS data request page:
https://www17.tceq.texas.gov/tamis/index.cfm?fuseaction=report.main


> Landing page
> 
> <img width="822" height="249" alt="image" src="https://github.com/user-attachments/assets/2fc065bf-6fe0-4ebc-a0e0-e9e50e3ef50b" />

## Reference tables
Several reference tables are included in the package. Additional information is provided in [Reference Table Information](./resources/reference_table_information.md)




