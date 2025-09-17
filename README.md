# GeoTAM Processor

## Purpose
The GeoTAM processor converts raw measurement report files from the [Texas Air Monitoring Information System (TAMIS)](https://www17.tceq.texas.gov/tamis/index.cfm?fuseaction=home.welcome) into csv and parquet (.gzip) formats. TAMIS is maintained by the [Texas Commission on Environmental Quality (TCEQ)](https://www.tceq.texas.gov/).


## Installation
This repository is constructed as a python package so it can be cloned to a local filesystem and then installed into a virtual environment 
(similar to using conda or pip to install packages directly from online package channels).
After the package is installed, the main modules can be imported and used in any scripts running in the virtual environment.

### To install the package:
1. Clone the repository into any location on your local filesystem using `git clone clone_path_from_github`
2. Activate a virtual environment (e.g., `conda activate my_env_name`)
3. Install conda to the virtual environment if it is not installed already `conda install anaconda::pip`
4. Run `pip install local_path/to/cloned/repo/TCEQ_geotam_reader`

#### Example using https and cloning directory to Desktop
From the command line:
```
cd /Path/to/Desktop                               # Change directory to Desktop
conda activate my_virtual_environment             # activate virtual environment
conda install anaconda::pip                       # Optional: install pip (if not already installed) to conda environment
git clone https://github.com/this_repo            # Clone repo to cwd (Desktop)
pip install /Path/to/Desktop/TCEQ_geotam_reader   # Install package to virtual environment
```
pip just needs the filepath to the directory holding the setup.py and *.toml files to install the package. 

The package can be optionally installed in development mode if you plan to edit the source code and would like the edits to appear automatically without having the update the code (e.g., `pip update TCEQ_geotam_viewer`). This can be done using the -e flag when installing with pip:  \
`pip install -e /Path/to/Desktop/TCEQ_geotam_reader`


## Data Access
Data can be accessed at the TCEQ TAMIS data request page:
https://www17.tceq.texas.gov/tamis/index.cfm?fuseaction=report.main


> Landing page
> 
> <img width="822" height="249" alt="image" src="https://github.com/user-attachments/assets/2fc065bf-6fe0-4ebc-a0e0-e9e50e3ef50b" />

## Reference tables
Several reference tables are included in the package. Additional information is provided in [Reference Table Information](./resources/reference_table_information.md)




