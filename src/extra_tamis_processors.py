#%%
from pathlib import Path
import pandas as pd
import tceq_tamis_processor as tgp
import importlib
importlib.reload(tgp)
from importlib import resources

def pull_extras_data(ref_dir, ref_file):
    with resources.path(ref_dir, ref_file) as ref_path:
        return ref_path, pd.read_table(ref_path)
    
#### Extras
def convert_ref_files_to_csv():
    
    '''Processes .txt reference files from TCEQ Geotam website to csv for easier reading in main module.
    The package comes with files that have already been processed. This script is just included as a reference'''


    params, tceq_keys = pull_extras_data("ref_files", "tceq_parameters.txt")
    units, tceq_units = pull_extras_data("ref_files", "tceq_units.txt")
    site_info, tceq_site_info = pull_extras_data("ref_files", "tceq_site_locations.txt")

    tceq_keys.rename(columns = {"Parm Code": "Parameter Cd", "Name":"Parameter Name"}, inplace = True)
    tceq_units.rename(columns = {"Code": "Unit Cd", "Description": "Unit Description", "Abbr": "Unit Abbr", "Type": "Unit Type"}, inplace = True)
    tceq_site_info.rename(columns = {"CAMS": "Site ID"}, inplace = True)
    tceq_site_info = tceq_site_info[["Site ID", "Site Name"]]
    tceq_site_info["Site ID"] = tceq_site_info['Site ID'].str.split(",") # Some sites have more than one site ID - this takes care of that
    tceq_site_info = tceq_site_info.explode("Site ID")

    tceq_site_info["Site ID"] = tceq_site_info['Site ID'].astype(int)

    tceq_keys.to_csv(Path(params).with_suffix(".csv"), index=False)
    tceq_units.to_csv(Path(units).with_suffix(".csv"), index=False)
    tceq_site_info.to_csv(Path(site_info).with_suffix(".csv"), index=False)

if __name__ == "__main__":
    convert_ref_files_to_csv() 


# %%
