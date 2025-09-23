#%%
import os
from pathlib import Path
import pandas as pd

#### Extras
def convert_ref_files_to_csv():
    file_path = Path(os.path.realpath(__file__)).parent

    params = fr"{file_path}/ref_files/tceq_parameters.txt"
    units = fr"{file_path}/ref_files/tceq_units.txt"
    site_info = fr"{file_path}/ref_files/tceq_site_locations.txt"

    tceq_keys = pd.read_table(params) 
    tceq_units = pd.read_table(units)
    tceq_site_info = pd.read_table(site_info)


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
