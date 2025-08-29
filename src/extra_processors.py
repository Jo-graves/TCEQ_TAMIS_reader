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

    # #print(Path(params).with_suffix(".csv"))

    tceq_keys.to_csv(Path(params).with_suffix(".csv"))
    tceq_units.to_csv(Path(units).with_suffix(".csv"))
    tceq_site_info.to_csv(Path(site_info).with_suffix(".csv"))

if __name__ == "__main__":
    convert_ref_files_to_csv() 