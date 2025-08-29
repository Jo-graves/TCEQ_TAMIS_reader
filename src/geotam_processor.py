import pandas as pd
from pathlib import Path
import os


def get_header(filepath):
    '''Gets header of filepath (e.g., where column headers are located)'''
    with open(filepath, "r+") as pracfile: 
            for line_index, line in enumerate(pracfile):
                if "State Cd" in line:
                    return line_index

def get_delimiter(filepath):
    with open(filepath, "r+") as pracfile: 
            for line_index, line in enumerate(pracfile):
                if "Fields Delimited by: " in line:
                    delimiter = line.split("Fields Delimited by: ")[1].split()[0]
                    if delimiter == "Tab":
                        delimiter = "\t"
                    return delimiter

def get_clean_reference_info():
 
    script_path = Path(os.path.realpath(__file__)).parent

    params = fr"{script_path}/ref_files/tceq_parameters.txt"
    units = fr"{script_path}/ref_files/tceq_units.txt"
    site_info = fr"{script_path}/ref_files/tceq_site_locations.txt"

    try:
        tceq_keys = pd.read_table(params) #open tceq parameter info file to match parameter codes to parameters
        tceq_units = pd.read_table(units) #open tceq unit info file to match unit codes to units
        tceq_site_info = pd.read_table(site_info)

    except:
        tceq_keys = pd.read_table(f"{script_path}/ref_files/tceq_parameters.txt") #open tceq parameter info file to match parameter codes to parameters
        tceq_units = pd.read_table(f"{script_path}/ref_files/tceq_units.txt") #open tceq unit info file to match unit codes to units
        tceq_site_info = pd.read_table(f"{script_path}/ref_files/tceq_site_locations.txt")

    tceq_keys.rename(columns = {"Parm Code": "Parameter Cd", "Name":"Parameter Name"}, inplace = True)
    tceq_units.rename(columns = {"Code": "Unit Cd", "Description": "Unit Description", "Abbr": "Unit Abbr", "Type": "Unit Type"}, inplace = True)
    tceq_site_info.rename(columns = {"CAMS": "Site ID"}, inplace = True)
    tceq_site_info = tceq_site_info[["Site ID", "Site Name"]]
    tceq_site_info["Site ID"] = tceq_site_info['Site ID'].str.split(",") # Some sites have more than one site ID - this takes care of that
    tceq_site_info = tceq_site_info.explode("Site ID")

    tceq_site_info["Site ID"] = tceq_site_info['Site ID'].astype(int)

    return tceq_keys, tceq_units, tceq_site_info

def df_splitter(filepath):

    TCEQ_Header = get_header(filepath)        #get header from get_header function
    TCEQ_delimiter = get_delimiter(filepath)

    df =  pd.read_csv(filepath, sep = TCEQ_delimiter, header = TCEQ_Header, #open csv
            skiprows = [], skip_blank_lines = False, encoding = 'ascii',
            encoding_errors = 'replace') 
    
    df.dropna(axis = 'columns', inplace = True ) #drop column if all values are na - there's usually a few
    df['Datetime'] = df['Date'].astype(str) + ' '  + df['Time'] #combine date and time to form datetime
    df["dt_string"] = pd.to_datetime(df["Datetime"], format='%Y%m%d %H:%M')
    
    tceq_keys, tceq_units, tceq_site_info = get_clean_reference_info()

    df_keys = df.merge(tceq_keys, on = "Parameter Cd", how = "inner")
    df_u = df_keys.merge(tceq_units, on = "Unit Cd", how = "inner")
    df_o = df_u.merge(tceq_site_info, on = "Site ID", how = "inner")

    return df_o


def geotam_to_structured_data(geotam_txt_file, date_start = None, date_end = None, save = False, saved_file_type = "csv"):


    # Process data
    df_out = df_splitter(geotam_txt_file)

    # Create new column that merges parameter name with units for pivoting
    df_out["Column_Name"] = "TCEQ " + df_out["Parameter Name"] + " (" + df_out["Unit Abbr"] + ")"

    # Pivot rows to column format
    df_clean = df_out[["Value", "Column_Name", "dt_string", "Site Name", "Site ID"]]

    df_clean_piv = df_clean.pivot(index = ["dt_string", "Site Name", "Site ID"], values = "Value", columns="Column_Name").reset_index()
    df_clean_piv.set_index("dt_string", inplace = True)
    df_clean_piv.insert(2, "Datetime - CST", df_clean_piv.index.tz_localize("Etc/GMT+6"))
    df_clean_piv.reset_index(inplace = True, drop = True)
    df_clean_piv.columns.name = None

  
    # Get only specified dates
    if date_start == None:
        date_start = df_clean_piv["Datetime - CST"].iloc[0]
    else:
        date_start == date_start

    if date_end == None:
        date_end = df_clean_piv["Datetime - CST"].iloc[-1]
    else:
        date_end == date_end

    df_clean_piv = df_clean_piv.copy()[df_clean_piv.copy()["Datetime - CST"].between(date_start, date_end)]


    # Saving functions
    if save == True:

        if saved_file_type == "csv":
                df_clean_piv.to_csv(Path(geotam_txt_file).with_suffix(".csv"))
                print(f"Processed file saved to: {Path(geotam_txt_file).with_suffix(".csv")}")

        elif saved_file_type == 'parquet':
            df_clean_piv.to_parquet(Path(geotam_txt_file).with_suffix(".gzip"))
            print(f"Processed file saved to: {Path(geotam_txt_file).with_suffix(".gzip")}")

    
    return df_clean_piv


if __name__ == "__main__":
     

    import sys
    import argparse

    def str2bool(v): 
        if isinstance(v, bool): 
            return v 
        if v.lower() in ('yes', 'true', 't', 'y', '1'): 
            return True 
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
                                                
            return False 
        else: 
            raise argparse.ArgumentTypeError('Boolean value expected.')

    p = argparse.ArgumentParser()
    p.add_argument('-f', "--file2proc", type=str, help='TCEQ geotam data file to process - processed data is saved to location of input file')
    p.add_argument('--start_date', type=int, default = None, help='Process only dates after start_date')
    p.add_argument('--end_date', type=int, default = None, help='Process only dates before end_date')
    p.add_argument('--saved_file_type', type=bool, default = False, help='save file as csv as well as parquet')
   
    args = p.parse_args()

    # By running the script directly from the command line, it is assumed the file intends to be saved so the save flag is always set to true
    geotam_to_structured_data(geotam_txt_file = args.file2proc, date_start = args.start_date, date_end = args.end_date, save = True, saved_file_type = args.saved_file_type)

    