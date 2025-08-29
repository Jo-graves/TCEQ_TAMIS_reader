import pandas as pd
import csv
from datetime import datetime
from pathlib import Path
from functools import reduce
from datetime import datetime
from functools import reduce
import sys
import os


def get_header(filepath):
    '''Gets header of filepath (e.g., where column headers are located)'''
    with open(filepath, "r+") as pracfile: 
            for id, line in enumerate(pracfile):
                if "State Cd" in line:
                    return id
            
            

def df_splitter(filepath):

    file_path = Path(os.path.realpath(__file__)).parent

    params = fr"{file_path}/ref_files/tceq_parameters.txt"
    units = fr"{file_path}/ref_files/tceq_units.txt"
    site_info = fr"{file_path}/ref_files/tceq_site_locations.txt"

    TCEQ_Header = get_header(filepath)        #get header from get_header function

    RD =  pd.read_csv(filepath, sep = ',', header = TCEQ_Header, #open csv
            skiprows = [], skip_blank_lines = False, encoding = 'ascii',
            encoding_errors = 'replace') 
    
    RD.dropna(axis = 'columns', inplace = True ) #drop column if all values are na - there's usually a few
    RD['Datetime'] = RD['Date'].astype(str) + ' '  + RD['Time'] #combine date and time to form datetime
    RD["dt_string"] = pd.to_datetime(RD["Datetime"], format='%Y%m%d %H:%M')

    try:
        tceq_keys = pd.read_table(params) #open tceq parameter info file to match parameter codes to parameters
        tceq_units = pd.read_table(units) #open tceq unit info file to match unit codes to units
        tceq_site_info = pd.read_table(site_info)

    except:
        tceq_keys = pd.read_table(f"{file_path}/ref_files/tceq_parameters.txt") #open tceq parameter info file to match parameter codes to parameters
        tceq_units = pd.read_table(f"{file_path}/ref_files/tceq_units.txt") #open tceq unit info file to match unit codes to units
        tceq_site_info = pd.read_table(f"{file_path}/ref_files/tceq_site_locations.txt")


    tceq_keys.rename(columns = {"Parm Code": "Parameter Cd", "Name":"Parameter Name"}, inplace = True)
    tceq_units.rename(columns = {"Code": "Unit Cd", "Description": "Unit Description", "Abbr": "Unit Abbr", "Type": "Unit Type"}, inplace = True)
    tceq_site_info.rename(columns = {"CAMS": "Site ID"}, inplace = True)
    tceq_site_info = tceq_site_info[["Site ID", "Site Name"]]
    tceq_site_info["Site ID"] = tceq_site_info['Site ID'].str.split(",") # Some sites have more than one site ID - this takes care of that
    tceq_site_info = tceq_site_info.explode("Site ID")

    #print(tceq_site_info["Site ID"].unique())
    tceq_site_info["Site ID"] = tceq_site_info['Site ID'].astype(int)

    #print(tceq_site_info)


    #print(RD)
    RD_keys = RD.merge(tceq_keys, on = "Parameter Cd", how = "inner")
    RD_u = RD_keys.merge(tceq_units, on = "Unit Cd", how = "inner")
    RD_o = RD_u.merge(tceq_site_info, on = "Site ID", how = "inner")

    return RD_o

    # return my_dict

def convert_ref_files():
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
    

def geotam_to_csv(geotam_txt_file, date_start = None, date_end = None, save = False, saved_file_type = "csv"):


    df_out = df_splitter(geotam_txt_file)

    df_out["Column_Name"] = "TCEQ " + df_out["Parameter Name"] + " (" + df_out["Unit Abbr"] + ")"

    #print(df_out)

    df_clean = df_out[["Value", "Column_Name", "dt_string", "Site Name", "Site ID"]]

    #print(df_clean)

    df_clean_piv = df_clean.pivot(index = ["dt_string", "Site Name", "Site ID"], values = "Value", columns="Column_Name").reset_index()
    df_clean_piv.set_index("dt_string", inplace = True)
    #print(df_clean_piv)
    df_clean_piv.insert(2, "Datetime - CST", df_clean_piv.index.tz_localize("Etc/GMT+6"))
    df_clean_piv.reset_index(inplace = True, drop = True)
    df_clean_piv.columns.name =None

    #print(df_clean_piv)

  
    if date_start == None:
        date_start = df_clean_piv["Datetime - CST"].iloc[0]
    else:
        date_start == date_start

    if date_end == None:
        date_end = df_clean_piv["Datetime - CST"].iloc[-1]
    else:
        date_end == date_end

    df_clean_piv = df_clean_piv.copy()[df_clean_piv.copy()["Datetime - CST"].between(date_start, date_end)]

    if save == True:

        if saved_file_type == "csv":
                df_clean_piv.to_csv(Path(geotam_txt_file).with_suffix(".csv"))
                print(f"Processd file saved to: {Path(geotam_txt_file).with_suffix(".csv")}")

        elif saved_file_type == 'parquet':
            df_clean_piv.to_parquet(Path(geotam_txt_file).with_suffix(".gzip"))
            print(f"Processd file saved to: {Path(geotam_txt_file).with_suffix(".gzip")}")

    print(df_clean_piv.columns)
    
    return df_clean_piv


if __name__ == "__main__":
     
    fpath = Path(os.path.realpath(__file__)).parent
    geotam_to_csv(f"{fpath}/../tests/2023_kc_autogc_w_ws_wd.txt", save = True, saved_file_type="csv")

    # import sys
    # import argparse

    # def str2bool(v): 
    #     if isinstance(v, bool): 
    #         return v 
    #     if v.lower() in ('yes', 'true', 't', 'y', '1'): 
    #         return True 
    #     elif v.lower() in ('no', 'false', 'f', 'n', '0'):
                                                
    #         return False 
    #     else: 
    #         raise argparse.ArgumentTypeError('Boolean value expected.')

    # p = argparse.ArgumentParser()
    # p.add_argument('-f', "--file2proc", type=str, help='TCEQ geotam data file to process - processed data is saved to location of input file')
    # p.add_argument('--start_date', type=int, default = None, help='Process only dates after start_date')
    # p.add_argument('--end_date', type=int, default = None, help='Process only dates before end_date')
    # p.add_argument('--saved_file_type', type=bool, default = False, help='save file as csv as well as parquet')
   
    # args = p.parse_args()

    # # By running the script directly from the command line, it is assumed the file intends to be saved so the save flag is always set to true
    # geotam_to_csv(geotam_txt_file = args.file2proc, date_start = args.start_date, date_end = args.end_date, save = True, saved_file_type = args.saved_file_type)

    