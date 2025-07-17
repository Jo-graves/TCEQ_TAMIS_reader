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


    TCEQ_Header = get_header(filepath)        #get header from get_header function


    RD =  pd.read_csv(filepath, sep = ',', header = TCEQ_Header, #open csv
            skiprows = [], skip_blank_lines = False, encoding = 'ascii',
            encoding_errors = 'replace') 
    
    RD.dropna(axis = 'columns', inplace = True ) #drop na values - there's usually a bunch
    RD['Datetime'] = RD['Date'].astype(str) + ' '  + RD['Time'] #combine date and time to form datetime
    RD["dt_string"] = pd.to_datetime(RD["Datetime"], format='%Y%m%d %H:%M')


    try:
        tceq_keys = pd.read_table(params) #open tceq parameter info file to match parameter codes to parameters
        tceq_units = pd.read_table(units) #open tceq unit info file to match unit codes to units

    except:
        tceq_keys = pd.read_table(f"{file_path}/ref_files/tceq_parameters.txt") #open tceq parameter info file to match parameter codes to parameters
        tceq_units = pd.read_table(f"{file_path}ref_files/tceq_units.txt") #open tceq unit info file to match unit codes to units


    tceq_keys.rename(columns = {"Parm Code": "Parameter Cd", "Name":"Parameter Name"}, inplace = True)
    tceq_units.rename(columns = {"Code": "Unit Cd", "Description": "Unit Description", "Abbr": "Unit Abbr", "Type": "Unit Type"}, inplace = True)

    # print(tceq_units.columns)
    tceq_units.rename(columns = {"Code": "Unit Cd"}, inplace = True)
    RD_keys = RD.merge(tceq_keys, on = "Parameter Cd", how = "inner")
    RD_u = RD_keys.merge(tceq_units, on = "Unit Cd", how = "inner")
    print(RD_u)

    return RD_u

    # return my_dict

def convert_ref_files():
    file_path = Path(os.path.realpath(__file__)).parent

    params = fr"{file_path}/ref_files/tceq_parameters.txt"
    units = fr"{file_path}/ref_files/tceq_units.txt"

    tceq_keys = pd.read_table(params) 
    tceq_units = pd.read_table(units)

    print(Path(params).with_suffix(".csv"))

    tceq_keys.to_csv(Path(params).with_suffix(".csv"))
    tceq_units.to_csv(Path(units).with_suffix(".csv"))
    

def geotam_to_csv(geotam_txt_file, date_start = None, date_end = None, save = False, save_csv = False):


    df_out = df_splitter(geotam_txt_file)

    df_out["Column_Name"] = "TCEQ " + df_out["Parameter Name"] + " (" + df_out["Unit Abbr"] + ")"

    print(df_out)

    df_clean = df_out[["Value", "Column_Name", "dt_string"]]

    print(df_clean)

    df_clean_piv = df_clean.pivot(index = "dt_string", values = "Value", columns="Column_Name")
    print(df_clean_piv)
    df_clean_piv["Datetime - CST"] = df_clean_piv.index.tz_localize("Etc/GMT+6")
    df_clean_piv.reset_index(inplace = True, drop = True)
    df_clean_piv.columns.name =None

    print(df_clean_piv)

  
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
        if save_csv == True:
                df_clean_piv.to_csv(Path(geotam_txt_file).with_suffix(".csv"))
                print(f"Processd file saved to: {Path(geotam_txt_file).with_suffix(".csv")}")

        
        df_clean_piv.to_parquet(Path(geotam_txt_file).with_suffix(".gzip"))
        print(f"Processd file saved to: {Path(geotam_txt_file).with_suffix(".gzip")}")

    
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
    p.add_argument('--save_as_csv', type=bool, default = False, help='save file as csv as well as parquet')
   
    args = p.parse_args()

    # By running the script directly from the command line, it is assumed the file intends to be saved so the save flag is always set to true
    geotam_to_csv(geotam_txt_file = args.file2proc, date_start = args.start_date, date_end = args.end_date, save = True, save_csv = args.save_as_csv)

    