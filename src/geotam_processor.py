import pandas as pd
import csv
from datetime import datetime
from pathlib import Path
from functools import reduce
from datetime import datetime
from functools import reduce
import sys
import os


class tceq_dataframes: #Create class for dataframes in the df_splitter dictionary 
    def __init__(self,dicty, key):
        self.parameter = key
        self.dictionary = dicty[key][1]
        self.unit = dicty[key][0]
        self.values = dicty[key][1]["Value"]
        self.dates = dicty[key][1]["dt_string"]
        self.tuple = self.dates, self.values, self.unit, self.parameter
        # self.units = dict[key][0]

def dict_processer(dict):
    final_dict = {}
    for key in dict:
        # print(keys)
        dict[key][1].reset_index(drop = True)
        process = tceq_dataframes(dict,key)
        final_dict[key] = process
    return final_dict


def header_getter(filepath):
    with open(filepath, newline = '', errors = 'ignore') as pracfile: #finds which line the header should be - always seems to be 1 less than expected (only if you use skip_blank_lines = false)
            p = 0
            rows = csv.reader(pracfile, delimiter = ',') #returns a list where each entry is the information in each row of the file (define delimiter as ,)
            for row in rows:
                p = p+1
                if "State Cd" in row:
                    # print(p)
                    # print(row)
                    # global TCEQ_Header # Why did I make this global?
                    TCEQ_Header = p-1
    return TCEQ_Header



def df_splitter(filepath):

    file_path = Path(os.path.realpath(__file__)).parent

    params = fr"{file_path}/ref_files/tceq_parameters.txt"
    units = fr"{file_path}/ref_files/tceq_units.txt"

    if params not in sys.path:
        sys.path.append(params)

    if units not in sys.path:
        sys.path.append(units)



    TCEQ_Header = header_getter(filepath)        #get header from header_getter function


    RD =  pd.read_csv(filepath, sep = ',', header = TCEQ_Header, #open csv
            skiprows = [], skip_blank_lines = False, encoding = 'ascii',
            encoding_errors = 'replace') 
    # print(RD.dropna(axis = 'columns'))
    RD.dropna(axis = 'columns', inplace = True ) #drop na values - there's usually a bunch
    RD['Datetime'] = RD['Date'].astype(str) + ' '  + RD['Time'] #combine date and time to form datetime
    RD["dt_string"] = [datetime.strptime(i,'%Y%m%d %H:%M') for i in RD['Datetime']] #create datetime object from strings in "Datetime" - confusing switch this

    Par_cd = RD.groupby('Parameter Cd') #create groupby object grouping by parameters - for creating keys later on
    Unit_cd = RD.groupby('Unit Cd') #create groupby object grouping units - for connecting them to parameters later on 
    parameter_keys =  [key for key, item in Par_cd] #make a list of the parameters (paramter codes - not names)
    parameter_units =  [key for key, item in Unit_cd] #make a list of the units (unit codes - not names
    #print(parameter_units)

    try:
        tceq_keys = pd.read_table(params) #open tceq parameter info file to match parameter codes to parameters
        tceq_units = pd.read_table(units) #open tceq unit info file to match unit codes to units

    except:
        tceq_keys = pd.read_table(f"{file_path}/ref_files/tceq_parameters.txt") #open tceq parameter info file to match parameter codes to parameters
        tceq_units = pd.read_table(f"{file_path}ref_files/tceq_units.txt") #open tceq unit info file to match unit codes to units

    vals = [i for i in tceq_keys.index if tceq_keys.iloc[i,0] in parameter_keys] #compares tceq codes to parameter codes from parameter_keys list - if a parameter code in the tceq parameter file shows up in the data uploaded, it gets saves in this list
    tceq_new = tceq_keys.iloc[vals] #truncate tceq_keys (parameter codes) df based on what is actually present

    unit_vals = [i for i in tceq_units.index if tceq_units.iloc[i,2] in parameter_units] #same as above but with units
    tceq_unit_new = tceq_units.iloc[unit_vals]

    parameters = list(tceq_new.iloc[:,1])
    units = list(tceq_unit_new.iloc[:,1])
    unit_codes = list(tceq_unit_new.iloc[:,2])
    #print(f"units: {units}")
    unit_dict = {}
    param_dict = {}
    my_dict = {}

    for i in range(len(parameter_units)): 
        unit_dict[unit_codes[i]] = units[i]

    print(unit_dict)

    for i in range(len(parameter_keys)):
        unit = unit_dict[Par_cd.get_group(parameter_keys[i]).iloc[0,8]]
        param_dict[ parameter_keys[i]] = unit,  parameters[i]
        my_dict[ parameters[i]] = unit,  Par_cd.get_group( parameter_keys[i])

    return my_dict


    

def geotam_to_csv(geotam_txt_file, date_start = None, date_end = None, save = False, save_csv = False):


    foo = df_splitter(geotam_txt_file)


    f = dict_processer(foo)
    print(f.keys())

    for key in f.keys():
        f[key].dictionary["units"]  = f[key].unit
        f[key].dictionary["parameter"] = f[key].parameter
        f[key].dictionary["Datetime - CST"] = [datetime.strptime(i, "%Y%m%d %H:%M").strftime("%Y-%m-%d %H:%M:%S") for i in f[key].dictionary["Datetime"]]


    df_list = [f[key].dictionary[["Datetime - CST", "parameter", "Value", "units"]] for key in f.keys()] 
    df_pivot_list = [df.pivot(index='Datetime - CST', columns='parameter', values='Value') for df in df_list]
    df_pivot_list_reset = [df.reset_index() for df in df_pivot_list]
    df_to_check = df_pivot_list[0]

    # print(df_to_check)


    #I see I concatenated them...
    # df_out = pd.concat(df_list)
    df_out = reduce(lambda  x,y: pd.merge(x,y,on="Datetime - CST",
                                                how='outer'), df_pivot_list_reset)

    for column in df_out.columns:
        if column != "Datetime - CST":
            unit_for_col = f[column].dictionary["units"].iloc[0]
            df_out.rename(columns = {f"{column}": f"TCEQ {column} ({unit_for_col})"}, inplace=True)
            # print(unit_for_col)

    # print(df_out)

    df_out_sort = df_out.sort_values("Datetime - CST")
    df_out_sort.reset_index(inplace = True, drop = True)

    df_out_sort_copy = df_out_sort.copy().drop(columns = "Datetime - CST")
    df_out_sort_copy['Datetime - CST'] = pd.to_datetime(df_out_sort["Datetime - CST"])

    df_out_sort_copy["Datetime - CST"] = pd.DatetimeIndex(df_out_sort_copy["Datetime - CST"]).tz_localize("Etc/GMT+6")
    df_out_sort_copy["Datetime - CDT"] = pd.DatetimeIndex(df_out_sort_copy["Datetime - CST"]).tz_convert("CST6CDT")

    if date_start == None:
        date_start = df_out_sort_copy["Datetime - CST"].iloc[0]
    else:
        date_start == date_start

    if date_end == None:
        date_end = df_out_sort_copy["Datetime - CST"].iloc[-1]
    else:
        date_end == date_end

    df_out_sort_copy = df_out_sort_copy.copy()[df_out_sort_copy.copy()["Datetime - CST"].between(date_start, date_end)]

    if save == True:
        if save_csv == True:
                df_out_sort_copy.to_csv(Path(geotam_txt_file).with_suffix(".csv"))
                print(f"Processd file saved to: {Path(geotam_txt_file).with_suffix(".csv")}")

        
        df_out_sort_copy.to_parquet(Path(geotam_txt_file).with_suffix(".gzip"))
        print(f"Processd file saved to: {Path(geotam_txt_file).with_suffix(".gzip")}")

    
    return df_out_sort_copy


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

    