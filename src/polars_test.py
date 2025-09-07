#%%
import polars as pl
from pathlib import Path
import os
import pandas as pd



def get_header(filepath):
    '''Gets header of filepath (e.g., where column headers are located)'''
    with open(filepath, "r+") as pracfile: 
            for line_index, line in enumerate(pracfile):
                if "State Cd" in line:
                    return line_index
                
def convert_date_and_time_columns_to_datetime(df: pl.DataFrame,
                                              date_column: str = "Date",
                                              date_format: str = "%Y%m%d",
                                              time_column: str = "Time",
                                              time_format: str = "%H:%M",
                                              tzone_in: str = "Etc/GMT+6", 
                                              tzone_out: str = "Etc/GMT+6" ) -> pl.DataFrame:
    '''
    Create combined datetime column with tzone info from individual date and time columns in a polars dataframe

    
    Parameters
    -----------
    df: pl.Dataframe
        Polars dataframe with individual date and time columns to convert
    
    date_column: str
        Name of column containing date information

    date_format: str
        datetime format code representing the format of the date_column data
    
    time_column: str
        Name of column containing time information

    time_format: str
        datetime format code representing the format of the time_column data

    tzone_in: str
        Timezone code for date and times being read in. TCEQ TAMIS data is presented in LST. 
        Default: ETC/GMT+6 -- the timezone covering most of Texas. Stations near El-Paso will be ETC/GMT+7.

    tzone_out: str
        Timezone code output data is converted to.
        Default: ETC/GMT+6 -- the timezone covering most of Texas. Stations near El-Paso will be ETC/GMT+7.

        
    Returns
    ---------

    pl.Dataframe
        View of the original polars dataframe passed to the function now with a new "Datetime" column 
        that is the combination of the original date and time columns


    Example
    ---------




    '''
    df = df.with_columns(pl.col(date_column, time_column).cast(str))
    df = df.with_columns(pl.col(date_column).str.to_date(format=date_format))
    df = df.with_columns(pl.col(time_column).str.to_time(format=time_format))
    df = df.with_columns(pl.col(date_column).dt.combine(pl.col(time_column)).alias("Datetime"))
    df = df.with_columns(pl.col("Datetime").dt.replace_time_zone(tzone_in).dt.convert_time_zone(tzone_out))
    df = df.with_columns(pl.exclude(date_column, time_column))
    return df
   
def read_tceq_to_csv(filepath, tzone_in = "Etc/GMT+6", tzone_out = "Etc/GMT+6"):

    '''
    
    See also
    ---------
    Info on time-tagging conventions from GeoTAMIS: https://www.tceq.texas.gov/cgi-bin/compliance/monops/agc_daily_summary.pl
    
    '''
     
    # Read in table
    TCEQ_HEADER = get_header(filepath)
    print(TCEQ_HEADER)
    df = pl.read_csv(filepath, has_header=True, skip_rows=TCEQ_HEADER)

    # drop columns if all values are null
    df = df[[s.name for s in df if not (s.null_count() == df.height)]]

    pf = pd.read_csv

    # Get datetime columns
    df = convert_date_and_time_columns_to_datetime(df, tzone_in=tzone_in, tzone_out=tzone_out)

  
    return df
     
def get_clean_reference_info():
 
    script_path = Path(os.path.realpath(__file__)).parent

    params = fr"{script_path}/ref_files/tceq_parameters.csv"
    units = fr"{script_path}/ref_files/tceq_units.csv"
    site_info = fr"{script_path}/ref_files/tceq_site_locations.txt"

    try:
        tceq_keys = pd.read_table(params) #open tceq parameter info file to match parameter codes to parameters
        tceq_units = pd.read_table(units) #open tceq unit info file to match unit codes to units
        tceq_site_info = pd.read_table(site_info)

    except:
        tceq_keys = pd.read_table(f"{script_path}/ref_files/tceq_parameters.txt") #open tceq parameter info file to match parameter codes to parameters
        tceq_units = pd.read_table(f"{script_path}/ref_files/tceq_units.txt") #open tceq unit info file to match unit codes to units
        tceq_site_info = pd.read_table(f"{script_path}/ref_files/tceq_site_locations.txt")


def main(fpath):
     df = read_tceq_to_csv(fpath)
     print(df)

if __name__ == "__main__":
     
    file_path = Path(os.path.realpath(__file__)).parent
    fpath = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_comma.txt"
    
    main(fpath)

# %%
