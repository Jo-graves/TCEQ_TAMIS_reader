#%%
import polars as pl
from pathlib import Path
import os
import pandas as pd



def get_TCEQ_header_row_number(filepath: str):
    '''
    Finds the number of rows from the start of a TCEQ GeoTAMIS report that 
    the data column headers are located
    
    Parameters
    ----------
    filepath: str
        filepath to GeoTAMIS data to open and parse

        
    Returns
    ----------
    int
        The number of rows from the start of the TCEQ GeoTAMIS report that 
        the data column headers are located

    
    Example
    ---------
    # TODO


    
    '''
    with open(filepath, "r+") as pracfile: 
            for line_index, line in enumerate(pracfile):
                if "State Cd" in line:
                    return line_index
                
def polars_convert_date_and_time_columns_to_datetime(df: pl.DataFrame,
                                              date_column: str = "Date",
                                              date_format: str = "%Y%m%d",
                                              time_column: str = "Time",
                                              time_format: str = "%H:%M",
                                              tzone_in: str = None, 
                                              tzone_out: str = None ) -> pl.DataFrame:
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
        Default: None 
        
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
    # TODO


    '''
    df = df.with_columns(pl.col(date_column, time_column).cast(str))
    df = df.with_columns(pl.col(date_column).str.to_date(format=date_format))
    df = df.with_columns(pl.col(time_column).str.to_time(format=time_format))
    df = df.with_columns(pl.col(date_column).dt.combine(pl.col(time_column)).alias("Datetime"))

    # Only add tzone info if tzone_in is not None
    # Only convert tzone if both tzone_in and tzone_out is specified
    if tzone_in is not None:
        df = df.with_columns(pl.col("Datetime").dt.replace_time_zone(tzone_in))
    
        if tzone_out is not None:
            df = df.with_columns(pl.col("Datetime").dt.convert_time_zone(tzone_out))


    df = df.with_columns(pl.exclude(date_column, time_column))
    return df
   
def pl_drop_col_if_all_null(df):

    '''Drops all columns in a dataframe which have only null records
    
    Parameters
    ----------
    df: pl.DataFrame
        dataframe to drop columns from


    Returns
    ----------
    pl.Dataframe
        view of original dataframew with null columns removed


    Example
    ----------
    #TODO

    '''

    df = df[[column.name for column in df if not (column.null_count() == df.height)]]
    return df

def read_tceq_to_pl_dataframe(filepath: str | Path, 
                     tzone_in: str = "Etc/GMT+6", 
                     tzone_out: str = "Etc/GMT+6",
                     **kwargs):

    '''
    Reads a TCEQ GeoTAMIS report (.txt), extracts the data,  processes the timezone info, 
    and returns a polars dataframe


    Parameters
    -----------
    filepath: str | Path
        filepath or Path (e.g. returned from pathlib.Path()) to GeoTAMIS 
        report to read and process
    
    tzone_in: str
        Timezone code for date and times being read in. TCEQ TAMIS data is presented in LST. 
        Default: None 

    tzone_out: str
        Timezone code output data is converted to.
        Default: ETC/GMT+6 -- the timezone covering most of Texas. Stations near El-Paso will be ETC/GMT+7.

    **kwargs: str
        Additional arguments passed to tzone conversion. See polars_convert_data_and_time_columns_to_datetime().


    
    Returns
    ---------
    pl.Dataframe
        polars dataframe in wide format containing GeoTAMIS records


    Example
    --------
    #TODO
    

    See also
    ---------
    polars_convert_date_and_time_columns_to_datetime()

    Info on time-tagging conventions from GeoTAMIS: https://www.tceq.texas.gov/cgi-bin/compliance/monops/agc_daily_summary.pl
    

    Helpful information
    -------------------

    GeoTAMIS reports data collected over some timestep at the start of the timestep 
    (e.g., data collected between 1 p.m. and 2 p.m. is reported at 1 p.m.). Data is also
    reported in local standard time (LST). Standard time does not consider daylight savings time.
    Most of Texas is in Central Standard Time (CST), except for portions of the far west of the state
    which is in mountain time (MT).

    Central standard time tzone code: Etc/GMT+6
    Mountain time tzone code: Etc/GMT+7

    '''
     
    # Read in table
    TCEQ_HEADER = get_TCEQ_header_row_number(filepath)
    print(TCEQ_HEADER)
    df = pl.read_csv(filepath, has_header=True, skip_rows=TCEQ_HEADER)

    # drop columns if all values are null
    df = pl_drop_col_if_all_null(df)

    # Get datetime columns
    df = polars_convert_date_and_time_columns_to_datetime(df, tzone_in=tzone_in, tzone_out=tzone_out, **kwargs)

    return df
     
def get_clean_reference_info():

    '''
    Pulls in GeoTAMIS parameter, unit, and site codes for labeling raw GeoTAMIS data'''
 
    # Get this script's path in user filesystem (allows for os-independent execution)
    script_path = Path(os.path.realpath(__file__)).parent

    # Grab TCEQ param, units, and site_info filepaths relative to this script, then read in
    TCEQ_parameter_codes_fpath = fr"{script_path}/ref_files/tceq_parameters.csv"
    TCEQ_unit_codes_fpath = fr"{script_path}/ref_files/tceq_units.csv"
    TCEQ_site_info_codes_fpath = fr"{script_path}/ref_files/tceq_site_locations.csv"

    tceq_param_codes = pd.read_csv(TCEQ_parameter_codes_fpath) 
    tceq_unit_codes = pd.read_csv(TCEQ_unit_codes_fpath)
    tceq_site_info_codes = pd.read_csv(TCEQ_site_info_codes_fpath)

    return tceq_param_codes, tceq_unit_codes, tceq_site_info_codes



def main(fpath):
    #  df = read_tceq_to_pl_dataframe(fpath)
    #  print(df)
    print(get_clean_reference_info())


if __name__ == "__main__":
     

    file_path = Path(os.path.realpath(__file__)).parent
    # print(type(file_path))
    fpath = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_comma.txt"
    
    main(fpath)

# %%
