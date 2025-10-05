#%%
from pathlib import Path
import os 
import src.tceq_geotam_processor as pt
import importlib
import pandas as pd
import polars as pl
importlib.reload(pt)
import polars.testing as ptesting

file_path = Path(os.path.realpath(__file__)).parent

def test_comma():
    '''
    Test if processor works for comma delimited data.
    
    '''
    test_file = f"{file_path}/test_data/2025_kc_autogc_w_ws_wd_comma.txt"
    df = pt.read_tceq_to_pl_dataframe(test_file, save = False)
    assert not df.is_empty()
    return df

def test_pipe():

    '''Test if processor works for pipe-delimited data'''
    test_file = f"{file_path}/test_data/2025_kc_autogc_w_ws_wd_pipe.txt"
    df = pt.read_tceq_to_pl_dataframe(test_file, save = False)
    assert not df.is_empty()
    return df

def test_tab():
    """Test if processor works for tab-delimited data"""
    test_file = f"{file_path}/test_data/2025_kc_autogc_w_ws_wd_tab.txt"
    df = pt.read_tceq_to_pl_dataframe(test_file, save = False)
    assert not df.is_empty()
    return df

def test_equivalence():

    '''
    Test if comma, pipe, and tab data are all processed identically
    
    '''
    df1 = test_comma()
    df2 = test_pipe()
    df3 = test_tab()

    ptesting.assert_frame_equal(df1, df2)
    ptesting.assert_frame_equal(df1, df3)
    ptesting.assert_frame_equal(df2, df3)




def proc_tceq_formatted_ethane_2025():

    '''
    Process data formatted by TCEQ (ethane) for comparing with data processed with TCEQ processor package
    
    '''
    f1 = f"{file_path}/test_data/202503_kc_autogc_formatted_tceq_ethane.txt"
    df = pl.read_csv(f1).select(pl.exclude("Rec"))
    df = df.unpivot(index = "Day")
    
    # Hours are in a.m. p.m. format. This extracts the HH entry from the HH:MM formatted timestamps
    df = df.with_columns(
    pl.when(pl.col("variable").str.contains("duplicated_0")).then(pl.col("variable").str.strip_suffix(":00_duplicated_0")).otherwise(pl.col("variable").str.strip_suffix(":00")).alias("hour"))
    
    #Wouldn't allow casting to int within the above line so do this here and convert to 24 hour timestamps
    df = df.with_columns(pl.col("hour").cast(int))
    df = df.with_columns(pl.when(pl.col("variable").str.contains("duplicated_0")).then(pl.col("hour")+12).otherwise(pl.col("hour")))
    df = df.with_columns(pl.col("value").cast(pl.Float64, strict = False))
    
    df = df.rename({"value": "TCEQ Ethane (ppbv)"})
  
    df = df.with_columns(
    pl.datetime(
        2025,
        4,
        pl.col("Day"),
        pl.col("hour"),
        time_zone="Etc/GMT+6",
    ).alias("Datetime"))

    df = df.select(pl.col("Datetime", "TCEQ Ethane (ppbv)")).sort("Datetime")

    return df


def compare_tceq_formatted_and_processed_data_ethane_2025():

    '''Compare the tceq formatted data and data processed from geotam_processor package
    
    '''
    f1 = f"{file_path}/test_data/2025_kc_autogc_w_ws_wd_tab.txt"
    df = pt.read_tceq_to_pl_dataframe(f1, save = False, saved_file_type="csv")
    df = df.select(pl.col("Datetime", "TCEQ Ethane (ppbv)"))
    df = df.with_columns(pl.col("TCEQ Ethane (ppbv)").round(2))

    df2 = proc_tceq_formatted_ethane_2025()
    
    df = df.filter(pl.col("Datetime").is_between(pl.datetime(2025,4,8, time_zone = "Etc/GMT+6"), pl.datetime(2025,4,20,23, time_zone = "Etc/GMT+6"))).sort('Datetime').upsample("Datetime", every="1h", maintain_order = True)
    df2 = df2.filter(pl.col("Datetime").is_between(pl.datetime(2025,4,8, time_zone = "Etc/GMT+6"), pl.datetime(2025,4,20,23, time_zone = "Etc/GMT+6"))).sort('Datetime').upsample("Datetime", every="1h", maintain_order = True)


    # If no error, they're essentially the same
    ptesting.assert_frame_equal(df, df2, check_exact = False, abs_tol = 0.1)
    


if __name__ == "__main__":

    compare_tceq_formatted_and_processed_data_ethane_2025()
    test_equivalence()


    


# %%
