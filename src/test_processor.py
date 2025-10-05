#%%
from pathlib import Path
import os 
import geotam_processor as gp
import polars_test as pt
import importlib
import pandas as pd
import polars as pl
importlib.reload(gp)
importlib.reload(pt)
import polars.testing as ptesting

file_path = Path(os.path.realpath(__file__)).parent

def test_comma():
    # test_file = f"{file_path}/../tests/2023_kc_autogc_w_ws_wd.txt"
    test_file = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_comma.txt"
    gp.geotam_to_structured_data(test_file, save = True, saved_file_type="csv")

def test_pipe():
    test_file = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_pipe.txt"
    gp.geotam_to_structured_data(test_file, save = True, saved_file_type="csv")

def test_tab():
    test_file = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_tab.txt"
    gp.geotam_to_structured_data(test_file, save = True, saved_file_type="csv")

def test_equivalence(file_1, file_2):
    df1 = pd.read_csv(file_1)
    df2 = pd.read_csv(file_2)

    print(df1.equals(df2))

def test_equivalence_w_files():
    f1 = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_pipe.csv"
    f2 = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_comma.csv"
    f3 = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_tab.csv"

    test_equivalence(f2, f3)
 
def test_comma_pl():
    f1 = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_comma.txt"
    pt.read_tceq_to_pl_dataframe(f1, save = True, saved_file_type="csv")

def test_pipe_pl():
    f1 = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_pipe.txt"
    pt.read_tceq_to_pl_dataframe(f1, save = True, saved_file_type="csv")

def test_tab_pl():
    f1 = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_tab.txt"
    pt.read_tceq_to_pl_dataframe(f1, save = True, saved_file_type="csv")

def proc_tceq_formatted_ethane():
    f1 = f"{file_path}/../tests/october_2023_ethane_ppbv_from_tceq_for_comparison.csv"
    df = pl.read_csv(f1).select(pl.exclude("Rec"))
    df = df.unpivot(index = "Day")
    # print(df)
    df = df.with_columns(
    pl.when(pl.col("variable").str.contains("duplicated_0")).then(pl.col("variable").str.strip_suffix(":00_duplicated_0")).otherwise(pl.col("variable").str.strip_suffix(":00")).alias("hour"))
    df = df.with_columns(pl.col("hour").cast(int))
    df = df.with_columns(pl.when(pl.col("variable").str.contains("duplicated_0")).then(pl.col("hour")+12).otherwise(pl.col("hour")))
    df = df.with_columns(pl.col("value").cast(pl.Float64, strict = False))
    df = df.rename({"value": "TCEQ Ethane (ppbv)"})
    # df = df.with_columns(pl.col("TCEQ Ethane (ppbv)").cast(pl.Float64))
    df = df.with_columns(
    pl.datetime(
        2023,
        10,
        pl.col("Day"),
        pl.col("hour"),
        time_zone="Etc/GMT+6",
    ).alias("Datetime"))

    df = df.select(pl.col("Datetime", "TCEQ Ethane (ppbv)")).sort("Datetime")

    return df

def proc_tceq_formatted_ethane_2025():
    f1 = f"{file_path}/../tests/202503_kc_autogc_formatted_tceq_ethane.txt"
    df = pl.read_csv(f1).select(pl.exclude("Rec"))
    df = df.unpivot(index = "Day")
    # print(df)
    df = df.with_columns(
    pl.when(pl.col("variable").str.contains("duplicated_0")).then(pl.col("variable").str.strip_suffix(":00_duplicated_0")).otherwise(pl.col("variable").str.strip_suffix(":00")).alias("hour"))
    df = df.with_columns(pl.col("hour").cast(int))
    df = df.with_columns(pl.when(pl.col("variable").str.contains("duplicated_0")).then(pl.col("hour")+12).otherwise(pl.col("hour")))
    df = df.with_columns(pl.col("value").cast(pl.Float64, strict = False))
    df = df.rename({"value": "TCEQ Ethane (ppbv)"})
    # df = df.with_columns(pl.col("TCEQ Ethane (ppbv)").cast(pl.Float64))
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

def compare_tceq_formatted_and_processed_data_ethane():
    f1 = f"{file_path}/../tests/2023_kc_autogc_w_ws_wd.txt"
    df = pt.read_tceq_to_pl_dataframe(f1, save = False, saved_file_type="csv")
    df = df.select(pl.col("Datetime", "TCEQ Ethane (ppbv)"))
    df = df.with_columns(pl.col("TCEQ Ethane (ppbv)").round(2))

    df2 = proc_tceq_formatted_ethane()
    df = df.filter(pl.col("Datetime").is_between(pl.datetime(2023,10,1, time_zone = "Etc/GMT+6"), pl.datetime(2023,10,31,23, time_zone = "Etc/GMT+6"))).sort('Datetime')

    df = df.drop_nulls()
    df2 = df2.drop_nulls()

    # If no error, they're essentially the same
    ptesting.assert_frame_equal(df, df2, check_exact = False, abs_tol = 0.1)


def compare_tceq_formatted_and_processed_data_ethane_2025():
    f1 = f"{file_path}/../tests/2025_kc_autogc_w_ws_wd_tab.txt"
    df = pt.read_tceq_to_pl_dataframe(f1, save = False, saved_file_type="csv")
    df = df.select(pl.col("Datetime", "TCEQ Ethane (ppbv)"))
    df = df.with_columns(pl.col("TCEQ Ethane (ppbv)").round(2))

    df2 = proc_tceq_formatted_ethane_2025()
    # df2 = df2.filter(pl.col("Datetime").is_between(pl.datetime(2025,4,8, time_zone = "Etc/GMT+6"), pl.datetime(2023,4,20,23, time_zone = "Etc/GMT+6"))).sort('Datetime')

    # print(df2)
    # print(df)
    df = df.filter(pl.col("Datetime").is_between(pl.datetime(2025,4,8, time_zone = "Etc/GMT+6"), pl.datetime(2025,4,20,23, time_zone = "Etc/GMT+6"))).sort('Datetime').upsample("Datetime", every="1h", maintain_order = True)
    df2 = df2.filter(pl.col("Datetime").is_between(pl.datetime(2025,4,8, time_zone = "Etc/GMT+6"), pl.datetime(2025,4,20,23, time_zone = "Etc/GMT+6"))).sort('Datetime').upsample("Datetime", every="1h", maintain_order = True)
    
    # print(df, df2)


    # If no error, they're essentially the same
    ptesting.assert_frame_equal(df, df2, check_exact = False, abs_tol = 0.1)
    

    


if __name__ == "__main__":
    # test_pipe()
    # test_comma()
    # test_equivalence_w_files()
    # test_tab()
    # test_comma_pl()
    # test_pipe_pl()
    # test_pipe_pl()
    # test_tab_pl()
    # proc_tceq_formatted()
    # compare_tceq_formatted_and_processed_data_ethane()
    compare_tceq_formatted_and_processed_data_ethane_2025()


    

# %%
