from pathlib import Path
import os 
import geotam_processor as gp
import polars_test as pt
import importlib
import pandas as pd
importlib.reload(gp)

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

if __name__ == "__main__":
    # test_pipe()
    # test_comma()
    # test_equivalence_w_files()
    # test_tab()
    test_comma_pl()
    