from pathlib import Path
import os 
import geotam_processor as gp
import importlib
importlib.reload(gp)

file_path = Path(os.path.realpath(__file__)).parent

test_file = f"{file_path}/../tests/2023_kc_autogc_w_ws_wd.txt"

gp.geotam_to_csv(test_file, save = True, saved_file_type="csv")
