import geotam_processor_copy as gpc

fpath = r"test_data\2023_kc_autogc_w_ws_wd.txt"

gpc.geotam_to_csv(fpath, save = True, save_csv=True)