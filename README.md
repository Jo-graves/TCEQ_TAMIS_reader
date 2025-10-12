# GeoTAM Reader

## Purpose
The GeoTAM reader converts raw measurement report files from the [Texas Air Monitoring Information System (TAMIS)](https://www17.tceq.texas.gov/tamis/index.cfm?fuseaction=home.welcome) into human-interpretable polars dataframes which can then be saved to csv and parquet (.gzip) formats. TAMIS is maintained by the [Texas Commission on Environmental Quality (TCEQ)](https://www.tceq.texas.gov/). Many thanks to TCEQ for providing these services. 


## Installation
This repo is a python package so it can be cloned to a local filesystem and then installed into a virtual environment 
(similar to using conda or pip to install packages directly from online package channels).
After the package is installed, the main modules can be imported and used in any scripts running in the virtual environment.

### To install the package:
1. Clone the repository into any location on your local filesystem using `git clone clone_path_from_github`
2. Activate a virtual environment (e.g., `conda activate my_env_name`)
3. Install conda to the virtual environment if it is not installed already `conda install anaconda::pip`
4. Run `pip install local_path/to/cloned/repo/TCEQ_geotam_reader`

#### Example using https and cloning directory to Desktop
From the command line:
```
>>> cd /Path/to/Desktop                               # Change directory to Desktop
>>> conda activate my_virtual_environment             # activate virtual environment
>>> conda install anaconda::pip                       # Optional: install pip 
                                                      # (if not already installed) to 
                                                      # conda environment

>>> git clone https://github.com/this_repo            # Clone repo to cwd (Desktop)
>>> pip install /Path/to/Desktop/TCEQ_geotam_reader   # Install package to virtual environment
```
pip needs just the filepath to the directory holding the setup.py and *.toml files to install the package. 

The package can be optionally installed in development mode if you plan to edit the source code and would like the edits to appear automatically without having the update the code (e.g., `pip update TCEQ_geotam_viewer`). This can be done using the -e flag when installing with pip:  \
`pip install -e /Path/to/Desktop/TCEQ_geotam_reader`


## Data Access
Data can be accessed at the TCEQ TAMIS data request page:
https://www17.tceq.texas.gov/tamis/index.cfm?fuseaction=report.main


> Landing page
> 
> <img width="822" height="249" alt="image" src="https://github.com/user-attachments/assets/2fc065bf-6fe0-4ebc-a0e0-e9e50e3ef50b" />



### GeoTAM Viewer
The [geoTAM viewer](https://tceq.maps.arcgis.com/apps/webappviewer/index.html?id=ab6f85198bda483a997a6956a8486539) is a useful resource to see how TAMIS sites are spatially related 

<img width="1369" height="876" alt="image" src="https://github.com/user-attachments/assets/d8ec705d-0104-41dc-a22b-bab645fe5aa3" />


## Reference tables
Several reference tables are included in the package. Additional information is provided in the [docs](./docs/reference_table_information.md)



## Documentation

The primary function is
### read_tceq_to_pl_dataframe()

It is used to read GeoTAMIS raw data files and convert them to a polars dataframe with human-interpretable data. Data is returned as a polars dataframe. Alternatively, data can also be saved directly to .csv or .gzip (parquet) file formats. 

Parameters
-----------
    filepath: str | Path
        filepath or Path (e.g. returned from pathlib.Path()) to GeoTAMIS 
        report to read and process
    
    tzone_in: str
        Timezone code for date and times being read in. TCEQ TAMIS data is presented in LST. 
        Default: Etc/GMT+6

    tzone_out: str
        Timezone code output data is converted to.
        Default: Etc/GMT+6

    save: bool
        Whether to save the data to a file or not. If true, the processed file is saved to the same path as the original .txt file with an updated file extension (.csv or .gzip)
        Default: False
    
    saved_file_type:
        Options: "parquet" or "csv"
        Save file as either csv or parquet (.gzip) file format. Parquet maintains datetime information precluding the need  for datetime conversions after reading in the data to polars or pandas later.  

    **kwargs: str
        Additional arguments passed to tzone conversion. See polars_convert_data_and_time_columns_to_datetime().


Returns
---------
    pl.Dataframe
        polars dataframe in wide format containing GeoTAMIS records with descriptive column names

Example use
------------

Sample data from raw GeoTAMIS report: 
```
AQS Raw Data (RD) Transaction Report, Version 1.6, 3/11/2011
Run By: TAMIS User
Run Date: 08/29/2025 16:58:22,  Run Time:      3.00 seconds
Fields Delimited by: ,  Action: I  Caution!  This report does not use the pipe (|) delimiter required in AQS Transaction reports.
Measurements reported from: 04/07/2025 00:00:00 up to but not including: 04/22/2025 00:00:00
Sample Duration Code: 1  Report in AQS Units: N
Report only valid data: Y  Validation levels included (0,1,2,3): 3
Only allow AQS codes: N  Column headings included: Y
Report Missing Measurements: N  Check for Negative Measurements: N
Comment: 
Transaction Type,Action,State Cd,County Cd,Site ID,Parameter Cd,POC,Dur Cd,Unit Cd,Meth Cd,Date,Time,Value,Null Data Cd,Col Freq,Mon Protocol ID,Qual Cd 1,Qual Cd 2,Qual Cd 3,Qual Cd 4,Qual Cd 5,Qual Cd 6,Qual Cd 7,Qual Cd 8,Qual Cd 9,Qual Cd 10,Alternate MDL,Uncertainty Value
RD,I,48,255,1070,43202,01,1,008,128,20250407,00:00,55.055,,,,,,,,,,,,,,,
RD,I,48,255,1070,43202,01,1,008,128,20250407,03:00,44.3327,,,,,,,,,,,,,,,
RD,I,48,255,1070,43202,01,1,008,128,20250407,04:00,35.5938,,,,,,,,,,,,,,,
RD,I,48,255,1070,43202,01,1,008,128,20250407,05:00,40.8661,,,,,,,,,,,,,,,
RD,I,48,255,1070,43202,01,1,008,128,20250407,06:00,41.76815,,,,,,,,,,,,,,,
RD,I,48,255,1070,43202,01,1,008,128,20250407,07:00,36.3294,,,,,,,,,,,,,,,
...
...
``` 
After installing the package, if the data is in a file with path "filepath":

```
>>> import tceq_geotam_processor as tgp
>>> from pathlib import Path
>>> filepath = Path('/path/to/geotam/data')

>>> df = tgp.read_tceq_to_pl_dataframe(fpath = filepath, 
                                        tzone_in = "Etc/GMT+6", 
                                        tzone_out = "Etc/GMT+6",
                                        save = False)
>>> df.columns
['Datetime', 'Site Name', 'Site ID', 'TCEQ Ethane (ppbv)', 'TCEQ Ethylene (ppbv)', ...,'TCEQ Wind Direction - Resultant (deg)', 'TCEQ Outdoor Temperature (Deg F)']


>>> df.head(5)

┌────────────┬───────────┬─────────┬───────────┬───┬───────────┬───────────┬───────────┬───────────┐
│ Datetime   ┆ Site Name ┆ Site ID ┆ TCEQ      ┆ … ┆ TCEQ 1,2, ┆ TCEQ Wind ┆ TCEQ Wind ┆ TCEQ      │
│ ---        ┆ ---       ┆ ---     ┆ Ethane    ┆   ┆ 3-Trimeth ┆ Speed -   ┆ Direction ┆ Outdoor   │
│ datetime[μ ┆ str       ┆ i64     ┆ (ppbv)    ┆   ┆ ylbenzene ┆ Resultant ┆ -         ┆ Temperatu │
│ s,         ┆           ┆         ┆ ---       ┆   ┆ (p…       ┆ (m…       ┆ Resultan… ┆ re (Deg … │
│ Etc/GMT+6] ┆           ┆         ┆ f64       ┆   ┆ ---       ┆ ---       ┆ ---       ┆ ---       │
│            ┆           ┆         ┆           ┆   ┆ f64       ┆ f64       ┆ f64       ┆ f64       │
╞════════════╪═══════════╪═════════╪═══════════╪═══╪═══════════╪═══════════╪═══════════╪═══════════╡
│ 2025-04-07 ┆ Karnes    ┆ 1070    ┆ 55.055    ┆ … ┆ 0.0       ┆ 3.54543   ┆ 328.241   ┆ 52.3258   │
│ 00:00:00   ┆ County    ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
│ -06        ┆           ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
│ 2025-04-07 ┆ Karnes    ┆ 1070    ┆ 44.3327   ┆ … ┆ 0.0       ┆ 3.42917   ┆ 345.279   ┆ 49.3633   │
│ 03:00:00   ┆ County    ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
│ -06        ┆           ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
│ 2025-04-07 ┆ Karnes    ┆ 1070    ┆ 35.5938   ┆ … ┆ 0.0       ┆ 5.29942   ┆ 319.843   ┆ 47.1892   │
│ 04:00:00   ┆ County    ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
│ -06        ┆           ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
│ 2025-04-07 ┆ Karnes    ┆ 1070    ┆ 40.8661   ┆ … ┆ 0.0       ┆ 5.49455   ┆ 324.205   ┆ 45.5842   │
│ 05:00:00   ┆ County    ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
│ -06        ┆           ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
│ 2025-04-07 ┆ Karnes    ┆ 1070    ┆ 41.76815  ┆ … ┆ 0.0       ┆ 3.91333   ┆ 318.764   ┆ 45.3275   │
│ 06:00:00   ┆ County    ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
│ -06        ┆           ┆         ┆           ┆   ┆           ┆           ┆           ┆           │
└────────────┴───────────┴─────────┴───────────┴───┴───────────┴───────────┴───────────┴───────────┘
```
Saving
-------
```
>>> df = tgp.read_tceq_to_pl_dataframe(fpath = filepath, 
                                        tzone_in = "Etc/GMT+6", 
                                        tzone_out = "Etc/GMT+6",
                                        save = True, 
                                        saved_file_type = "parquet")

Processed file saved to: /path/to/filepath.gzip
```

## Additional documention

Additional documention can be found in the [docs](./docs/pydoc_docs.md)


