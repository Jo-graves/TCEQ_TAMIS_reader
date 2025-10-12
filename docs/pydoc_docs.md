# TCEQ TAMIS Processor

Functions
-----------
### **get_TCEQ_header_row_number()** 
Finds the number of rows from the start of a TCEQ TAMIS report that
        the data column headers are located

### Parameters
- **filepath**: str

        filepath to TAMIS data to open and parse


### Returns
- **int**

        The number of rows from the start of the  TCEQ TAMIS report that the data column headers are located


### Example

Sample raw data report:

```
1.   AQS Raw Data (RD) Transaction Report, Version 1.6, 3/11/2011
2.   Run By: TAMIS User
...
12.  Transaction Type,Action,State Cd,County Cd,Site ID,Parameter Cd,...
13.  RD,I,48,255,1070,43202,01,1,008,128,20250407,00:00,55.055,,,,,,,,,,,,,,,
...
```

If data is stored in "filepath":
```
>>> ttp.get_TCEQ_header_row_number(filepath)
12
```


## **get_clean_reference_info()**
Pulls in TAMIS parameter, unit, and site codes for labeling raw TAMIS data

## **get_delimiter**()
Reads the raw file header lines and finds the character string used to
delimit the data columns.

### Parameters

- **filepath**: str | Path
    
        filepath to TAMIS data to open and parse


### Returns

- **str**
    
        The character string used to delimit the data columns.



### Examples


Sample raw data report with comma delimiter:

```
AQS Raw Data (RD) Transaction Report, Version 1.6, 3/11/2011
...
Run Date: 08/29/2025 16:58:22,  Run Time:      3.00 seconds
Fields Delimited by: ,  Action: I  Caution!  This report does not use the pipe (|) delimiter required in AQS Transaction reports.
...
```


If data is stored in "filepath":

```
>>> ttp.get_delimiter(filepath)
,
```


Sample raw data report with tab delimiter:
```
AQS Raw Data (RD) Transaction Report, Version 1.6, 3/11/2011
...
Run Date: 08/29/2025 16:58:22,  Run Time:      3.00 seconds
Fields Delimited by: Tab  Action: I  Caution!  This report does not use the pipe (|) delimiter required in AQS Transaction reports.
...
```

If data is stored in "filepath":
```
>>> ttp.get_delimiter(filepath)
\t
```

## pl_drop_col_if_all_null()
Drops all columns in a dataframe which have only null records.


### Parameters

- **df**: pl.DataFrame
        
        dataframe to drop columns from


### Returns

- **pl.Dataframe**
    
        Original dataframe with null columns removed


### Example

```
>>> d_range = pl.datetime_range(pl.datetime(2025, 4, 7), pl.datetime(2025, 4, 21,23), eager = True, interval="1h")
>>> a = np.arange(0, len(d_range), 1)
>>> df = pl.DataFrame({"Datetime": d_range, "a": a})
>>> df = df.with_columns(pl.lit(None).alias("b"), pl.lit(None).alias("c"))
>>> df
┌─────────────────────┬─────┬──────┬──────┐
│ Datetime            ┆ a   ┆ b    ┆ c    │
│ ---                 ┆ --- ┆ ---  ┆ ---  │
│ datetime[μs]        ┆ i64 ┆ null ┆ null │
╞═════════════════════╪═════╪══════╪══════╡
│ 2025-04-07 00:00:00 ┆ 0   ┆ null ┆ null │
│ 2025-04-07 01:00:00 ┆ 1   ┆ null ┆ null │
│ 2025-04-07 02:00:00 ┆ 2   ┆ null ┆ null │
│ 2025-04-07 03:00:00 ┆ 3   ┆ null ┆ null │
│ 2025-04-07 04:00:00 ┆ 4   ┆ null ┆ null │
│ …                   ┆ …   ┆ …    ┆ …    │
│ 2025-04-21 19:00:00 ┆ 355 ┆ null ┆ null │
│ 2025-04-21 20:00:00 ┆ 356 ┆ null ┆ null │
│ 2025-04-21 21:00:00 ┆ 357 ┆ null ┆ null │
│ 2025-04-21 22:00:00 ┆ 358 ┆ null ┆ null │
│ 2025-04-21 23:00:00 ┆ 359 ┆ null ┆ null │
└─────────────────────┴─────┴──────┴──────┘

>>> df = ttp.pl_drop_col_if_all_null(df = df)
>>> df
┌─────────────────────┬─────┐
│ Datetime            ┆ a   │
│ ---                 ┆ --- │
│ datetime[μs]        ┆ i64 │
╞═════════════════════╪═════╡
│ 2025-04-07 00:00:00 ┆ 0   │
│ 2025-04-07 01:00:00 ┆ 1   │
│ 2025-04-07 02:00:00 ┆ 2   │
│ 2025-04-07 03:00:00 ┆ 3   │
│ 2025-04-07 04:00:00 ┆ 4   │
│ …                   ┆ …   │
│ 2025-04-21 19:00:00 ┆ 355 │
│ 2025-04-21 20:00:00 ┆ 356 │
│ 2025-04-21 21:00:00 ┆ 357 │
│ 2025-04-21 22:00:00 ┆ 358 │
│ 2025-04-21 23:00:00 ┆ 359 │
└─────────────────────┴─────┘
```

### Notes

    There are sometimes many null columns after the raw data is processed. It usually occurs if a station does not have equipment for measuring all of the parameters included in the data access request.  Dropping all null columns manually is usually cumbersome.

## polars_convert_date_and_time_columns_to_datetime()
Create combined datetime column with tzone info from individual date and time columns in a polars dataframe


Parameters
-----------
- **df**: pl.Dataframe
    
        Polars dataframe with individual date and time columns to convert

- **date_column:** str
    
        Name of column containing date information

- **date_format:** str
    
        datetime format code representing the format of the date_column data

- **time_column:** str
    
        Name of column containing time information

- **time_format:** str
    
        datetime format code representing the format of the time_column data

- **tzone_in:** str
    
        Timezone code for date and times being read in. TCEQ TAMIS data is presented in LST.
        Default: None

- **tzone_out:** str
    
        Timezone code output data is converted to.
        Default: None


Returns
---------

- **pl.Dataframe**
    
        The original polars dataframe now with a new "Datetime" column


### Example
```
>>> d_range = pl.datetime_range(pl.datetime(2025, 4, 7), pl.datetime(2025, 4, 21,23), eager = True, interval="1h")
>>> df = pl.DataFrame({"Date": d_range.dt.date().dt.strftime("%Y%m%d").cast(int), "Time": d_range.dt.time().dt.strftime("%H:%M")})
>>> df
┌──────────┬───────┐
│ Date     ┆ Time  │
│ ---      ┆ ---   │
│ i64      ┆ str   │
╞══════════╪═══════╡
│ 20250407 ┆ 00:00 │
│ 20250407 ┆ 01:00 │
│ 20250407 ┆ 02:00 │
│ 20250407 ┆ 03:00 │
│ 20250407 ┆ 04:00 │
│ …        ┆ …     │
│ 20250421 ┆ 19:00 │
│ 20250421 ┆ 20:00 │
│ 20250421 ┆ 21:00 │
│ 20250421 ┆ 22:00 │
│ 20250421 ┆ 23:00 │
└──────────┴───────┘

>>> df2 = ttp.polars_convert_date_and_time_columns_to_datetime(df = df,
                                                                date_column = "Date",
                                                                date_format: str = "%Y%m%d",
                                                                time_column: str = "Time",
                                                                time_format: str = "%H:%M",
                                                                tzone_in: str = "Etc/GMT+6",
                                                                tzone_out: str = "Etc/GMT+6")
>>> df2
┌────────────┬──────────┬─────────────────────────┐
│ Date       ┆ Time     ┆ Datetime                │
│ ---        ┆ ---      ┆ ---                     │
│ date       ┆ time     ┆ datetime[μs, Etc/GMT+6] │
╞════════════╪══════════╪═════════════════════════╡
│ 2025-04-07 ┆ 00:00:00 ┆ 2025-04-07 00:00:00 -06 │
│ 2025-04-07 ┆ 03:00:00 ┆ 2025-04-07 03:00:00 -06 │
│ 2025-04-07 ┆ 04:00:00 ┆ 2025-04-07 04:00:00 -06 │
│ 2025-04-07 ┆ 05:00:00 ┆ 2025-04-07 05:00:00 -06 │
│ 2025-04-07 ┆ 06:00:00 ┆ 2025-04-07 06:00:00 -06 │
│ …          ┆ …        ┆ …                       │
│ 2025-04-21 ┆ 19:00:00 ┆ 2025-04-21 19:00:00 -06 │
│ 2025-04-21 ┆ 20:00:00 ┆ 2025-04-21 20:00:00 -06 │
│ 2025-04-21 ┆ 21:00:00 ┆ 2025-04-21 21:00:00 -06 │
│ 2025-04-21 ┆ 22:00:00 ┆ 2025-04-21 22:00:00 -06 │
│ 2025-04-21 ┆ 23:00:00 ┆ 2025-04-21 23:00:00 -06 │
└────────────┴──────────┴─────────────────────────┘

```
### Notes

Etc/GMT+6 is the timezone covering most of Texas. Stations near El-Paso will be Etc/GMT+7.

## pull_ref_info()

    Use pathlib.resources to pull reference file for for labeling raw TAMIS data

## read_and_extract_tceq_data_to_df()
    Reads a TCEQ TAMIS report (.txt), extracts the data, processes the timezone info, and returns a polars dataframe. Data is described by parameter and unit codes.


### Parameters

- **filepath:** str | Path
    
        filepath or Path (e.g. returned from pathlib.Path()) to TAMIS report to read and process

- **tzone_in:** str
    
        Timezone code for date and times being read in. TCEQ TAMIS data is presented in LST.
        Default: Etc/GMT+6

- **tzone_out:** str
    
        Timezone code output data is converted to. 
        Default: Etc/GMT+6

- ****kwargs**: str
    
        Additional arguments passed to tzone conversion. 



### Returns
- **pl.Dataframe**
    
        polars dataframe in wide format containing TAMIS records


### Example

Sample data from raw TAMIS report:
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
```
If data is in file at "filepath":
```
>>> df = ttp.read_and_extract_tceq_data_to_df(fpath = filepath,
                                                tzone_in = "Etc/GMT+6",
                                                tzone_out = "Etc/GMT+6")
>>> df.columns
['Transaction Type', 'Action', 'State Cd', 'County Cd', 'Site ID', 'Parameter Cd', 'POC', 'Dur Cd', 'Unit Cd', 'Meth Cd', 'Date', 'Time', 'Value', 'Datetime']

>>> df.head(5)

┌─────────────┬────────┬──────────┬───────────┬───┬────────────┬──────────┬──────────┬─────────────┐
│ Transaction ┆ Action ┆ State Cd ┆ County Cd ┆ … ┆ Date       ┆ Time     ┆ Value    ┆ Datetime    │
│ Type        ┆ ---    ┆ ---      ┆ ---       ┆   ┆ ---        ┆ ---      ┆ ---      ┆ ---         │
│ ---         ┆ str    ┆ i64      ┆ i64       ┆   ┆ date       ┆ time     ┆ f64      ┆ datetime[μs │
│ str         ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ ,           │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ Etc/GMT+6]  │
╞═════════════╪════════╪══════════╪═══════════╪═══╪════════════╪══════════╪══════════╪═════════════╡
│ RD          ┆ I      ┆ 48       ┆ 255       ┆ … ┆ 2025-04-07 ┆ 00:00:00 ┆ 55.055   ┆ 2025-04-07  │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ 00:00:00    │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ -06         │
│ RD          ┆ I      ┆ 48       ┆ 255       ┆ … ┆ 2025-04-07 ┆ 03:00:00 ┆ 44.3327  ┆ 2025-04-07  │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ 03:00:00    │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ -06         │
│ RD          ┆ I      ┆ 48       ┆ 255       ┆ … ┆ 2025-04-07 ┆ 04:00:00 ┆ 35.5938  ┆ 2025-04-07  │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ 04:00:00    │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ -06         │
│ RD          ┆ I      ┆ 48       ┆ 255       ┆ … ┆ 2025-04-07 ┆ 05:00:00 ┆ 40.8661  ┆ 2025-04-07  │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ 05:00:00    │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ -06         │
│ RD          ┆ I      ┆ 48       ┆ 255       ┆ … ┆ 2025-04-07 ┆ 06:00:00 ┆ 41.76815 ┆ 2025-04-07  │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ 06:00:00    │
│             ┆        ┆          ┆           ┆   ┆            ┆          ┆          ┆ -06         │
└─────────────┴────────┴──────────┴───────────┴───┴────────────┴──────────┴──────────┴─────────────┘
```


### See also
`polars_convert_date_and_time_columns_to_datetime`

Info on time-tagging conventions from TAMIS: https://www.tceq.texas.gov/cgi-bin/compliance/monops/agc_daily_summary.pl


### Helpful information


TAMIS reports data collected over a given time interval at the beginning of the timestep
(e.g., data collected between 1 p.m. and 2 p.m. is reported at 1 p.m.). Data is also
reported in local standard time (LST). Standard time does not consider daylight savings time.
Most of Texas is in Central Standard Time (CST), except for portions of the far west of the state
which is in mountain time (MT).

**Central standard time tzone code:** Etc/GMT+6

**Mountain time tzone code:** Etc/GMT+7

**List of tz codes:** https://gist.github.com/heyalexej/8bf688fd67d7199be4a1682b3eec7568


## read_tceq_to_pl_dataframe() 
    Read TAMIS raw data file and convert to a polars dataframe with human-interpretable data. Data can be saved directly to .csv or .gzip (parquet) file formats.

### Parameters

- **filepath:** str | Path
    
        filepath or Path (e.g. returned from pathlib.Path()) to TAMIS report to read and process

- **tzone_in:** str
    
        Timezone code for date and times being read in. TCEQ TAMIS data is presented in LST.
        Default: Etc/GMT+6

- **tzone_out:** str
    
        Timezone code output data is converted to. 
        Default: Etc/GMT+6 

- **save:** bool

        Whether to save the data to a file or not. If true, the processed file is saved to the same path as the original .txt file with an updated file extension (.csv or .gzip)
        Default: False

- **saved_file_type:**
    
        Options: "parquet" or "csv"
        
        Save file as either csv or parquet (.gzip) file format. Parquet maintains datetime information precluding the need to do any datetime conversions after reading in the data to polars or pandas later.

- ****kwargs:** str
    
        Additional arguments passed to tzone conversion. 
        See polars_convert_data_and_time_columns_to_datetime().


Returns
---------
- **pl.Dataframe**

       polars dataframe in wide format containing TAMIS records with descriptive column names


### Example


```
Sample data from raw TAMIS report:

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
```
If data is in file at "filepath":

```
>>> df = ttp.read_tceq_to_pl_dataframe(fpath = filepath,
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

### Saving
```
>>> df = ttp.read_tceq_to_pl_dataframe(fpath = filepath,
                                        tzone_in = "Etc/GMT+6",
                                        tzone_out = "Etc/GMT+6",
                                        save = True,
                                        saved_file_type = "parquet")

Processed file saved to: /path/to/filepath.gzip
```



