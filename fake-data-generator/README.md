# Fake Data Generator
Simplifies Generation Of Fake Data Based On The Schema Specified

## Pre-requisites
- python version 3.7 or above
- conda (Optional) 

## Getting started

```bash
conda activate prj-thanatos-env
pip install -r requirements.txt
```

## Datatypes Supported
```
date_yyyy_mm_dd : Date in YYYY-MM-DD format
varchar(n)      : n >=5, Random string between 5-n characters
bigint          : Random integer from 65536-99999999999999999
timestamp(6)    : Timestamp in YYYY-MM-DD HH:MM:SS format
int             : Random integer from 0-65535
decimal(d,f)    : Random decimal with "(d-f).f" format
binary          : Random number between [0,1] inclusive
date_yyyymm     : Random year-month string in format "yyyymm"
time_hh24_mm_ss : Random time in HH:MM:SS format
```

## Steps

1. Add the schema in `schema.ini` file
2. Run `python generate.py` in `src/`
3. mkdir `data` in root directory
4. Output is generated in `data` folder
