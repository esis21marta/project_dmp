# Studio Schema Uploader
Uploads Schema From Excel Sheets To Studio Using A Simple CLI Command

## Pre-requisites
- python version 3.7 or above
- conda (Optional)

## Getting started

```bash
conda activate prj-thanatos-env
pip install -r requirements.txt
```

## Optional Arguments

```
-o OMIT_ROWS, --omit_rows  OMIT_ROWS    Number Of Rows To Omit
```

## Required Arguments

```
-f FILENAME,  --filename   FILENAME    Excel File Name, Needs Absolute Path
-s SHEETNAME, --sheetname  SHEETNAME   Excel Sheet Name
-c COLS,      --cols       COLS        List Of Columns To Be Used For (Column Name, Type, Description)

--data_set    DATA_SET     Data Set Name
--data_source DATA_SOURCE  Data Source Name
```

## Example
```bash
python upload.py -f "data/DataDictionary.xlsx" 
                 -s "Revenue" 
                 -o 3 
                 -c "Attribute Name, Data Type, Business Rule (If Any)" 
                 --data_set "xyz.revenue" 
                 --data_source "XYZ"
```

If more than 3 columns are specified, the extra columns (apart from the starting 2) are concatenated using '|' and uploaded into the `Description` of Studio.
