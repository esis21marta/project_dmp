## !!! This is an auto-generated file - DO NOT MODIFY !!!


**How it works:**
1. We read real catalog: conf/dmp/aggregation
2. We read smoke catalog: conf/smoke_run/aggregation
3. We replace the filepath from "real" to the one defined in "smoke" (for parquet)
4. We replace the database and table name from "real" to the ones defined in "smoke" (for hive)
5. Repeat 1-4 for training.catalog
6. Save it into conf/smoke_run/smoke_test and that is automatically passed on to project context for smoke run :)