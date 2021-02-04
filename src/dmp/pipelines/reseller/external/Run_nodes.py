import logging
import os
import time

import geopandas as gp

# Load packages
import pandas as pd
import yaml

# Load nodes
from Node_Admin_boundaries_HDX import Process_Admin_boundaries_HDX
from Node_Cellphone_Signal_OpenCellID import Process_Cellphone_Signal_OpenCellID
from Node_Demographics_GAR import Process_Demographics_GAR
from Node_Demographics_WorldPop import (
    Merge_all_datasets,
    Process_births,
    Process_dependency_ratio,
    Process_pregnancies,
)
from Node_Elevation_WorldPop import Process_Elevation_WorldPop
from Node_GDP_and_HDI_Dryad import (
    Process_GDP_Dryad,
    Process_GDP_per_cap_Dryad,
    Process_HDI_Dryad,
)
from Node_POIs_OSM import Process_POIs_OSM
from Node_Rainfall_NASA import Process_Rainfall_NASA
from Node_Roads_OSM import Process_Roads_OSM
from Node_Security_ACLED import Process_Security_ACLED
from Node_Urbanicity_GHS import Process_Urbanicity_GHS
from preprocess_4g_bts_share import preprocess_4g_bts_excel
from preprocess_big_poi_download import (
    collate_directories,
    delete_intermediate_files_dirs,
    extract_shapefile_data,
    merge_domains_dfs,
    merge_shapefiles,
)
from preprocess_city_archetype import preprocess_city_archetypes_excel
from preprocess_fb_pop import process_fb_pop
from preprocess_fb_share import preprocess_fb_share_excel
from preprocess_opensignal import preprocess_opensignal_excel

# --- PIPELINE INITIALIZATION ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)
log.info("initializing PIPELINE")

# Load parameters.yml and read variables
with open("conf/reseller/external/parameters.yml", "r") as f:
    config = yaml.safe_load(f)

ext_data_dir = config["ext_data_dir"]
cwd = os.getcwd()


# --- NODE 0: ADMIN BOUNDARIES HDX ---
if config["nodes"]["Admin_boundaries_HDX"]:
    log.info("processing ADMIN BOUNDARIES HDX")

    # Addresses
    input_shp = ext_data_dir + config["Admin_boundaries_HDX"]["p_input_shp"]
    input_2_shp = ext_data_dir + config["Admin_boundaries_HDX"]["p_input_2_shp"]
    out_csv = (
        ext_data_dir
        + config["Admin_boundaries_HDX"]["p_out_csv"]
        + "mck.ext_HDX_administrative_divisions_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # create folder if does not exist
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    # Data processing function
    Process_Admin_boundaries_HDX(input_shp, input_2_shp, out_csv)


# --- NODE 1: CELLPHONE SIGNAL OPENCELLID ---
if config["nodes"]["Cellphone_Signal_OpenCellID"]:
    log.info("processing CELLPHONE SIGNAL OPENCELLID")

    # Addresses
    input_WP_shp = (
        ext_data_dir + config["Cellphone_Signal_OpenCellID"]["p_input_WP_shp"]
    )
    input_OCI_csv = (
        ext_data_dir + config["Cellphone_Signal_OpenCellID"]["p_input_OCI_csv"]
    )
    out_csv = (
        ext_data_dir
        + config["Cellphone_Signal_OpenCellID"]["p_out_csv"]
        + "mck.ext_OCiD_cell_towers_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # create folder if does not exist
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    # Load data
    WP_1km_grid_gdf = gp.read_file(input_WP_shp)
    cell_phone_signal_df = pd.read_csv(input_OCI_csv)

    # Data processing function
    WP_1km_grid_gdf = Process_Cellphone_Signal_OpenCellID(
        WP_1km_grid_gdf, cell_phone_signal_df
    )

    # Save to CSV
    WP_1km_grid_gdf.to_csv(out_csv, index=False)


# --- NODE 2: DEMOGRAPHICS GAR ---
if config["nodes"]["Demographics_GAR"]:
    log.info("processing DEMOGRAPHICS GAR")

    # Addresses
    input_shp = ext_data_dir + config["Demographics_GAR"]["p_input_shp"]
    out_csv = (
        ext_data_dir
        + config["Demographics_GAR"]["p_out_csv"]
        + "mck.ext_GAR_demographics_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # create folder if does not exist
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    # Load data
    GAR_df = gp.read_file(input_shp)

    # Data processing function
    GAR_2_df = Process_Demographics_GAR(GAR_df)

    # Save to CSV
    GAR_2_df.to_csv(out_csv, index=False)


# --- NODE 3: DEMOGRAPHICS WORLDPOP ---
if config["nodes"]["Demographics_WorldPop"]:
    log.info("processing DEMOGRAPHICS WORLDPOP")

    # Define addresses
    dependency_ratio_raster = (
        ext_data_dir + config["Demographics_WorldPop"]["p_dependency_ratio_raster"]
    )
    de_temp = ext_data_dir + config["Demographics_WorldPop"]["p_de_temp"]
    dependency_ratio_csv = (
        ext_data_dir
        + config["Demographics_WorldPop"]["p_dependency_ratio_csv"]
        + "Dem_WP_DR_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    births_raster = ext_data_dir + config["Demographics_WorldPop"]["p_births_raster"]
    bi_temp = ext_data_dir + config["Demographics_WorldPop"]["p_bi_temp"]
    births_csv = (
        ext_data_dir
        + config["Demographics_WorldPop"]["p_births_csv"]
        + "Dem_WP_Bi_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    pregnancies_raster = (
        ext_data_dir + config["Demographics_WorldPop"]["p_pregnancies_raster"]
    )
    pr_temp = ext_data_dir + config["Demographics_WorldPop"]["p_pr_temp"]
    pregnancies_csv = (
        ext_data_dir
        + config["Demographics_WorldPop"]["p_pregnancies_csv"]
        + "Dem_WP_Pr_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )
    merged_data_csv = (
        ext_data_dir
        + config["Demographics_WorldPop"]["p_merged_data_csv"]
        + "mck.ext_WP_demographics_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # Create folder if does not exist
    os.makedirs(os.path.dirname(de_temp), exist_ok=True)
    os.makedirs(os.path.dirname(merged_data_csv), exist_ok=True)

    # Data processing functions
    Process_dependency_ratio(dependency_ratio_raster, de_temp, dependency_ratio_csv)
    Process_births(births_raster, bi_temp, births_csv)
    Process_pregnancies(pregnancies_raster, pr_temp, pregnancies_csv)
    Merge_all_datasets(
        dependency_ratio_csv, births_csv, pregnancies_csv, merged_data_csv
    )


# --- NODE 4: ELEVATION WORLDPOP ---
if config["nodes"]["Elevation_WorldPop"]:
    log.info("processing ELEVATION WORLDPOP")

    # Define addresses
    input_raster = ext_data_dir + config["Elevation_WorldPop"]["p_input_raster"]
    temp_csv = ext_data_dir + config["Elevation_WorldPop"]["p_temp_csv"]
    out_csv = (
        ext_data_dir
        + config["Elevation_WorldPop"]["p_out_csv"]
        + "mck.ext_WP_elevation_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # Create folder if does not exist
    os.makedirs(os.path.dirname(temp_csv), exist_ok=True)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    # Data processing function
    Process_Elevation_WorldPop(input_raster, temp_csv, out_csv)


# --- NODE 5: GDP and HDI Dryad ---
if config["nodes"]["GDP_and_HDI_Dryad"]:
    log.info("processing GDP AND HDI DRYAD")

    # Addresses
    input_1_nc = ext_data_dir + config["GDP_and_HDI_Dryad"]["p_input_1_nc"]
    input_2_nc = ext_data_dir + config["GDP_and_HDI_Dryad"]["p_input_2_nc"]
    input_3_nc = ext_data_dir + config["GDP_and_HDI_Dryad"]["p_input_3_nc"]
    out_1_csv = (
        ext_data_dir
        + config["GDP_and_HDI_Dryad"]["p_out_1_csv"]
        + "mck.ext_Dryad_HDI_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )
    out_2_csv = (
        ext_data_dir
        + config["GDP_and_HDI_Dryad"]["p_out_2_csv"]
        + "mck.ext_Dryad_GDP_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )
    out_3_csv = (
        ext_data_dir
        + config["GDP_and_HDI_Dryad"]["p_out_3_csv"]
        + "mck.ext_Dryad_GDP_per_capita_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # Create folder if does not exist
    os.makedirs(os.path.dirname(out_1_csv), exist_ok=True)

    # Data processing functions
    Process_HDI_Dryad(input_1_nc, out_1_csv)
    Process_GDP_Dryad(input_2_nc, out_2_csv)
    Process_GDP_per_cap_Dryad(input_3_nc, out_3_csv)


# --- NODE 6: POIs OSM ---
if config["nodes"]["POIs_OSM"]:
    log.info("processing POIs OSM")

    # Addresses
    input_shp = ext_data_dir + config["POIs_OSM"]["p_input_shp"]
    out_csv = (
        ext_data_dir
        + config["POIs_OSM"]["p_out_csv"]
        + "mck.ext_OSM_POIs_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # Create folder if does not exist
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    # Load data
    OSM_POIs_gdf = gp.read_file(input_shp)

    # Data processing function
    OSM_POIs_gdf = Process_POIs_OSM(OSM_POIs_gdf)

    # Save to CSV
    OSM_POIs_gdf.to_csv(out_csv, index=False)


# --- NODE 7: RAINFALL NASA ---
if config["nodes"]["Rainfall_NASA"]:
    log.info("processing RAINFALL NASA")

    # Adresses
    input_template = ext_data_dir + config["Rainfall_NASA"]["p_input_template"]
    data_dir = ext_data_dir + config["Rainfall_NASA"]["p_data_dir"]
    input_raster = ext_data_dir + config["Rainfall_NASA"]["p_input_raster"]
    temp_csv = ext_data_dir + config["Rainfall_NASA"]["p_temp_csv"]
    out_csv = (
        ext_data_dir
        + config["Rainfall_NASA"]["p_out_csv"]
        + "mck.ext_NEO_rainfall_index_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # Create folder if does not exist
    os.makedirs(os.path.dirname(temp_csv), exist_ok=True)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    # Data processing function
    Process_Rainfall_NASA(input_template, data_dir, input_raster, temp_csv, out_csv)


# --- NODE 8: ROADS OSM ---
if config["nodes"]["Roads_OSM"]:
    log.info("processing ROADS OSM")

    # Addresses
    input_shp = ext_data_dir + config["Roads_OSM"]["p_input_shp"]
    out_csv = (
        ext_data_dir
        + config["Roads_OSM"]["p_out_csv"]
        + "mck.ext_OSM_primary_and_secondary_roads_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # Create folder if does not exist
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    # Load data
    OSM_roads_lines_df = gp.read_file(input_shp)

    # Data processing function
    road_points_df = Process_Roads_OSM(OSM_roads_lines_df)

    # Save to CSV
    road_points_df.to_csv(out_csv, index=False)


# --- NODE 9: SECURITY ACLED ---
if config["nodes"]["Security_ACLED"]:
    log.info("processing SECURITY ACLED")

    # Addresses
    input_xlsx = ext_data_dir + config["Security_ACLED"]["p_input_xlsx"]
    out_csv = (
        ext_data_dir
        + config["Security_ACLED"]["p_out_csv"]
        + "mck.ext_ACLED_conflict_events_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # Create folder if does not exist
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    # Load data
    Security_ACLED_df = pd.read_excel(input_xlsx)

    # Data processing function
    Security_ACLED_2_df = Process_Security_ACLED(Security_ACLED_df)

    # Save to CSV
    Security_ACLED_2_df.to_csv(out_csv, index=False)


# --- NODE 10: URBANICITY GHS ---
if config["nodes"]["Urbanicity_GHS"]:
    log.info("processing URBANICITY GHS")

    # Define addresses
    input_tif = ext_data_dir + config["Urbanicity_GHS"]["p_input_tif"]
    temp_csv = ext_data_dir + config["Urbanicity_GHS"]["p_temp_csv"]
    out_csv = (
        ext_data_dir
        + config["Urbanicity_GHS"]["p_out_csv"]
        + "mck.ext_GHS_urbanicity_"
        + time.strftime("%Y%m%d_%H%M")
        + ".csv"
    )

    # Create folder if does not exist
    os.makedirs(os.path.dirname(temp_csv), exist_ok=True)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    # Data processing function
    Process_Urbanicity_GHS(input_tif, temp_csv, out_csv)


# --- NODE 11: 4G BTS SHARE ---
if config["nodes"]["4g_bts_share"]:
    log.info("processing 4G BTS SHARE")

    # declare directory path where input excel file is stored
    input_dir_path = ext_data_dir + config["4g_bts_share"]["input_dir_path"]
    os.chdir(input_dir_path)

    # declare input excel filename
    input_excel_filename = config["4g_bts_share"]["input_excel_filename"]

    # declare output csv filepath
    output_csv_filename = config["4g_bts_share"]["output_csv_filename"]

    # create folder if does not exist
    os.makedirs(os.path.dirname(output_csv_filename), exist_ok=True)

    preprocess_4g_bts_excel(input_excel_filename, output_csv_filename)

    # revert to original wd
    os.chdir(cwd)


# --- NODE 12: BIG POI ---
if config["nodes"]["big_poi"]:
    log.info("processing BIG POI")

    # path to directory where BIG POI downloads reside
    big_poi_dir_path = ext_data_dir + config["big_poi"]["big_poi_dir_path"]
    os.chdir(big_poi_dir_path)

    # parameters
    kwargs = config["big_poi"]["kwargs"]

    # create folder if does not exist
    os.makedirs(os.path.dirname(kwargs["output_csv_filename"]), exist_ok=True)

    collate_directories(**kwargs)
    merge_shapefiles(**kwargs)
    merge_domains_dfs(*extract_shapefile_data(**kwargs))
    delete_intermediate_files_dirs(**kwargs)

    # revert to original wd
    os.chdir(cwd)


# --- NODE 13: FACEBOOK POPULATION ---
if config["nodes"]["fb_pop"]:
    log.info("processing FACEBOOK POPULATION")

    # declare directory path where input excel file is stored
    input_dir_path = ext_data_dir + config["fb_pop"]["input_dir_path"]
    os.chdir(input_dir_path)

    # declare input (filenames: population column's renamed name) within python dictionary
    input_dict = config["fb_pop"]["filenames"]

    # declare output csv filepath
    output_csv_filename = config["fb_pop"]["output_csv_filename"]

    # create folder if does not exist
    os.makedirs(os.path.dirname(output_csv_filename), exist_ok=True)

    process_fb_pop(input_dict, output_csv_filename)

    # revert to original wd
    os.chdir(cwd)


# --- NODE 14: FACEBOOK SHARE ---
if config["nodes"]["fb_share"]:
    log.info("processing FACEBOOK SHARE")

    # declare directory path where input excel file is stored
    input_dir_path = ext_data_dir + config["fb_share"]["input_dir_path"]
    os.chdir(input_dir_path)

    # declare input excel filename
    input_excel_filename = config["fb_share"]["input_excel_filename"]

    # declare output csv filepath
    output_csv_filename = config["fb_share"]["output_csv_filename"]

    # create folder if does not exist
    os.makedirs(os.path.dirname(output_csv_filename), exist_ok=True)

    preprocess_fb_share_excel(input_excel_filename, output_csv_filename)

    # revert to original wd
    os.chdir(cwd)


# --- NODE 15: OPENSIGNAL ---
if config["nodes"]["opensignal"]:
    log.info("processing OPENSIGNAL")

    # declare directory path where input excel file is stored
    input_dir_path = ext_data_dir + config["opensignal"]["input_dir_path"]
    os.chdir(input_dir_path)

    # declare input excel filename
    input_excel_filename = config["opensignal"]["input_excel_filename"]

    # declare output csv filepath
    output_csv_filename = config["opensignal"]["output_csv_filename"]

    # create folder if does not exist
    os.makedirs(os.path.dirname(output_csv_filename), exist_ok=True)

    preprocess_opensignal_excel(input_excel_filename, output_csv_filename)

    # revert to original wd
    os.chdir(cwd)


# --- NODE 16: CITY ARCHETYPES ---
if config["nodes"]["city_archetype"]:
    log.info("processing CITY ARCHETYPES")

    # declare directory path where input excel file is stored
    input_dir_path = ext_data_dir + config["city_archetype"]["input_dir_path"]
    os.chdir(input_dir_path)

    # declare input excel filename
    input_excel_filename = config["city_archetype"]["input_excel_filename"]

    # declare output csv filepath
    output_csv_filename = config["city_archetype"]["output_csv_filename"]

    # create folder if does not exist
    os.makedirs(os.path.dirname(output_csv_filename), exist_ok=True)

    preprocess_city_archetypes_excel(input_excel_filename, output_csv_filename)

    # revert to original wd
    os.chdir(cwd)
