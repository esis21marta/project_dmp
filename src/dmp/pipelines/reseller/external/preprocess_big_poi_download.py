import logging
import os
import time
from shutil import copyfile, move, rmtree
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd

from googletrans import Translator

log = logging.getLogger(__name__)


def collate_directories(**kwargs):
    for idx, folder_prefix in enumerate(kwargs["folder_prefixes"]):
        log.info(f"Unzipping and collating {folder_prefix}: START")
        indo_cat = folder_prefix[:8]
        collated_dir = f"shp files {folder_prefix[-3:]}"
        for cat in kwargs["categories"]:
            if indo_cat in cat:
                category = cat

        # derived directories
        category_dir_fp = os.path.join(os.getcwd(), category)
        collated_dir_fp = os.path.join(category_dir_fp, collated_dir)

        # create collated directory if does not exist
        if not os.path.exists(collated_dir_fp):
            os.mkdir(collated_dir_fp)

        zip_files = [
            os.path.join(category_dir_fp, fn)
            for fn in os.listdir(category_dir_fp)
            if ".zip" in fn
        ]

        for zip_fp in zip_files:
            zip_prefix = os.path.basename(zip_fp).split(" ")[0]
            part_num = os.path.basename(zip_fp).split("(")[1].split(")")[0]

            archive = ZipFile(zip_fp)
            extracted_dir_fp = os.path.join(
                category_dir_fp, f"{zip_prefix} part {part_num}"
            )

            for file in archive.namelist():
                if file.startswith("zipfolder/"):
                    archive.extract(file, extracted_dir_fp)

            for p, d, f in os.walk(extracted_dir_fp, topdown=False):
                for n in f:
                    os.replace(os.path.join(p, n), os.path.join(extracted_dir_fp, n))
                for n in d:
                    os.rmdir(os.path.join(p, n))

        # subdirectories in category directory
        category_subdirs = next(os.walk(category_dir_fp))[1]

        for subdir in category_subdirs:
            if folder_prefix in subdir:
                part_num = subdir.split(" ")[-1]
                sub_dir_fp = os.path.join(category_dir_fp, subdir)
                filenames = os.listdir(sub_dir_fp)
                for filename in filenames:
                    if (
                        os.path.splitext(filename)[-1]
                        in kwargs["extensions_to_extract"]
                    ):
                        copyfile(
                            os.path.join(sub_dir_fp, filename),
                            os.path.join(
                                collated_dir_fp,
                                f"{os.path.splitext(filename)[0]} part {part_num}{os.path.splitext(filename)[1]}",
                            ),
                        )

        log.info(f"Unzipping and collating {folder_prefix}: END")


def merge_shapefiles(**kwargs):
    for category in kwargs["categories"]:
        for shp_folder in kwargs["shp_folders"]:
            shp_folder = os.path.join(category, shp_folder)
            log.info(f"Merging shapefiles in {shp_folder}: START")
            shp_filenames = [
                fn
                for fn in os.listdir(shp_folder)
                if ".shp" == os.path.splitext(fn)[-1]
            ]
            parts = len(
                set(
                    [
                        os.path.splitext(os.path.basename(fp))[0].split(" ")[-1]
                        for fp in shp_filenames
                    ]
                )
            )
            log.info(f"{shp_folder} parts: {parts}")

            for shp_fn in shp_filenames:
                shp_fn = shp_fn.split(" ")[0]
                log.info(f"Merging {shp_fn}")
                shp_fp = os.path.join(shp_folder, shp_fn)
                shp_fp = shp_fp.replace(" ", "\ ").replace("(", "\(").replace(")", "\)")
                merged_shp_fp = shp_fp + "\ merged.shp"
                part1_shp_fp = shp_fp + "\ part\ 1.shp"
                os.system(
                    f"ogr2ogr -f 'ESRI Shapefile' {merged_shp_fp} {part1_shp_fp} -a_srs EPSG:4326"
                )
                for part_num in range(2, parts + 1):
                    part_shp_fp = shp_fp + "\ part\ " + str(part_num) + ".shp"
                    os.system(
                        f"ogr2ogr -f 'ESRI Shapefile' -update -append {merged_shp_fp} {part_shp_fp} -a_srs EPSG:4326"
                    )

            log.info(f"Merging shapefiles in {shp_folder}: END\n")

            log.info(f"Shifting shapefiles from {shp_folder} to merged folder: START")
            fn_keyword = "merged"
            merged_dir_fp = os.path.join(category, fn_keyword)

            if not os.path.exists(merged_dir_fp):
                os.mkdir(merged_dir_fp)

            # get absolute filepaths of merged files to shift
            files_to_shift = [
                os.path.join(shp_folder, fn)
                for fn in os.listdir(shp_folder)
                if fn_keyword in fn
            ]

            # shift merged files from shp_folder to merged dir
            for fp in files_to_shift:
                move(fp, os.path.join(merged_dir_fp, os.path.basename(fp)))

            log.info(f"Shifting merged files from {shp_folder} to merged folder: END\n")


def extract_shapefile_data(**kwargs):
    translator = Translator() if kwargs["googletrans"] else None
    translation_dict = kwargs["translation_dict"]

    # translate dictionary wrapper
    def translate_dict(string_to_translate: str) -> str:
        output_str = subcategory
        if subcategory in translation_dict:
            output_str = translation_dict[subcategory]
        elif kwargs["googletrans"]:
            output_str = translator.translate(
                string_to_translate.lower(), src="id", dest="en"
            ).text.title()
            translation_dict[string_to_translate] = output_str
        return output_str

    # function to take in a shapefile and convert into pandas dataframe
    def shp_extract(shp_fp: str, category: str, subcategory: str) -> pd.DataFrame:
        shp_data = gpd.read_file(shp_fp).geometry.centroid
        shp_type = os.path.basename(shp_fp).split(" ")[0].split("_")[-2]
        output_df = pd.DataFrame(
            {
                "category": category,
                "subcategory": translate_dict(subcategory),
                "shp_type": shp_type,
                "longitude": shp_data.apply(lambda geo: geo.x),
                "latitude": shp_data.apply(lambda geo: geo.y),
            }
        )
        return output_df

    output_dfs = []

    for category in kwargs["categories"]:
        log.info(f"Extracting POI data from {category}: START")
        merged_dir = "merged"
        output_csv_fn = f"extracted_poi_data {category}.csv"
        map_type = "_"

        merged_dir_fp = os.path.join(os.getcwd(), category, merged_dir)
        os.path.join(os.getcwd(), category, output_csv_fn)

        category = category.split(" (")[0]

        # identify all subcategories by getting all shp filenames first
        shp_fns = [
            fn for fn in os.listdir(merged_dir_fp) if ".shp" in fn and map_type in fn
        ]

        # convert shp filenames to filepaths
        shp_fps = [os.path.join(merged_dir_fp, fn) for fn in shp_fns]

        for idx, shp_fp in enumerate(shp_fps):
            subcategory = os.path.basename(shp_fp).split("_")[1]
            if idx == 0:
                output_df = shp_extract(shp_fp, category, subcategory)
            else:
                output_df = pd.concat(
                    [output_df, shp_extract(shp_fp, category, subcategory)],
                    axis=0,
                    ignore_index=True,
                )

        output_dfs.append(output_df)

        log.info(f"Extracting POI data from {category}: END\n")

    return output_dfs, kwargs["output_csv_filename"]


def merge_domains_dfs(dfs, output_csv_filename):
    for idx, df in enumerate(dfs):
        if idx == 0:
            output_df = df
        else:
            output_df = pd.concat([output_df, df], ignore_index=True)

    timestamp = time.strftime("%Y-%m-%d_%H-%M-%S")
    output_df.drop_duplicates().to_csv(
        f"{output_csv_filename}_{timestamp}.csv", index=False
    )
    log.info(f"Saving BIG POI CSV to {output_csv_filename}_{timestamp}.csv")


def delete_intermediate_files_dirs(**kwargs):
    for category in kwargs["categories"]:
        log.info(f"Removing intermediate files from {category}: START")
        fps = [
            os.path.join(os.getcwd(), category, e.name)
            for e in os.scandir(category)
            if os.path.splitext(e.name)[-1] != ".zip"
        ]

        for fp in fps:
            if os.path.isdir(fp):
                rmtree(fp)
                log.info(f"Removing directory: {fp}")
            else:
                os.remove(fp)
                log.info(f"Removing file: {fp}")

        log.info(f"Removing intermediate files from {category}: END\n")
