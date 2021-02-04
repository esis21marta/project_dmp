# Packages
import geopandas as gp
import pandas as pd


# Define function
def Process_POIs_OSM(
    OSM_POIs_gdf: gp.geodataframe.GeoDataFrame,
) -> gp.geodataframe.GeoDataFrame:

    # Select columns
    OSM_POIs_gdf = OSM_POIs_gdf.loc[
        :, ["fclass", "geometry"]
    ]  # 'fclass' contains the class of POI

    # Extract lat-long
    OSM_POIs_gdf["Latitude"] = OSM_POIs_gdf.centroid.y
    OSM_POIs_gdf["Longitude"] = OSM_POIs_gdf.centroid.x

    # Rename columns
    OSM_POIs_gdf.rename(
        columns={"fclass": "Category"}, inplace=True
    )  # inplace True is for returning a new dataframe

    # Order columns order
    column_titles = ["Category", "Latitude", "Longitude", "geometry"]
    OSM_POIs_gdf = OSM_POIs_gdf.reindex(columns=column_titles)

    # Check categories
    cats_df = pd.DataFrame(list(OSM_POIs_gdf.Category.unique()))
    cats_df.rename(columns={"0": "Cat"}, inplace=True)

    return OSM_POIs_gdf
