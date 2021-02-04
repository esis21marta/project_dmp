# Load packages
import geopandas as gp


# Define function
def Process_Admin_boundaries_HDX(input_shp: str, input_2_shp: str, out_csv: str):

    # Load data
    admin_boundaries_gdf = gp.read_file(input_shp)
    locations_gdf = gp.read_file(input_2_shp)

    # Prepare admin boundaries
    admin_boundaries_2_gdf = admin_boundaries_gdf[
        ["ADM4_EN", "ADM3_EN", "ADM2_EN", "ADM1_EN", "geometry"]
    ]
    admin_boundaries_2_gdf.rename(
        columns={
            "ADM4_EN": "Admin_4",
            "ADM3_EN": "Admin_3",
            "ADM2_EN": "Admin_2",
            "ADM1_EN": "Admin_1",
        },
        inplace=True,
    )

    # Make spatial match (to assign admin boundaries to locations)
    locations_2_gdf = gp.sjoin(
        locations_gdf,
        admin_boundaries_2_gdf,
        how="inner",
        op="intersects",
        lsuffix="left",
        rsuffix="right",
    )
    locations_3_df = locations_2_gdf.drop(columns=["geometry", "index_right"])

    # Save to CSV
    locations_3_df.to_csv(out_csv, index=False)
