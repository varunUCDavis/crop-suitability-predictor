import os
import rasterio
import pandas as pd
import numpy as np
from shapely.geometry import Point
import geopandas as gpd
from tqdm import tqdm
from joblib import Parallel, delayed
from progress_utils import tqdm_joblib

#TEST TO SEE IF FILTERING WORKS, OLD FILTERING CODE IN "filter_points_by_cropcode.py"

# Set this to True to enable filtering (can later use os.getenv)
FILTER_POINTS = True  # or bool(os.getenv("FILTER_POINTS", "False"))

def convert_raster_to_csv(raster_path: str):
    counties = gpd.read_file("../supporting_files/CA_Counties.shp").to_crs(epsg=32610)

    with rasterio.open(raster_path) as src:
        band1 = src.read(1)
        transform = src.transform
        rows, cols = np.where(band1 != src.nodata)
        lon, lat = rasterio.transform.xy(transform, rows, cols)
        geometry = [Point(lon, lat) for lon, lat in zip(lon, lat)]
        values = band1[rows, cols]

        df = pd.DataFrame({"crop_code": values})
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:32610")

        def process_chunk(chunk):
            return gpd.sjoin(chunk, counties, how="left", predicate="within")

        chunks = np.array_split(gdf, 100)
        with tqdm_joblib(tqdm(desc="Processing", total=100)):
            results = Parallel(n_jobs=6)(delayed(process_chunk)(chunk) for chunk in chunks)

        result = pd.concat(results)
        result = result[['geometry', 'crop_code', 'NAME']].rename(columns={'NAME': 'county_name'})
        result = result.reset_index(drop=True).to_crs(epsg=4326)
        result["id"] = result.index + 1
        result = result[["id", "crop_code", "county_name", "geometry"]]
        result["geometry"] = result["geometry"].apply(lambda geom: geom.wkt)

        # Save full unfiltered results first
        result.to_csv("crop_locations_full.csv", index=False)
        print("✅ Saved full dataset to 'crop_locations_full.csv'")

        if FILTER_POINTS:
            print("🔍 Filtering based on cleaned crop data...")
            cleaned_df = pd.read_csv("cleaned_crop_data_converted.csv", usecols=["CDL Code", "county"])
            cleaned_df = cleaned_df.rename(columns={"CDL Code": "crop_code", "county": "county_name"})
            filtered_df = result.merge(cleaned_df.drop_duplicates(), on=["crop_code", "county_name"], how="inner")
            filtered_df.to_csv("../supporting_files/intermediate_files/filtered_points.csv", index=False)
            print("✅ Filtered points saved to 'filtered_points.csv'")

convert_raster_to_csv(raster_path="../supporting_files/cropland_data.TIF")



# import rasterio
# import pandas as pd
# import numpy as np
# from shapely.geometry import Point
# import geopandas as gpd
# from tqdm import tqdm
# from joblib import Parallel, delayed
# from progress_utils import tqdm_joblib
# from sqlalchemy import create_engine
# from sqlalchemy import text, Integer
# from geoalchemy2 import Geometry

# def convert_raster_to_csv(raster_path: str):
#     counties = gpd.read_file("/Users/varunwadhwa/Downloads/ca_counties 2/CA_Counties.shp").to_crs(epsg=32610)
#     engine = create_engine("postgresql+psycopg2://varunwadhwa@localhost:5432/geo_db")
#     # Path to the input raster file
#     raster_path = "/Users/varunwadhwa/Downloads/clipped (3)/clipped.TIF"
#     # output_csv = "output.csv"

#     # Open the raster file
#     with rasterio.open(raster_path) as src:
#         band1 = src.read(1)  # Read the first band (assuming single-band raster)
#         transform = src.transform  # Get the affine transform

#         # Get indices of non-null pixels
#         rows, cols = np.where(band1 != src.nodata)
        
#         # Convert pixel indices to geographic coordinates
#         lon, lat = rasterio.transform.xy(transform, rows, cols)
#         geometry = [Point(lon, lat) for lon, lat in zip(lon, lat)]
#         # Get pixel values
#         values = band1[rows, cols]

#         # Create DataFrame
#         df = pd.DataFrame({"crop_code": values})
#         gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:32610")


#         # Function to perform spatial join on a chunk
#         def process_chunk(chunk):
#             return gpd.sjoin(chunk, counties, how="left", predicate="within")

#         # Split into chunks for parallel processing
#         num_chunks = 100
#         chunks = np.array_split(gdf, num_chunks)

#         with tqdm_joblib(tqdm(desc="Processing", total=num_chunks)) as progress_bar:
#             results = Parallel(n_jobs=6)(
#                 delayed(process_chunk)(chunk) for chunk in chunks
#             )

#         # Run spatial join in parallel
#         # results = Parallel(n_jobs=num_chunks)(delayed(process_chunk)(chunk) for chunk in chunks)

#         # Combine results
#         result = pd.concat(results)
      
#         results = result[['geometry', 'crop_code', 'NAME']]
#         results = results.rename(columns={'NAME': 'county_name'})
#         results = results.reset_index(drop=True).to_crs(epsg=4326)
#         results["id"] = results.index + 1  # Start from 1

#         # Reorder columns so 'id' comes first (optional but nice)
#         results = results[["id", "crop_code", "county_name", "geometry"]]
#         breakpoint()
#         results.to_postgis(
#             name="crop_locations",
#             con=engine,
#             if_exists="replace",  # or "fail", or "append"
#             index=False,
#             dtype={
#                 "id": Integer,
#                 "geometry": Geometry("POINT", srid=4326)
#             }
#         )

#         with engine.connect() as conn:
#             conn.execute(text("""
#                 CREATE INDEX IF NOT EXISTS crop_locations_geom_idx
#                 ON crop_locations
#                 USING GIST (geometry);
#             """))

        


# convert_raster_to_csv(raster_path="/Users/varunwadhwa/Downloads/clipped (3)/clipped.TIF")