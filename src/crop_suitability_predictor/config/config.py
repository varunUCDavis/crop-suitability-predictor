import os
from dotenv import load_dotenv

# universal
PROJECT_PATH = "/Users/varunwadhwa/Desktop/crop-suitability-predictor"
RAW_DATA_PATH = os.path.join(PROJECT_PATH, "data/raw")
PROCESSED_DATA_PATH = os.path.join(PROJECT_PATH, "data/processed")
INTERMEDIATE_PROCESSED_DATA_PATH = os.path.join(
    PROCESSED_DATA_PATH, "intermediate_processed_files"
)
TOP_LEVEL_PACKAGE_PATH = os.path.join(PROJECT_PATH, "src/crop_suitability_predictor")
RUNTIME_CONFIG_PATH = os.path.join(TOP_LEVEL_PACKAGE_PATH, "config")
ENV_FILE_PATH = os.path.join(PROJECT_PATH, ".env")
load_dotenv(ENV_FILE_PATH)
N_JOBS = 4
FINAL_CRS = 32610

# DATA INJESTION

# cdl
CDL_RASTER_DOWNLOAD_URL = "https://github.com/varunUCDavis/crop-suitability-predictor/releases/download/v1.0.0/cdl_crop_location_data.zip"
CDL_RASTER_FOLDER_NAME = "cdl_rasters"
CDL_RASTER_FOLDER_PATH = os.path.join(RAW_DATA_PATH, CDL_RASTER_FOLDER_NAME)
CDL_RASTER_FILE_NAME = "clipped.TIF"
CDL_RASTER_PATH = os.path.join(CDL_RASTER_FOLDER_PATH, CDL_RASTER_FILE_NAME)
CDL_ZIP_FILE_NAME = "cdl_rasters.zip"
CDL_ZIP_FILE_PATH = os.path.join(CDL_RASTER_FOLDER_PATH, CDL_ZIP_FILE_NAME)
ORIGINAL_CDL_CRS = 3857
CDL_CRS = 32610

# ca county shapefile
CA_SHAPEFILE_FOLDER_NAME = "county_boundaries_shapefile"
CA_SHAPEFILE_FOLDER_PATH = os.path.join(RAW_DATA_PATH, CA_SHAPEFILE_FOLDER_NAME)
CA_SHAPEFILE_ZIP_FILE_NAME = "ca_counties.zip"
CA_SHAPEFILE_ZIP_PATH = os.path.join(
    CA_SHAPEFILE_FOLDER_PATH, CA_SHAPEFILE_ZIP_FILE_NAME
)
CA_SHAPEFILE_FILE_NAME = "CA_Counties.shp"
CA_SHAPEFILE_FILE_PATH = os.path.join(CA_SHAPEFILE_FOLDER_PATH, CA_SHAPEFILE_FILE_NAME)
CA_SHAPEFILE_URL = "https://data.ca.gov/dataset/e212e397-1277-4df3-8c22-40721b095f33/resource/b0007416-a325-4777-9295-368ea6b710e6/download/ca_counties.zip"
CA_SHAPEFILE_CRS = 32610
# ndvi
PLANETARY_COMPUTER_API_URL = "https://planetarycomputer.microsoft.com/api/stac/v1"
NDVI_TILES_FOLDER_NAME = "landsat_tiles"
NDVI_TILES_FOLDER_PATH = os.path.join(RAW_DATA_PATH, NDVI_TILES_FOLDER_NAME)
NDVI_TIME_RANGE = "2022-01-01/2022-12-31"
NDVI_CRS = 32612

# soil Rasters
SOIL_API_URL = "https://soilmap2-1.lawr.ucdavis.edu/800m_grids/rasters/"
SOIL_FILE_NAMES = [
    "caco3_kg_sq_m.tif",
    "cec.tif",
    "db.tif",
    "drainage_class_int.tif",
    "ec.tif",
    "om_kg_sq_m.tif",
    "ph.tif",
    "rf_025.tif",
    "water_storage.tif",
    "texture_05.tif",
    "texture_025.tif",
    "texture_2550.tif",
]
SOIL_FOLDER_NAME = "soil_data"
SOIL_FOLDER_PATH = os.path.join(RAW_DATA_PATH, SOIL_FOLDER_NAME)
SOIL_RASTER_PATHS = [
    os.path.join(SOIL_FOLDER_PATH, file_name) for file_name in SOIL_FILE_NAMES
]
SOIL_FILE_PATHS_DICT = {
    file_name: os.path.join(SOIL_FOLDER_PATH, file_name)
    for file_name in SOIL_FILE_NAMES
}
SOIL_FILE_URLS_PATHS = [
    (os.path.join(SOIL_FOLDER_PATH, file_name), os.path.join(SOIL_API_URL, file_name))
    for file_name in SOIL_FILE_NAMES
]
SOIL_CRS = 5070

# crop reports
PULL_PRECOMPILED_CROP_REPORT_DATA = True
CROP_REPORT_DOWNLOAD_URL = "https://github.com/varunUCDavis/crop-suitability-predictor/releases/download/v1.0.0/crop_report_data_2022_cleaned.csv"
CROP_REPORT_DATA_FOLDER_NAME = "crop_report_data_clean"
CROP_REPORTS_FOLDER_PATH = os.path.join(RAW_DATA_PATH, CROP_REPORT_DATA_FOLDER_NAME)
CROP_REPORT_DATA_FILE_NAME = "crop_report_data_2022_cleaned.csv"
CROP_REPORT_FILE_PATH = os.path.join(
    CROP_REPORTS_FOLDER_PATH, CROP_REPORT_DATA_FILE_NAME
)

# histroical max data
MAX_YIELD_DOWNLOAD_URL = ""
MAX_YIELD_FOLDER_NAME = "max_yield_data"
MAX_YIELD_FOLDER_PATH = os.path.join(RAW_DATA_PATH, MAX_YIELD_FOLDER_NAME)
MAX_YIELD_DATA_FILE_NAME = "max_yield_data.csv"
MAX_YIELD_DATA_PATH = os.path.join(MAX_YIELD_FOLDER_PATH, MAX_YIELD_DATA_FILE_NAME)

# map csvs
MAP_FOLDER_NAME = "code_map"
MAP_FOLDER_PATH = os.path.join(RAW_DATA_PATH, MAP_FOLDER_NAME)

# cdl crop label map
CDL_CROP_LABEL_MAP_DOWNLOAD_URL = "https://github.com/varunUCDavis/crop-suitability-predictor/releases/download/v1.0.0/cdl_crop_labels.csv"
CDL_CROP_LABEL_MAP_FILE_NAME = "cdl_crop_labels.csv"
CDL_CROP_LABEL_MAP_FILE_PATH = os.path.join(
    MAP_FOLDER_PATH, CDL_CROP_LABEL_MAP_FILE_NAME
)

# commodity code label map
COMMODITY_CODE_MAP_DOWNLOAD_URL = "https://github.com/varunUCDavis/crop-suitability-predictor/releases/download/v1.0.0/cdl_commodity_code_map.csv"
COMMODITY_CODE_MAP_FILE_NAME = "commodity_code_map.csv"
COMMODITY_CODE_MAP_FILE_PATH = os.path.join(
    MAP_FOLDER_PATH, COMMODITY_CODE_MAP_FILE_NAME
)

# DATA TRANSFORMATION

# crop yield data
PULL_PRECOMPILED_CROP_AND_YIELD_DATA = True
CROP_PROD_DATA_FOLDER_NAME = "crop_production_data"
CROP_PROD_DATA_FOLDER_PATH = os.path.join(
    INTERMEDIATE_PROCESSED_DATA_PATH, CROP_PROD_DATA_FOLDER_NAME
)
MERGED_CROP_AND_YIELD_DATA_FILE_NAME = "merged_crop_yield_data.csv"
MERGED_CROP_AND_YIELD_DATA_FILE_PATH = os.path.join(
    CROP_PROD_DATA_FOLDER_PATH, MERGED_CROP_AND_YIELD_DATA_FILE_NAME
)
PRECOMPILED_MERGED_CROP_AND_YIELD_DATA_DOWNLOAD_URL = "https://github.com/varunUCDavis/crop-suitability-predictor/releases/download/v1.0.0/merged_crop_and_yield_data.csv"

# crop cover points
FILTER_COUNTY_POINTS = True
FILTER_BY_CROP_REPORT_CDL_VALS = True
CDL_VECTOR_POINTS_FOLDER_NAME = "cdl_vector_points"
CDL_VECTOR_POINTS_FOLDER_PATH = os.path.join(
    INTERMEDIATE_PROCESSED_DATA_PATH, CDL_VECTOR_POINTS_FOLDER_NAME
)
CDL_RASTER_NPY_FILE_NAME = "cdl_raster.npy"
CDL_RASTER_NPY_PATH = os.path.join(
    CDL_VECTOR_POINTS_FOLDER_PATH, CDL_RASTER_NPY_FILE_NAME
)
CDL_VECTOR_POINTS_FULL_FILE_NAME = "cdl_vector_points_full.csv"
CDL_VECTOR_POINTS_FULL_FILE_PATH = os.path.join(
    CDL_VECTOR_POINTS_FOLDER_PATH, CDL_VECTOR_POINTS_FULL_FILE_NAME
)
DOWN_SAMPLE_POINTS = True
DOWN_SAMPLING_CHUNK_SIZE = 100_000
ORIGINAL_POINT_RESOLUTION = 30  # meters
DOWN_SAMPLE_GRID_SIZE = 250  # meters
ACRE_TO_METER_CONVERSION = 4046.86
DOWN_SAMPLED_POINTS_FILE_NAME = (
    f"cdl_crop_points_downsampled_{DOWN_SAMPLE_GRID_SIZE}m.csv"
)
DOWN_SAMPLED_POINTS_FILE_PATH = os.path.join(
    CDL_VECTOR_POINTS_FOLDER_PATH, DOWN_SAMPLED_POINTS_FILE_NAME
)
VECTOR_POINTS_NPY_FILE_NAME = "cdl_vector_points.npy"
VECTOR_POINTS_NPY_PATH = os.path.join(
    CDL_VECTOR_POINTS_FOLDER_PATH, VECTOR_POINTS_NPY_FILE_NAME
)

# ndvi
INTERMEDIATE_NDVI_FOLDER_NAME = "ndvi"
INTERMEDIATE_NDVI_FOLDER_PATH = os.path.join(
    INTERMEDIATE_PROCESSED_DATA_PATH, INTERMEDIATE_NDVI_FOLDER_NAME
)
INTERMEDIATE_NDVI_FILE_NAME = "ndvi_values.csv"
INTERMEDIATE_NDVI_FILE_PATH = os.path.join(
    INTERMEDIATE_NDVI_FOLDER_PATH, INTERMEDIATE_NDVI_FILE_NAME
)
MERGED_CROP_AND_NDVI_DATA_FILE_NAME = "merged_crop_and_ndvi_data.csv"
MERGED_CROP_AND_NDVI_DATA_FILE_PATH = os.path.join(
    INTERMEDIATE_NDVI_FOLDER_PATH, MERGED_CROP_AND_NDVI_DATA_FILE_NAME
)
POINT_ACRES_CONVERSION = (
    ORIGINAL_POINT_RESOLUTION * ORIGINAL_POINT_RESOLUTION / ACRE_TO_METER_CONVERSION
)

# suitability score
INTERMIEDIATE_SUITABILITY_FOLDER_NAME = "suitability"
SUITABILITY_FINAL_TRAINING_DATA_PATH = os.path.join(
    PROCESSED_DATA_PATH, "final_training_data"
)
SUITABILITY_FOLDER_PATH = os.path.join(
    INTERMEDIATE_PROCESSED_DATA_PATH, INTERMIEDIATE_SUITABILITY_FOLDER_NAME
)

SUITABILITY_SCORE_PER_POINT_FILE_NAME = "suitability_score_per_point.csv"
SUITABILITY_SCORE_PER_POINT_FILE_PATH = os.path.join(
    SUITABILITY_FOLDER_PATH, SUITABILITY_SCORE_PER_POINT_FILE_NAME
)
SUITABILITY_SCORE_ALL_POINTS_FILE_NAME = "suitability_score_all_points.csv"
SUITABILITY_SCORE_ALL_POINTS_FILE_PATH = os.path.join(
    SUITABILITY_FINAL_TRAINING_DATA_PATH, SUITABILITY_SCORE_ALL_POINTS_FILE_NAME
)


# soil

INTERMEDIATE_SOIL_FOLDER_NAME = "soil"
INTERMEDIATE_SOIL_FOLDER_PATH = os.path.join(
    INTERMEDIATE_PROCESSED_DATA_PATH, INTERMEDIATE_SOIL_FOLDER_NAME
)
SAMPLED_SOIL_FILE_NAME = "sampled_soil_data.csv"
SAMPLED_SOIL_FILE_PATH = os.path.join(
    INTERMEDIATE_SOIL_FOLDER_PATH, SAMPLED_SOIL_FILE_NAME
)

REDUCED_SOIL_FEATURES_FILE_NAME = "cleaned_soil_features.csv"
REDUCED_SOIL_FEATURES_FILE_PATH = os.path.join(
    INTERMEDIATE_SOIL_FOLDER_PATH, REDUCED_SOIL_FEATURES_FILE_NAME
)


NUMERICAL_FEATURES = [
    "ph",
    "db",
    "rf_025",
    "cec",
    "ec",
    "caco3_kg_sq_m",
    "water_storage",
    "om_kg_sq_m",
]


CATEGORICAL_FEATURES = [
    "drainage_class_int",
    "texture_05",
    "texture_025",
    "texture_2550",
]

DRAINAGE_CLASS_VALS = [1, 2, 3, 4, 5, 6, 7, 8]
TEXTURE_VALS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]


ALL_FEATURES = SOIL_FILE_NAMES = [
    "caco3_kg_sq_m",
    "cec",
    "db",
    "drainage_class_int",
    "ec",
    "om_kg_sq_m",
    "ph",
    "rf_025",
    "water_storage",
    "texture_05",
    "texture_025",
    "texture_2550",
]
