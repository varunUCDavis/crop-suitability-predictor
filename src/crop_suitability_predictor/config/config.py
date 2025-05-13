
import os
# universal
PROJECT_PATH = "/Users/varunwadhwa/Desktop/crop-suitability-predictor"
RAW_DATA_PATH = os.path.join(PROJECT_PATH, "data/raw")
PROCESSED_DATA_PATH = os.path.join(PROJECT_PATH, "data/processed")
TOP_LEVEL_PACKAGE_PATH = os.path.join(PROJECT_PATH, "src/crop_suitability_predictor")
RUNTIME_CONFIG_PATH = os.path.join(TOP_LEVEL_PACKAGE_PATH, "config")
# data injestion
CDL_ZIP_FILE_NAME = "cdl_rasters.zip"
CDL_RASTER_PAYLOAD_NAME = "cdl_payload.json"
CDL_RASTER_FOLDER_NAME = "cdl_rasters"