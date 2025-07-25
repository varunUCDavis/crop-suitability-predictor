import os
from src.crop_suitability_predictor.config.config import (
    PROJECT_PATH,
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
    INTERMEDIATE_PROCESSED_DATA_PATH,
    CDL_RASTER_FOLDER_PATH,
    CA_SHAPEFILE_FOLDER_PATH,
    NDVI_TILES_FOLDER_PATH,
    SOIL_FOLDER_PATH,
    CROP_REPORTS_FOLDER_PATH,
    MAX_YIELD_FOLDER_PATH,
    MAP_FOLDER_PATH,
    CROP_PROD_DATA_FOLDER_PATH,
    CDL_VECTOR_POINTS_FOLDER_PATH,
    INTERMEDIATE_NDVI_FOLDER_PATH,
    SUITABILITY_FINAL_TRAINING_DATA_PATH,
    SUITABILITY_FOLDER_PATH,
    INTERMEDIATE_SOIL_FOLDER_PATH,
)


def make_dirs():
    dirs = [
        RAW_DATA_PATH,
        PROCESSED_DATA_PATH,
        INTERMEDIATE_PROCESSED_DATA_PATH,
        CDL_RASTER_FOLDER_PATH,
        CA_SHAPEFILE_FOLDER_PATH,
        NDVI_TILES_FOLDER_PATH,
        SOIL_FOLDER_PATH,
        CROP_REPORTS_FOLDER_PATH,
        MAX_YIELD_FOLDER_PATH,
        MAP_FOLDER_PATH,
        CROP_PROD_DATA_FOLDER_PATH,
        CDL_VECTOR_POINTS_FOLDER_PATH,
        INTERMEDIATE_NDVI_FOLDER_PATH,
        SUITABILITY_FINAL_TRAINING_DATA_PATH,
        SUITABILITY_FOLDER_PATH,
        INTERMEDIATE_SOIL_FOLDER_PATH,
    ]
    for d in dirs:
        if not os.path.exists(d):
            os.makedirs(d, exist_ok=True)
            print(f"Created: {d}")
        else:
            print(f"Already exists: {d}")


if __name__ == "__main__":
    make_dirs()
    print("Data directory structure is ready!")
