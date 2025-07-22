import os
from crop_suitability_predictor.config.config import (
    PULL_PRECOMPILED_CROP_AND_YIELD_DATA,
)
from crop_suitability_predictor.modules.data.data_injestion_utils import (
    fetch_CA_shape_file,
    fetch_cdl_map,
    fetch_cdl_rasters,
    fetch_commodity_code_map,
    fetch_crop_report_data,
    fetch_historical_max_data,
    fetch_ndvi_tiles,
    fetch_soil_rasters,
)


def fetch_raw_data():
    fetch_cdl_rasters()
    fetch_CA_shape_file()
    fetch_ndvi_tiles()
    fetch_soil_rasters()
    # if not PULL_PRECOMPILED_CROP_AND_YIELD_DATA:
    #     fetch_cleaned_crop_and_yield_data()
    # else:
    fetch_crop_report_data()
    fetch_historical_max_data()
    fetch_cdl_map()
    fetch_commodity_code_map()


def main() -> None:
    fetch_raw_data()


if __name__ == "__main__":
    fetch_raw_data()
