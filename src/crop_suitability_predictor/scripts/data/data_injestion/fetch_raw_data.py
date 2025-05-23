import os 
from crop_suitability_predictor.modules.data.data_injestion_utils import fetch_CA_shape_file, fetch_cdl_rasters, fetch_crop_reports, fetch_ndvi_tiles, fetch_soil_rasters, fetch_usda_historical_max






def fetch_raw_data():
    # breakpoint()
    fetch_cdl_rasters()
    fetch_CA_shape_file()
    fetch_ndvi_tiles()
    fetch_soil_rasters()  
    fetch_crop_reports()
    fetch_usda_historical_max()

def main() -> None:
    fetch_raw_data()

if __name__ == '__main__':
    fetch_raw_data()
#submit_cdl_clip_job, CDLRasterStatus, check_if_cdl_raster_exists, unzip_file, download_cdl_raster
