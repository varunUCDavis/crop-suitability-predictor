import os 
from crop_suitability_predictor.modules.data.data_injestion_utils import CDLRasterStatus, check_if_cdl_raster_exists, download_cdl_raster, submit_cdl_clip_job, unzip_file
from crop_suitability_predictor.config.config import PROJECT_PATH, RAW_DATA_PATH, CDL_RASTER_FOLDER_NAME, CDL_ZIP_FILE_NAME, CDL_RASTER_PAYLOAD_NAME, RUNTIME_CONFIG_PATH


def fetch_raw_data():
    breakpoint()
    # check if cdl rasters exist
    cdl_zip_file_path = os.path.join(RAW_DATA_PATH, CDL_RASTER_FOLDER_NAME,CDL_ZIP_FILE_NAME)
    cdl_extracted_folder_path = os.path.join(RAW_DATA_PATH, CDL_RASTER_FOLDER_NAME)
    cdl_extracted_file_path = os.path.join(cdl_extracted_folder_path,"clipped", "clipped.TIF")
    cdl_raster_status = check_if_cdl_raster_exists(cdl_extracted_raster_path=cdl_extracted_file_path, cdl_zip_file_path=cdl_zip_file_path)
    cdl_raster_payload_path = os.path.join(RUNTIME_CONFIG_PATH, CDL_RASTER_PAYLOAD_NAME)
    if cdl_raster_status == CDLRasterStatus.NOTHING_EXISTS:
        download_url = submit_cdl_clip_job(json_payload_path=cdl_raster_payload_path)
        download_cdl_raster(url=download_url, zip_path=cdl_extracted_folder_path, extraction_path=cdl_extracted_file_path)
    elif cdl_raster_status == CDLRasterStatus.ZIP_EXISTS:
        # extract zip file
        unzip_file(zip_path=cdl_zip_file_path, output_folder=cdl_extracted_folder_path)
        
        
    # check if 

def main() -> None:
    fetch_raw_data()

if __name__ == '__main__':
    fetch_raw_data()
#submit_cdl_clip_job, CDLRasterStatus, check_if_cdl_raster_exists, unzip_file, download_cdl_raster
