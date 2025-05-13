from enum import Enum, auto
import json
import os
from pathlib import Path
import time
import zipfile
import requests

class CDLRasterStatus(Enum):
    ZIP_EXISTS = auto()
    EXTRACTED_RASTER_EXISTS = auto()
    NOTHING_EXISTS = auto()
    
def check_file_exists(file_path: str) -> bool:
    """
    Checks if a file exists at the specified path.

    Args:
        file_path (str): The path of the file to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    file = Path(file_path)
    return file.is_file()

def download_file(url: str, output_file: str, output_path: str) -> bool:
    """
    Downloads a file from the specified URL and saves it locally.

    Args:
        url (str): The URL of the file to download.
        output_file (str): The local filename to save the downloaded file.

    Returns:
        bool: True if download is successful, False otherwise.
    """
    output_path = os.path.join(output_path, output_file)
    try:
        print(f"Downloading file from {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        o
        # Save the file in chunks to avoid memory issues
        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print(f"✅ Download complete. File saved as {output_file}")
        return True

    except requests.RequestException as e:
        print(f"❌ Error downloading the file: {e}")
        return False

def unzip_file(zip_path:str, output_path:str|None) -> bool:
    output_folder = os.path.dirname(zip_path) if not output_path else output_folder
    # Extract the ZIP
    os.makedirs(output_path, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(output_folder)
        print(f"✅ Unzipped zipfile to: {output_folder}")
        return True
    except zipfile.BadZipFile:
        print(f"❌ Error: The given file is not a valid ZIP file.")
        return False


def download_soil_raster(output_file: str = "caco3_kg_sq_m.tif") -> bool:
    """
    Downloads the CA soil raster file (CaCO3) from the UC Davis server.

    Args:
        output_file (str): The local filename to save the downloaded file.

    Returns:
        bool: True if download is successful, False otherwise.
    """
    url = "https://soilmap2-1.lawr.ucdavis.edu/800m_grids/rasters/caco3_kg_sq_m.tif"
    return download_file(url, output_file)


def load_and_serialize_payload(json_path):
    """
    Loads a JSON payload file and serializes it for HTTP POST submission.

    Args:
        json_path (str): Path to the JSON file.

    Returns:
        dict: Serialized form data ready for submission.
    """
    with open(json_path, "r") as f:
        raw = json.load(f)

    form_data = {key: json.dumps(value) if isinstance(value, (dict, list)) else str(value) for key, value in raw.items()}
    return form_data

def submit_cdl_clip_job(json_payload_path: str) -> str:
    """
    Submits a CDL clipping job and downloads the resulting clipped raster ZIP.

    Args:
        json_payload_path (str): Path to the JSON file with job parameters.
        output_folder (str): Directory to save and extract the clipped raster files.
    """
    url = "https://pdi.scinet.usda.gov/geoprocessing/rest/services/ExportCropImage/GPServer/Clip%20Zip%20Ship/submitJob"

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "*/*",
        "Origin": "https://croplandcros.scinet.usda.gov",
        "Referer": "https://croplandcros.scinet.usda.gov/",
        "User-Agent": "Mozilla/5.0"
    }

    data = load_and_serialize_payload(json_payload_path)
    submit_response = requests.post(url, headers=headers, data=data)
    submit_response.raise_for_status()
    job_id = submit_response.json().get("jobId")

    # Polling for job status
    status_url = f"{url}/jobs/{job_id}?f=json"
    while True:
        status_response = requests.get(status_url)
        job_state = status_response.json().get("jobStatus")
        
        if job_state == "esriJobSucceeded":
            break
        elif job_state in ["esriJobFailed", "esriJobCancelled"]:
            raise RuntimeError(f"❌ Job failed or was cancelled: {job_state}")
        else:
            print(f"⌛ Job status: {job_state} ... retrying in 5s")
            time.sleep(5)
    
    # Download the result ZIP
    result_url = f"{url}/jobs/{job_id}/results/Clipped_Imagery?returnFeatureCollection=false&f=json"
    return result_url

def download_cdl_raster(url, zip_path: str, extraction_path):
    zip_link = requests.get(url).json()['value']['url']
    downloaded_successfully = download_file(url = zip_link, output_file=zip_path)
    if downloaded_successfully:
        unzip_file(zip_path=zip_path, output_folder= extraction_path)
    else:
        print("Failed zipfile extraction")

def check_if_cdl_raster_exists(cdl_zip_file_path: str, cdl_extracted_raster_path:str) -> str:
    if check_file_exists(cdl_extracted_raster_path):
        return CDLRasterStatus.EXTRACTED_RASTER_EXISTS
    elif check_file_exists(cdl_zip_file_path):
        return CDLRasterStatus.ZIP_EXISTS
    return CDLRasterStatus.NOTHING_EXISTS

