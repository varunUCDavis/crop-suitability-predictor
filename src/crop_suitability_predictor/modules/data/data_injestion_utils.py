from enum import Enum, auto
import json
import os
from pathlib import Path
import time
from typing import Dict, List, Optional, Tuple
import zipfile
import requests
from crop_suitability_predictor.config.config import (
    CA_SHAPEFILE_FILE_PATH,
    CA_SHAPEFILE_FOLDER_PATH,
    CA_SHAPEFILE_URL,
    CA_SHAPEFILE_ZIP_FILE_NAME,
    CA_SHAPEFILE_ZIP_PATH,
    CDL_CROP_LABEL_MAP_DOWNLOAD_URL,
    CDL_CROP_LABEL_MAP_FILE_NAME,
    CDL_CROP_LABEL_MAP_FILE_PATH,
    CDL_RASTER_DOWNLOAD_URL,
    CDL_ZIP_FILE_NAME,
    CDL_ZIP_FILE_PATH,
    CDL_RASTER_FOLDER_PATH,
    CDL_RASTER_PATH,
    COMMODITY_CODE_MAP_DOWNLOAD_URL,
    COMMODITY_CODE_MAP_FILE_NAME,
    COMMODITY_CODE_MAP_FILE_PATH,
    CROP_REPORT_DOWNLOAD_URL,
    CROP_REPORT_FILE_PATH,
    MAP_FOLDER_PATH,
    MAX_YIELD_DATA_FILE_NAME,
    MAX_YIELD_DATA_PATH,
    MAX_YIELD_DOWNLOAD_URL,
    MAX_YIELD_FOLDER_PATH,
    MERGED_CROP_AND_YIELD_DATA_FILE_NAME,
    MERGED_CROP_AND_YIELD_DATA_FILE_PATH,
    NDVI_TILES_FOLDER_PATH,
    NDVI_TIME_RANGE,
    PLANETARY_COMPUTER_API_URL,
    PRECOMPILED_MERGED_CROP_AND_YIELD_DATA_DOWNLOAD_URL,
    PROCESSED_DATA_PATH,
    SOIL_FILE_URLS_PATHS,
    SOIL_FOLDER_PATH,
)
from pystac_client import Client
import planetary_computer
import geopandas as gpd
import pandas as pd
import aiohttp
import aiofiles
import asyncio

from crop_suitability_predictor.modules.data.common_data_utils import (
    DataStatus,
    check_existing_files,
    check_file_exists,
    check_file_exists_async,
    load_crop_reports,
)


async def download_async(session, url, file_name, output_dir_path) -> str:
    os.makedirs(output_dir_path, exist_ok=True)
    output_path = os.path.join(output_dir_path, file_name)
    try:
        print(f"Downloading file from {url}...")
        async with session.get(url) as response:
            response.raise_for_status()
            async with aiofiles.open(output_path, "wb") as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
        print(f"✅ Download complete. File saved as {file_name}")
        return output_path

    except aiohttp.ClientError as e:
        print(f"❌ Error downloading {url}: {e}")
        return ""


async def download_all(
    urls_and_file_paths: List,
    output_dir_path: str,
) -> list[str]:
    async with aiohttp.ClientSession() as session:

        tasks = [
            download_async(session, url_file[0], url_file[1], output_dir_path)
            for url_file in urls_and_file_paths
        ]
        return await asyncio.gather(*tasks)


def download_file(url: str, output_file_name: str, output_dir_path: str) -> str:
    """
    Downloads a file from the specified URL and saves it locally.

    Args:
        url (str): The URL of the file to download.
        output_file (str): The local filename to save the downloaded file.
        output_path (str): The directory to download the file to

    Returns:
        bool: True if download is successful, False otherwise.
    """

    async def _inner():
        async with aiohttp.ClientSession() as session:
            return await download_async(session, url, output_file_name, output_dir_path)

    return asyncio.run(_inner())  # wraps the async function for sync usage


def unzip_file(zip_path: str, output_path: Optional[str] = None) -> bool:
    output_path = os.path.dirname(zip_path) if not output_path else output_path
    # Extract the ZIP
    os.makedirs(output_path, exist_ok=True)
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(output_path)
        print(f"✅ Unzipped zipfile to: {output_path}")
        return True
    except zipfile.BadZipFile:
        print(f"❌ Error: The given file is not a valid ZIP file.")
        return False


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

    form_data = {
        key: json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        for key, value in raw.items()
    }
    return form_data


def fetch_cdl_rasters():
    cdl_raster_status = check_file_exists(file_path=CDL_RASTER_PATH)
    cdl_raster_zip_status = check_file_exists(file_path=CDL_ZIP_FILE_PATH)
    if not cdl_raster_status:
        if not cdl_raster_zip_status:
            # download zip file
            download_status = download_file(
                url=CDL_RASTER_DOWNLOAD_URL,
                output_file_name=CDL_ZIP_FILE_NAME,
                output_dir_path=CDL_RASTER_FOLDER_PATH,
            )
            if not download_status:
                exit()
        # extract zip
        unzip_status = unzip_file(zip_path=CDL_ZIP_FILE_PATH)
        if not unzip_status:
            exit()


# def check_if_cdl_raster_exists(cdl_zip_file_path: str, cdl_extracted_raster_path:str) -> str:
#     if check_file_exists(cdl_extracted_raster_path):
#         return DataStatus.EXTRACTED_FILE_EXISTS
#     elif check_file_exists(cdl_zip_file_path):
#         return DataStatus.ZIP_EXISTS
#     return DataStatus.NOTHING_EXISTS


async def fetch_ndvi_tiles_async():
    # 1. Load shapefile and define search
    ca = gpd.read_file(CA_SHAPEFILE_FILE_PATH).to_crs("EPSG:4326")
    ca_geom = ca.unary_union

    catalog = Client.open(PLANETARY_COMPUTER_API_URL)
    search = catalog.search(
        collections=["landsat-8-c2-l2"],
        intersects=ca_geom,
        datetime=NDVI_TIME_RANGE,
        query={"eo:cloud_cover": {"lt": 20}},
        max_items=5000,
    )

    items = list(search.get_all_items())
    print(f"🔍 Found {len(items)} scenes")

    os.makedirs(NDVI_TILES_FOLDER_PATH, exist_ok=True)

    # 2. Collect all (url, path) pairs
    download_targets = []
    for item in items:
        scene_id = item.id
        signed = planetary_computer.sign(item)

        b4_url = signed.assets["SR_B4"].href
        b5_url = signed.assets["SR_B5"].href

        b4_path = os.path.join(NDVI_TILES_FOLDER_PATH, f"{scene_id}_B4.TIF")
        b5_path = os.path.join(NDVI_TILES_FOLDER_PATH, f"{scene_id}_B5.TIF")

        download_targets.extend([(b4_url, b4_path), (b5_url, b5_path)])

    # 3. Check existence concurrently
    paths = [path for _, path in download_targets]
    existence_flags = await asyncio.gather(*[check_file_exists_async(p) for p in paths])

    # 4. Filter to files that need downloading
    filtered_targets = [
        (url, name)
        for (url, name), exists in zip(download_targets, existence_flags)
        if not exists
    ]

    await download_all(
        urls_and_file_paths=filtered_targets, output_dir_path=NDVI_TILES_FOLDER_PATH
    )


def fetch_ndvi_tiles():
    async def _wrapper():
        return await fetch_ndvi_tiles_async()

    return asyncio.run(_wrapper())


def fetch_CA_shape_file():
    # check if it is already downloaded
    shapefile_status = check_file_exists(file_path=CA_SHAPEFILE_FILE_PATH)
    shapefile_zip_status = check_file_exists(file_path=CA_SHAPEFILE_ZIP_PATH)
    if not shapefile_status:
        if not shapefile_zip_status:
            # download zip file
            download_status = download_file(
                url=CA_SHAPEFILE_URL,
                output_file_name=CA_SHAPEFILE_ZIP_FILE_NAME,
                output_dir_path=CA_SHAPEFILE_FOLDER_PATH,
            )
            if not download_status:
                exit()
        # extract zip
        unzip_status = unzip_file(zip_path=CA_SHAPEFILE_ZIP_PATH)
        if not unzip_status:
            exit()


def fetch_soil_rasters():
    async def _wrapper():
        return await fetch_soil_rasters_async()

    return asyncio.run(_wrapper())


async def fetch_soil_rasters_async():
    os.makedirs(SOIL_FOLDER_PATH, exist_ok=True)
    # 3. Check existence concurrently
    paths = [path for path, _ in SOIL_FILE_URLS_PATHS]
    existence_flags = await asyncio.gather(*[check_file_exists_async(p) for p in paths])

    # 4. Filter to files that need downloading
    filtered_targets = [
        (url, path)
        for (path, url), exists in zip(SOIL_FILE_URLS_PATHS, existence_flags)
        if not exists
    ]
    await download_all(
        urls_and_file_paths=filtered_targets, output_dir_path=SOIL_FOLDER_PATH
    )


def fetch_crop_report_data():

    if check_file_exists(file_path=CROP_REPORT_FILE_PATH):
        return
    download_file(url=CROP_REPORT_DOWNLOAD_URL)


def fetch_historical_max_data():
    if check_file_exists(MAX_YIELD_DATA_PATH):
        return
    download_file(
        url=MAX_YIELD_DOWNLOAD_URL,
        output_file_name=MAX_YIELD_DATA_FILE_NAME,
        output_dir_path=MAX_YIELD_FOLDER_PATH,
    )


def fetch_cdl_map():
    if not check_file_exists(file_path=CDL_CROP_LABEL_MAP_FILE_PATH):
        download_file(
            url=CDL_CROP_LABEL_MAP_DOWNLOAD_URL,
            output_file_name=CDL_CROP_LABEL_MAP_FILE_NAME,
            output_dir_path=MAP_FOLDER_PATH,
        )


def fetch_commodity_code_map():
    breakpoint()
    if not check_file_exists(file_path=COMMODITY_CODE_MAP_FILE_PATH):
        download_file(
            url=COMMODITY_CODE_MAP_DOWNLOAD_URL,
            output_file_name=COMMODITY_CODE_MAP_FILE_NAME,
            output_dir_path=MAP_FOLDER_PATH,
        )


def fetch_cleaned_crop_and_yield_data():
    if not check_file_exists(file_path=MERGED_CROP_AND_YIELD_DATA_FILE_PATH):
        download_file(
            url=PRECOMPILED_MERGED_CROP_AND_YIELD_DATA_DOWNLOAD_URL,
            output_file_name=MERGED_CROP_AND_YIELD_DATA_FILE_NAME,
            output_dir_path=PROCESSED_DATA_PATH,
        )
