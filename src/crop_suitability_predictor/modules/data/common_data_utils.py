import asyncio
from contextlib import contextmanager
from enum import Enum, auto
import os
from pathlib import Path
from typing import List, Optional, Tuple, Union
import joblib
import pandas as pd
import geopandas as gpd
from pyproj import CRS
from shapely import wkt

# from crop_suitability_predictor.config.config import (
#     CDL_CROP_LABEL_MAP_FILE_PATH,
#     CROP_DESC_AND_MAX_PROD_FILE_PATH,
# )


class DataStatus(Enum):
    SOME_MISSING = auto()
    ALL_EXIST = auto()
    NONE_EXIST = auto()


ConvertibleToCRS = Union[int, str, CRS, dict]


def load_crop_reports(crop_reports_dir: str) -> pd.DataFrame:
    """
    Loads and combines crop report CSV files into a single DataFrame with a 'county' column.

    Args:
        file_paths (List[str]): List of full paths to crop report CSV files.

    Returns:
        pd.DataFrame: Combined DataFrame with a 'county' column.
    """
    df_list = []
    for file_name in os.listdir(crop_reports_dir):
        if file_name.startswith("Crop_Data - ") and file_name.endswith(".csv"):
            county = file_name.replace("Crop_Data - ", "").replace(".csv", "")
            df = pd.read_csv(os.path.join(crop_reports_dir, file_name), index_col=None)
            df["county"] = county
            df_list.append(df)

    full_df = pd.concat(df_list, ignore_index=True)
    full_df.rename({"CDL Code": " crop_code"})
    return full_df


def load_county_boudnary_shape_file(shape_file_path: str, crs: ConvertibleToCRS):
    county_gdf = gpd.read_file(shape_file_path).to_crs(epsg=crs)
    return county_gdf[["NAME", "geometry"]]


def load_cdl_code_map(cdl_Crop_label_map_file_path):
    cdl_label_df = pd.read_csv(cdl_Crop_label_map_file_path, index_col="CDL Code")
    return cdl_label_df


def load_crop_descriptions(
    crop_desc_and_max_prod_file_path: str, usecols: Optional[str] = None
):
    if usecols:
        selected_df = pd.read_csv(crop_desc_and_max_prod_file_path, usecols=usecols)
    else:
        selected_df = pd.read_csv(crop_desc_and_max_prod_file_path)
    return selected_df


def load_points(crop_points_file_path: str, crs: ConvertibleToCRS) -> gpd.GeoDataFrame:
    full_df = pd.read_csv(crop_points_file_path)
    full_df["geometry"] = full_df["geometry"].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(full_df, geometry="geometry", crs=crs)
    # gdf = gpd.read_file(crop_points_file_path)
    # gdf.set_crs(crs=crs, inplace=True)
    gdf = gdf.rename(columns={"NAME": "county"})
    gdf["county"] = gdf["county"].str.strip().str.title()
    return gdf


async def check_file_exists_async(file_path: str) -> bool:
    return Path(file_path).is_file()  # Still I/O-bound, but very fast


def check_file_exists(file_path: str) -> bool:
    async def _wrapper():
        return await check_file_exists_async(file_path)

    return asyncio.run(_wrapper())


async def check_existing_files_async(paths: List[str]) -> Tuple[List[str], DataStatus]:

    # Run all file checks concurrently
    exists_list = await asyncio.gather(*[check_file_exists_async(p) for p in paths])

    existing_files = [path for path, exists in zip(paths, exists_list) if exists]

    if all(exists_list):
        status_enum = DataStatus.ALL_EXIST
    elif any(exists_list):
        status_enum = DataStatus.SOME_MISSING
    else:
        status_enum = DataStatus.NONE_EXIST

    return existing_files, status_enum


def check_existing_files(paths: List[str]) -> Tuple[List[str], DataStatus]:
    return asyncio.run(check_existing_files_async(paths=paths))


@contextmanager
def tqdm_joblib(tqdm_object):
    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_callback
        tqdm_object.close()
