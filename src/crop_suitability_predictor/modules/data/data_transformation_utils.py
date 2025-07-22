from concurrent.futures import ThreadPoolExecutor
from csv import writer
import csv
import gc
import glob
from itertools import chain
import json
import multiprocessing
from multiprocessing import Pool, shared_memory
import os
import re
import tempfile
import time
from typing import List, Optional, Tuple
import gower_multiprocessing as gower


from affine import Affine
import dask
from pyproj import Transformer, transform
from scipy.stats import mode
from shapely.geometry import box
from joblib import Parallel, delayed
import numpy as np
from numpy.typing import NDArray
import pandas as pd
import geopandas as gpd
import rasterio
from rasterio.windows import Window
from rasterio.transform import rowcol
from rasterio.features import rasterize
from shapely import Point, points, wkt
from shapely import points as shapely_points
from sklearn.decomposition import PCA
from sklearn.discriminant_analysis import StandardScaler
from sklearn.metrics.pairwise import euclidean_distances, manhattan_distances

from tqdm import tqdm

from dask import delayed as dask_delayed, compute
from dask.distributed import Client as dask_client
import dask_geopandas as dgpd
from dask.diagnostics import ProgressBar

# from crop_suitability_predictor.config.config import
from crop_suitability_predictor.config.config import (
    ALL_FEATURES,
    CATEGORICAL_FEATURES,
    DRAINAGE_CLASS_VALS,
    TEXTURE_VALS,
)
from crop_suitability_predictor.modules.data.common_data_utils import (
    ConvertibleToCRS,
    load_cdl_code_map,
    load_county_boudnary_shape_file,
    load_crop_descriptions,
    load_points,
    load_crop_reports,
    DataStatus,
    check_existing_files,
    check_file_exists,
    check_file_exists_async,
    tqdm_joblib,
)


def check_cdl_conflicts(full_df):
    conflict_counts = (
        full_df.groupby(["county", "CDL Code"])["Crop Name"].nunique().reset_index()
    )
    conflicts = conflict_counts[conflict_counts["Crop Name"] > 1]
    if not conflicts.empty:
        print(
            "❌ Conflicts detected. Please resolve CDL Code conflicts within counties before proceeding."
        )
        print(conflicts)
        exit()


def get_crop_report_cdl_codes(crop_reports_dir: List[str]) -> np.array:
    crop_reports_df = load_crop_reports(crop_reports_dir=crop_reports_dir)
    cdl_codes = np.array(crop_reports_df["CDL Code"].unique())
    return cdl_codes


def get_crop_report_counties(crop_reports_dir: List[str]) -> np.array:
    crop_reports_df = load_crop_reports(crop_reports_dir=crop_reports_dir)
    counties = crop_reports_df["county"].unique()
    return counties


def normalize_and_convert_production(full_df):
    # Standardize and clean Unit column
    full_df["Unit"] = full_df["Unit"].astype(str).str.upper().str.strip()
    full_df["Unit"] = (
        full_df["Unit"]
        .replace({"LBS": "LB", "TON": "TONS", "500 LB BALE": "BALE"})
        .fillna("TONS")
    )

    # Standardize Max Unit
    full_df["Max Unit"] = full_df["Max Unit"].astype(str).str.upper().str.strip()

    # Clean Total Production column (remove commas, cast to float)
    full_df["Total Production"] = (
        full_df["Total Production"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    # Extract target unit from Max Unit
    def get_target_unit(max_unit):
        return max_unit.split("/")[0].strip()

    # Convert from current unit to LB
    def to_lb(row):
        val = row["Total Production"]
        unit = row["Unit"]
        if unit == "TONS":
            return val * 2000
        elif unit == "CWT":
            return val * 100
        elif unit == "40 LB CTN":
            return val * 40
        elif unit == "BALE":
            return val * 500
        elif unit == "LB":
            return val
        else:
            print(f"Unknown unit conversion for {row["Crop Name"]} from {unit} to lb")
            return None

    # Convert from LB to target unit
    def lb_to_target(lb_val, target_unit, crop_name):
        if target_unit == "TONS":
            return lb_val / 2000
        elif target_unit == "CWT":
            return lb_val / 100
        elif target_unit == "LB":
            return lb_val
        elif target_unit == "BOXES" and "ORANGE" in crop_name.upper():
            return lb_val / 40
        elif target_unit == "BU":
            if "CORN" in crop_name.upper():
                return lb_val / 56
            elif "BARLEY" in crop_name.upper():
                return lb_val / 48
            elif "OATS" in crop_name.upper():
                return lb_val / 32
            else:
                print(
                    f"Unknown unit conversion for {crop_name} from lb to {target_unit}"
                )
                return None

        else:
            print(f"Unknown unit conversion for {crop_name} from lb to {target_unit}")
            return None

    # Perform conversions
    rows_to_drop = []
    for i, row in full_df.iterrows():
        unit = row["Unit"]
        max_unit = row["Max Unit"]
        crop = row["Crop Name"]

        if unit and max_unit:
            target = get_target_unit(max_unit)
            if unit != target:
                lb_val = to_lb(row)
                if not lb_val:
                    rows_to_drop.append(i)
                    continue
                new_val = lb_to_target(lb_val, target, crop)
                if not new_val:
                    rows_to_drop.append(i)
                    continue
                full_df.at[i, "Total Production"] = round(new_val, 2)
                full_df.at[i, "Unit"] = target

    full_df["Crop Name"] = full_df["Crop Name"].str.strip()
    full_df["Total Production"] = pd.to_numeric(
        full_df["Total Production"], errors="coerce"
    )
    full_df["Harvested Acres"] = pd.to_numeric(
        full_df["Harvested Acres"], errors="coerce"
    )
    full_df["County Yield"] = full_df["Total Production"] / full_df["Harvested Acres"]

    full_df = full_df.dropna(subset=["Total Production", "CDL Code", "county"])

    return full_df


def merge_crop_and_max_prod_data(
    crop_report_data_path: str,
    max_yield_data_path: str,
    cdl_commodity_code_map_path: str,
) -> pd.DataFrame:

    # read in 2022 data
    df_2022 = pd.read_csv(crop_report_data_path)
    # read in max data
    max_yield_data = pd.read_csv(max_yield_data_path)
    max_yield_data = max_yield_data[["CDL Code", "Max Yield", "Unit"]]
    max_yield_data = max_yield_data.rename(columns={"Unit": "Max Unit"})
    # merge max data with 2022 data
    df_2022 = df_2022.merge(right=max_yield_data, on="CDL Code", how="left")
    match = (df_2022["Unit"] == df_2022["Max Unit"]).unique()
    if match.shape[0] != 1 and not match[0]:
        raise Exception("Unit Mismatch")

    return df_2022


def stream_batches(np_path, batch_size):
    data = np.load(np_path, mmap_mode="r")
    total_rows = data.shape[0]

    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        yield data[start:end]


def batch_to_dask_gdf(batch, crs="EPSG:4326"):
    xs = batch[:, 0]
    ys = batch[:, 1]
    crop_codes = batch[:, 2].astype(int)
    geometry = [Point(x, y) for x, y in zip(xs, ys)]
    gdf = gpd.GeoDataFrame({"crop_code": crop_codes}, geometry=geometry, crs=crs)
    return dgpd.from_geopandas(gdf, npartitions=1)


# def spatial_join_batch(
#     batch, county_dask_gdf, points_crs: ConvertibleToCRS, output_crs: ConvertibleToCRS
# ):


def spatial_join_and_write_batch(
    r_start: int,
    batch_size: int,
    npy_path: str,
    county_gdf: gpd.GeoDataFrame,
    points_crs: ConvertibleToCRS,
    output_crs: ConvertibleToCRS,
    output_file_path: str,
):
    data = np.load(npy_path, mmap_mode="r")
    batch = data[r_start : r_start + batch_size]

    # batch[:, 1] = x, batch[:, 2] = y
    geometries = points(batch[:, 1], batch[:, 2])

    # Create GeoDataFrame with index and crop_code
    gdf = gpd.GeoDataFrame(
        {"point_index": batch[:, 0].astype(int), "crop_code": batch[:, 3].astype(int)},
        geometry=geometries,
        crs=points_crs,
    ).to_crs(output_crs)

    # Spatial join
    joined = gpd.sjoin(gdf, county_gdf, predicate="within", how="left").drop(
        columns=["index_right"]
    )

    # Write results to file
    joined.to_csv(path_or_buf=output_file_path, mode="a", header=False, index=False)


def convert_cdl_raster_to_vector_points(
    down_sample_points: bool,
    original_raster_resolution: Optional[int],
    sampling_grid_resolution: Optional[int],
    n_jobs: int,
    cdl_raster_path: str,
    cdl_raster_npy_path: str,
    vector_points_npy_path,
    output_cdl_crs: ConvertibleToCRS,
    county_boundary_shape_file: str,
    county_shapefile_crs: ConvertibleToCRS,
    output_file_path: str,
    conversion_block_size=50,
    geo_conversion_batch_size: int = 10_000,
    filter_crop_codes: bool = True,
    filter_counties: bool = True,
    included_crop_codes: Optional[NDArray[np.int_]] = None,
    included_counties: Optional[List[str]] = None,
    overwrite_existing_file: bool = False,
):

    if not overwrite_existing_file and check_file_exists(output_file_path):
        return

    if overwrite_existing_file or not check_existing_files(vector_points_npy_path):
        if filter_counties and not (
            county_boundary_shape_file or county_shapefile_crs or included_counties
        ):
            exception_statement = "filter_points set to true, but included_counties, county_boundary_shape_file, county_shapefile_crs not provided"
            raise Exception(exception_statement)

        if filter_crop_codes and included_crop_codes is None:
            exception_statement = (
                "filter_points set to true, but included_crop_codes not provided"
            )
            raise Exception(exception_statement)

        if down_sample_points and not (
            original_raster_resolution or sampling_grid_resolution
        ):
            exception_statement = "down_sample_points set to true, original_raster_resolution or sampling_raster_resolution not provided"
            raise Exception(exception_statement)

        # mask out no data points and filter by requirements
        if overwrite_existing_file or not check_file_exists(cdl_raster_npy_path):
            with rasterio.open(cdl_raster_path) as src:
                data = src.read(1)
                raster_transform = src.transform
                raster_crs = src.crs
                raster_height, raster_width = src.height, src.width
                no_data_val = src.nodata

                mask = np.zeros_like(data, dtype=bool)
                if filter_counties:
                    counties = load_county_boudnary_shape_file(
                        shape_file_path=county_boundary_shape_file,
                        crs=county_shapefile_crs,
                    )

                    selected_counties = counties[
                        counties["NAME"].isin(included_counties)
                    ].to_crs(raster_crs)

                    county_mask = rasterize(
                        [(geom, 1) for geom in selected_counties.geometry],
                        out_shape=(raster_height, raster_width),
                        transform=raster_transform,
                        fill=0,
                        dtype="uint8",
                    )
                    mask |= county_mask == 0

                if filter_crop_codes:
                    mask |= ~np.isin(data, included_crop_codes)
                mask[data == no_data_val] = False
                rows, cols = np.where(mask)
                data[rows, cols] = no_data_val
                np.save(file=cdl_raster_npy_path, arr=data)
                del data
                gc.collect()
        else:
            with rasterio.open(cdl_raster_path) as src:
                raster_transform = src.transform
                raster_crs = src.crs
                raster_height, raster_width = src.height, src.width
                no_data_val = src.nodata

        if down_sample_points:
            raw_vector_points = down_sample_crop_points(
                cdl_npy_path=cdl_raster_npy_path,
                raster_height=raster_height,
                raster_width=raster_width,
                raster_transform=raster_transform,
                raster_no_data_val=no_data_val,
                sampling_grid_resolution=sampling_grid_resolution,
                original_raster_resolution=original_raster_resolution,
                n_jobs=n_jobs,
            )
        else:
            raw_vector_points = covert_full_raster(
                cdl_npy_path=cdl_raster_npy_path,
                raster_height=raster_height,
                raster_width=raster_width,
                raster_transform=raster_transform,
                raster_no_data_val=no_data_val,
                n_jobs=n_jobs,
                conversion_block_size=conversion_block_size,
            )

        indices = np.arange(raw_vector_points.shape[0]).reshape(-1, 1)
        raw_vector_points = np.hstack((indices, raw_vector_points))
        total_rows = raw_vector_points.shape[0]
        np.save(vector_points_npy_path, raw_vector_points)
        del raw_vector_points
        gc.collect()
    else:
        vector_points_npy_shape = get_npy_shape(vector_points_npy_path)
        total_rows = vector_points_npy_shape[0]

    county_gdf = load_county_boudnary_shape_file(
        shape_file_path=county_boundary_shape_file, crs=county_shapefile_crs
    ).to_crs(output_cdl_crs)

    r_starts = list(range(0, total_rows, geo_conversion_batch_size))
    pd.DataFrame(columns=["point_index", "crop_code", "geometry", "NAME"]).to_csv(
        output_file_path, index=False
    )
    Parallel(n_jobs=n_jobs)(
        delayed(spatial_join_and_write_batch)(
            r_start,
            geo_conversion_batch_size,
            vector_points_npy_path,
            county_gdf,
            raster_crs,
            output_cdl_crs,
            output_file_path,
        )
        for r_start in tqdm(r_starts, desc="Running spatial joins in parallel")
    )


def get_npy_shape(path):
    with open(path, "rb") as f:
        magic = np.lib.format.read_magic(f)
        if magic == (1, 0):
            header = np.lib.format.read_array_header_1_0(f)
        elif magic == (2, 0):
            header = np.lib.format.read_array_header_2_0(f)
        else:
            raise ValueError("Unsupported .npy format version")
        shape = header[0]["shape"]
        return shape


def covert_full_raster(
    cdl_npy_path: str,
    raster_height: int,
    raster_width: int,
    raster_transform: Affine,
    raster_no_data_val: int,
    n_jobs: int = 4,
    conversion_block_size: int = 50,
):

    tasks = []
    for row in range(0, raster_height, conversion_block_size):
        for col in range(0, raster_width, conversion_block_size):
            row_end = min(row + conversion_block_size, raster_height)
            col_end = min(col + conversion_block_size, raster_width)
            tasks.append((row, row_end, col, col_end))

    results = Parallel(n_jobs=n_jobs)(
        delayed(process_full_block)(
            r0, r1, c0, c1, raster_transform, raster_no_data_val, cdl_npy_path
        )
        for r0, r1, c0, c1 in tqdm(tasks, desc="Processing Blocks")
    )

    filtered = [
        r for r in results if r is not None and r.size > 0 and not np.all(np.isnan(r))
    ]

    stacked = np.vstack(filtered)
    return stacked


def process_full_block(r_start, r_end, c_start, c_end, transform, no_data_val, np_path):
    data = np.load(np_path, mmap_mode="r")
    block = data[r_start:r_end, c_start:c_end]
    rows, cols = np.meshgrid(
        np.arange(r_start, r_end), np.arange(c_start, c_end), indexing="ij"
    )

    valid_mask = ~(block == no_data_val)
    if not np.any(valid_mask):
        return None

    flat_vals = block[valid_mask].astype(int)
    flat_rows = rows[valid_mask]
    flat_cols = cols[valid_mask]

    xs, ys = transform * (flat_cols, flat_rows)
    return np.column_stack((xs, ys, flat_vals))


def save_raster_in_batches(
    temp_path, output_csv_path, original_raster_crs, output_crs, batch_size
):
    # If the file exists, remove to avoid header duplication
    if os.path.exists(output_csv_path):
        os.remove(output_csv_path)

    # Load from .npy using memory mapping (doesn't load into RAM)
    stacked = np.load(temp_path, mmap_mode="r")
    total_rows = stacked.shape[0]

    for i in tqdm(range(0, total_rows, batch_size)):
        batch = stacked[i : i + batch_size]
        xs = batch[:, 0].astype(np.float32)
        ys = batch[:, 1].astype(np.float32)
        crop_codes = batch[:, 2].astype(int)

        geometry = list(points(xs, ys))
        gdf = gpd.GeoDataFrame(
            {"crop_code": crop_codes},
            geometry=geometry,
            crs=original_raster_crs,
        ).to_crs(output_crs)

        # Write first batch with header, others in append mode
        gdf.to_csv(output_csv_path, mode="a", header=(i == 0), index=False)
        tqdm.write(f"✅ Wrote rows {i} to {min(i + batch_size, total_rows)}")
        del batch, gdf, geometry, xs, ys, crop_codes
        gc.collect()


def sample_block(r_start, width, height, block_size, transform, no_data_val, np_path):
    data = np.load(np_path, mmap_mode="r")
    # block = data[r_start:r_end, c_start:c_end]
    # rows, cols = np.meshgrid(
    #     np.arange(r_start, r_end), np.arange(c_start, c_end), indexing="ij"
    # )

    # valid_mask = ~(block == no_data_val)  # np.isnan(block)
    # if not np.any(valid_mask):
    #     return None

    # flat_vals = block[valid_mask].astype(int)
    # flat_rows = rows[valid_mask]
    # flat_cols = cols[valid_mask]
    # mode_val = np.bincount(flat_vals).argmax()
    # mean_row = np.round(flat_rows.mean()).astype(int)
    # mean_col = np.round(flat_cols.mean()).astype(int)
    # center_x, center_y = transform * (mean_col, mean_row)
    # return np.array([center_x, center_y, mode_val])
    r_end = min(r_start + block_size, height)
    results = []
    block = data[r_start:r_end, :]
    if not np.any(~(block == no_data_val)):
        return results
    for c_start in range(0, width, block_size):
        block = data[r_start:r_end, c_start : c_start + block_size]
        valid_mask = ~(block == no_data_val)
        if not np.any(valid_mask):
            continue
        mode = np.argmax(np.bincount(block[valid_mask])).astype(int)
        row_indices, col_indices = np.where(valid_mask)
        row_indices += r_start
        col_indices += c_start
        mean_row = np.round(row_indices.mean()).astype(int)
        mean_col = np.round(col_indices.mean()).astype(int)
        center_x, center_y = transform * (mean_col, mean_row)
        results.append((center_x, center_y, mode))
    return results


def down_sample_crop_points(
    cdl_npy_path: str,
    raster_height: int,
    raster_width: int,
    raster_transform: Affine,
    raster_no_data_val: int,
    sampling_grid_resolution: int,
    original_raster_resolution: int,
    n_jobs: int,
):

    sampling_block_size = sampling_grid_resolution // original_raster_resolution

    r_starts = []
    for r_start in range(0, raster_height, sampling_block_size):
        r_starts.append(r_start)

    results = Parallel(n_jobs=n_jobs)(
        delayed(sample_block)(
            r_start,
            raster_width,
            raster_height,
            sampling_block_size,
            raster_transform,
            raster_no_data_val,
            cdl_npy_path,
        )
        for r_start in tqdm(r_starts, desc="Sampling Blocks")
    )

    filtered = [r for r in results if r is not None and len(r) > 0]
    stacked = np.vstack(filtered)
    return stacked


def initialize_output_file(output_path: str):
    if not check_file_exists(output_path):
        with open(output_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["point_index", "ndvi_value"])


def compute_ndvi(red_band, nir_band):
    ndvi = (nir_band - red_band) / (nir_band + red_band + 1e-6)
    # print(np.unique(ndvi))
    return np.clip(ndvi, -1, 1)


@dask_delayed
def process_scene_dask(b4_path: str, points_path: str) -> list:
    scene_id = os.path.basename(b4_path).replace("_B4.TIF", "")
    b5_path = b4_path.replace("_B4.TIF", "_B5.TIF")

    if not os.path.exists(b5_path):
        print(f"⚠️ B5 missing for {scene_id}, skipping.")
        return []

    try:
        # Read points
        points = gpd.read_parquet(points_path)
        xs = points.geometry.x.values
        ys = points.geometry.y.values
        coords = list(zip(xs, ys))
        point_indices = points["point_index"].values

        with rasterio.open(b4_path) as red_src, rasterio.open(b5_path) as nir_src:
            red_no_data_val = red_src.nodata
            nir_no_data_val = nir_src.nodata
            red_vals = np.array(
                [val[0] for val in red_src.sample(coords)], dtype="float32"
            )
            nir_vals = np.array(
                [val[0] for val in nir_src.sample(coords)], dtype="float32"
            )

        # Compute NDVI only where both bands are valid
        # valid_mask = (~(red_vals == 0)) & (~(nir_vals == 0))
        invalid_red = np.isin(red_vals, [0, red_no_data_val]) | np.isnan(red_vals)
        invalid_nir = np.isin(nir_vals, [0, nir_no_data_val]) | np.isnan(nir_vals)

        valid_mask = ~(invalid_red | invalid_nir)

        if not np.any(valid_mask):
            return []
        red_vals = red_vals[valid_mask]
        nir_vals = nir_vals[valid_mask]
        valid_indices = point_indices[valid_mask]
        # valid_indices = np.where(valid_mask)[0]

        ndvi_vals = compute_ndvi(red_vals, nir_vals)
        # print("Scene:", scene_id)
        # print(
        #     "Red band stats:", np.min(red_vals), np.max(red_vals), np.median(red_vals)
        # )
        # print(
        #     "NIR band stats:", np.min(nir_vals), np.max(nir_vals), np.median(nir_vals)
        # )
        # print("NDVI stats:", np.min(ndvi_vals), np.max(ndvi_vals), np.median(ndvi_vals))
        return list(zip(valid_indices, ndvi_vals.tolist()))

    except Exception as e:
        print(f"❌ Error with scene {scene_id}: {e}")
        return []


def process_all_scenes(
    band_folder: str, points_file_path: str, points_crs, output_file: str
):

    landsat_crs = set()
    points = load_points(crop_points_file_path=points_file_path, crs=points_crs)
    with tempfile.TemporaryDirectory() as tmpdirname:
        print("Temp directory path:", tmpdirname)
        b4_files = sorted(glob.glob(os.path.join(band_folder, "*_B4.TIF")))
        scene_id_to_crs_dict = dict()
        crs_to_file_path = dict()
        for file in b4_files:
            with rasterio.open(file) as src:
                if src.crs not in landsat_crs:
                    landsat_crs.add(src.crs)
                    points = points.to_crs(src.crs)
                    temp_file_name = f"points_projected_{src.crs}"
                    temp_file_path = os.path.join(tmpdirname, temp_file_name)
                    points.to_parquet(temp_file_path, engine="pyarrow", index=False)

                    crs_to_file_path[src.crs] = temp_file_path
                scene_id = re.sub(r"_B\d\.TIF$", "", os.path.basename(file))
                scene_id_to_crs_dict[scene_id] = src.crs
        del points
        gc.collect()

        tasks = [
            process_scene_dask(
                b4_path,
                crs_to_file_path[
                    scene_id_to_crs_dict[
                        re.sub(r"_B\d\.TIF$", "", os.path.basename(b4_path))
                    ]
                ],
            )
            for b4_path in b4_files
        ]
        with ProgressBar():
            results = compute(*tasks)
            results = [r for r in results if r is not None]
            with open(output_file, "a", newline="") as f:
                writer = csv.writer(f)
                for scene_results in results:
                    if scene_results:
                        writer.writerows(scene_results)


def compute_and_merge_mean_ndvi(
    points_path: str, points_crs: ConvertibleToCRS, ndvi_values_path: str
) -> gpd.GeoDataFrame:

    points = load_points(crop_points_file_path=points_path, crs=points_crs)
    ndvi_df = pd.read_csv(ndvi_values_path)
    mean_ndvi_df = ndvi_df.groupby("point_index")["ndvi_value"].mean().reset_index()

    # Use column instead of index to map NDVI
    points["mean_ndvi"] = points["point_index"].map(
        mean_ndvi_df.set_index("point_index")["ndvi_value"]
    )

    # points.to_csv(output_file_path, index=False)
    return points


def read_in_point_amounts(input_file_path: str) -> Tuple:
    with open(input_file_path, "r") as file:
        data = json.load(file)
        return data["total_points_unfiltered"], data["total_points_filtered"]


def compute_point_estimates_and_suitability_scores(
    is_down_sampled: bool,
    points_file_path: str,
    county_and_yield_data_file_path: str,
    point_acre_conversion: float,
    output_file_path: Optional[str] = None,
):
    POINT_ACRES_CONVERSION = point_acre_conversion
    if is_down_sampled:
        # read in the total points length
        total_points, _ = read_in_point_amounts(input_file_path=points_file_path)
        coverage_ratio = len(matched_points) / total_points
    else:
        coverage_ratio = 1

    points_df = pd.read_csv(points_file_path)
    county_df = pd.read_csv(county_and_yield_data_file_path)
    point_results = []
    grouped = county_df.groupby(["county", "CDL Code"])

    for (county, crop_code), row in grouped:
        total_production = row["Total Production"].sum()
        # Adjust the total production accordingly
        adjusted_total_production = total_production * coverage_ratio
        max_yield = row["Max Value"].mean()

        matched_points = points_df[
            (points_df["county_name"] == county) & (points_df["crop_code"] == crop_code)
        ].copy()

        if matched_points.empty:
            continue

        matched_points["ndvi_weight"] = matched_points["mean_ndvi"]
        ndvi_sum = matched_points["ndvi_weight"].sum()

        if ndvi_sum == 0:
            matched_points["ndvi_weight"] = 1 / len(matched_points)
        else:
            matched_points["ndvi_weight"] /= ndvi_sum

        matched_points["assigned_production"] = (
            matched_points["ndvi_weight"] * adjusted_total_production
        )
        matched_points["estimated_yield"] = (
            matched_points["assigned_production"] / POINT_ACRES_CONVERSION
        )
        matched_points["suitability_score"] = (
            matched_points["estimated_yield"] / max_yield
        ).clip(upper=0.95)

        point_results.append(
            matched_points[
                [
                    "id",
                    "crop_code",
                    "county_name",
                    "geometry",
                    "mean_ndvi",
                    "assigned_production",
                    "estimated_yield",
                    "suitability_score",
                ]
            ]
        )

    if output_file_path:
        point_results.to_csv(output_file_path)
    return (
        pd.concat(point_results, ignore_index=True) if point_results else pd.DataFrame()
    )


def worker_compute_crop_scores(args):
    (
        crop_code,
        shm_npy_name,
        shape_df,
        dtype_df,
        numeric_range,
        ordinal_range,
        column_dict,
        batch_size,
        cdl_map,
        progress_position,
    ) = args
    print(f"Processing Crop {crop_code}")
    crop_code_idx = column_dict["crop_code"]
    suitability_score_idx = column_dict["suitability_score"]

    # Access shared memory as view
    shm_npy = shared_memory.SharedMemory(name=shm_npy_name)
    shm_all = np.ndarray(shape_df, dtype=np.dtype(dtype_df), buffer=shm_npy.buf)

    # Locate rows for the current crop
    left = np.searchsorted(shm_all[:, crop_code_idx], crop_code, side="left")
    right = np.searchsorted(shm_all[:, crop_code_idx], crop_code, side="right")

    crop_subset = shm_all[left:right]
    y_crop_scores = crop_subset[:, suitability_score_idx]
    num_start_idx, num_end_idx = numeric_range
    ord_start_idx, ord_end_idx = ordinal_range

    x_crop_numeric = crop_subset[:, num_start_idx : num_end_idx + 1]
    x_crop_ordinal = crop_subset[:, ord_start_idx : ord_end_idx + 1]

    all_scores = np.zeros(shm_all.shape[0])
    all_weights = np.zeros(shm_all.shape[0])

    distances = np.empty((x_crop_numeric.shape[0], batch_size), dtype=np.float32)
    weights = np.empty((x_crop_numeric.shape[0], batch_size), dtype=np.float32)
    numerator = np.empty(batch_size, dtype=np.float32)
    denominator = np.empty(batch_size, dtype=np.float32)

    D_numeric = np.empty((x_crop_numeric.shape[0], batch_size), dtype=np.float32)
    D_ordinal = np.empty((x_crop_ordinal.shape[0], batch_size), dtype=np.float32)

    # First half of the data (before the matching crop)
    for j_start in tqdm(
        range(0, left, batch_size),
        position=progress_position,
        desc=f"Crop {crop_code} (before)",
        leave=False,
    ):
        j_end = min(j_start + batch_size, left)
        x_batch_numeric = shm_all[j_start:j_end, num_start_idx : num_end_idx + 1]
        x_batch_ordinal = shm_all[j_start:j_end, ord_start_idx : ord_end_idx + 1]

        actual_batch_size = j_end - j_start

        # Use views of correct shape
        Dn = D_numeric[:, :actual_batch_size]
        Do = D_ordinal[:, :actual_batch_size]
        dist = distances[:, :actual_batch_size]
        wts = weights[:, :actual_batch_size]
        num = numerator[:actual_batch_size]
        denom = denominator[:actual_batch_size]

        Dn[:] = euclidean_distances(x_crop_numeric, x_batch_numeric)
        Dn /= Dn.max()

        Do[:] = manhattan_distances(x_crop_ordinal, x_batch_ordinal)
        Do /= Do.max()

        np.add(Dn, Do, out=dist)
        dist *= 0.5

        np.divide(1.0, dist + 1e-6, out=wts)

        num[:] = wts.T @ y_crop_scores
        denom[:] = wts.sum(axis=0)

        all_scores[j_start:j_end] = num / denom
        all_weights[j_start:j_end] = denom

    # Second half of the data (after the matching crop)
    for j_start in tqdm(
        range(right, shm_all.shape[0], batch_size),
        position=progress_position,
        desc=f"Crop {crop_code} (after)",
        leave=False,
    ):
        j_end = min(j_start + batch_size, shm_all.shape[0])
        actual_batch_size = j_end - j_start
        x_batch_numeric = shm_all[j_start:j_end, num_start_idx : num_end_idx + 1]
        x_batch_ordinal = shm_all[j_start:j_end, ord_start_idx : ord_end_idx + 1]

        # Use views of correct shape
        Dn = D_numeric[:, :actual_batch_size]
        Do = D_ordinal[:, :actual_batch_size]
        dist = distances[:, :actual_batch_size]
        wts = weights[:, :actual_batch_size]
        num = numerator[:actual_batch_size]
        denom = denominator[:actual_batch_size]

        Dn[:] = euclidean_distances(x_crop_numeric, x_batch_numeric)
        Dn /= Dn.max()

        Do[:] = manhattan_distances(x_crop_ordinal, x_batch_ordinal)
        Do /= Do.max()

        np.add(Dn, Do, out=dist)
        dist *= 0.5

        np.divide(1.0, dist + 1e-6, out=wts)

        num[:] = wts.T @ y_crop_scores
        denom[:] = wts.sum(axis=0)

        all_scores[j_start:j_end] = num / denom
        all_weights[j_start:j_end] = denom

    print(f"finished crop {crop_code}")
    # Fill in known crop scores
    all_scores[left:right] = shm_all[left:right, suitability_score_idx]
    crop_name = cdl_map.get(crop_code, f"CROP_{crop_code}").upper().replace(" ", "_")
    point_index = shm_all[:, column_dict["point_index"]]
    # Cleanup
    shm_npy.close()  # Close shared memory handle
    del shm_all, crop_subset, y_crop_scores, x_crop_numeric, x_crop_ordinal, all_weights
    gc.collect()

    return crop_name, all_scores, point_index


def compute_weighted_suitability_scores(
    soil_features_path: str,
    suitability_scores_path: str,
    cdl_map_path: str,
    batch_size: int = 750,
):
    # Read and merge data
    soil_df = read_in_cleaned_soil_data(file_path=soil_features_path)
    suitability_df = pd.read_csv(suitability_scores_path)
    cdl_map = pd.read_csv(cdl_map_path)
    # cdl_map = dict(zip(cdl_map_df["crop_code"], cdl_map_df["crop_name"]))

    df = (
        pd.merge(soil_df, suitability_df, on="point_index", how="right")
        .dropna()
        .drop(columns=["geometry"])
    )

    numeric_features = list(
        set(df.columns)
        - set(CATEGORICAL_FEATURES).union(
            {"point_index", "suitability_score", "crop_code"}
        )
    )

    # apply standard scalar to numeric features
    scaler = StandardScaler()
    X_all_numeric = scaler.fit_transform(df[numeric_features])
    # convert entire dataframe to np array
    cols = ["point_index", "suitability_score", "crop_code"]
    cols.extend(CATEGORICAL_FEATURES)
    other_data = df[cols].to_numpy()
    combined_array = np.concatenate([other_data, X_all_numeric], axis=1)
    cols.extend(numeric_features)
    # make a dict of column names to indicies

    column_dict = {col: idx for idx, col in enumerate(cols)}

    numeric_range = (
        column_dict[numeric_features[0]],
        column_dict[numeric_features[-1]],
    )
    ordinal_range = (
        column_dict[CATEGORICAL_FEATURES[0]],
        column_dict[CATEGORICAL_FEATURES[-1]],
    )
    # sort by crop code
    sort_idx = np.argsort(combined_array[:, column_dict["crop_code"]])
    combined_array = combined_array[sort_idx]
    # store numpy array in memory
    shm_df = shared_memory.SharedMemory(create=True, size=combined_array.nbytes)
    shared_npy = np.ndarray(
        combined_array.shape, dtype=combined_array.dtype, buffer=shm_df.buf
    )
    np.copyto(shared_npy, combined_array)
    # crop_codes = df["crop_code"].unique()
    crop_codes = sorted(
        df["crop_code"].unique(), key=lambda c: -(df["crop_code"] == c).sum()
    )
    results_df = (
        df[cols]
        .set_index("point_index")
        .copy()
        .drop(columns=["crop_code", "suitability_score"])
    )
    del combined_array, soil_df, suitability_df, df
    gc.collect()

    # crop_code,
    #     shm_df_name,
    #     shape_df,
    #     dtype_df,
    #     numeric_range,
    #     ordinal_range,
    #     column_dict,
    #     batch_size,
    #     cdl_map,

    with Pool(processes=4) as pool:
        args = [
            (
                crop_code,
                shm_df.name,
                shared_npy.shape,
                shared_npy.dtype.name,
                numeric_range,
                ordinal_range,
                column_dict,
                batch_size,
                cdl_map,
                i + 1,  # position offset (1-based because 0 is the global tqdm bar)
            )
            for i, crop_code in enumerate(crop_codes)
        ]
        results = list(
            tqdm(pool.imap(worker_compute_crop_scores, args), total=len(args))
        )
    # results = []

    # for crop_code in crop_codes:
    #     args = (
    #         crop_code,
    #         shm_df.name,
    #         shared_npy.shape,
    #         shared_npy.dtype.name,
    #         numeric_range,
    #         ordinal_range,
    #         column_dict,
    #         batch_size,
    #         cdl_map,
    #     )

    #     result = worker_compute_crop_scores(args)  # direct call instead of pool
    #     results.append(result)

    # Clean up shared memory
    shm_df.close()
    shm_df.unlink()

    # Assemble result dataframe

    for crop_name, scores, point_indices in results:
        results_df.loc[point_indices, crop_name] = scores

    return results_df


# def compute_soil_similarity_scores(points: pd.DataFrame, points_scaled:  pd.DataFrame, cdl_map, n_jobs):

# iterate through every point
# get the points crop code
# iterate through every other point
# calculate the distance from that point to


# X_all = points_scaled[ALL_FEATURES].values
# results = pd.DataFrame(index=points.index)
# crop_codes = points["crop_code"].unique()
# for crop_code in tqdm(points["crop_code"].unique(), desc="Computing soil-based suitability"):
#     crop_mask = points["crop_code"] == crop_code
#     X_crop = points_scaled.loc[crop_mask, ALL_FEATURES].values
#     y_crop = points.loc[crop_mask, "suitability_score"].values

#     suitability_scores = []
#     for i, x in enumerate(tqdm(X_all, desc=f"  → Crop {crop_code}", leave=False)):
#         row = points.iloc[i]
#         if row["crop_code"] == crop_code:
#             suitability_scores.append(row["suitability_score"])
#         else:
#             distances = np.linalg.norm(X_crop - x, axis=1)
#             weights = 1 / (distances + 1e-6)
#             score = np.average(y_crop, weights=weights)
#             suitability_scores.append(score)

#     crop_name = cdl_map.get(crop_code, f"CROP_{crop_code}").upper().replace(" ", "_")
#     results[f"soil_suitability_{crop_name}"] = suitability_scores
# with tqdm(desc = "", total = crop_codes) as pbar:
#     def wrapped_compute(code):
#         result = compute_crop_suitability(code, X_all, points, points_scaled, cdl_map):
#         pbar.update(1)
#         return result

#     results = Parallel(n_jobs=n_jobs) (
#         delayed(wrapped_compute)(code) for code in crop_codes
#     )

# return pd.concat(results, axis=1)


def sample_soil_data(
    vector_points_npy_path: str,
    soil_raster_paths: List[str],
    soil_raster_features: List[str],
    output_crs: int,
    crs_points: int = 3857,
    crs_soil: int = 5070,
) -> pd.DataFrame:
    """
    Samples soil raster values at crop point locations and returns a GeoDataFrame
    containing the extracted soil features.

    Parameters
    ----------
    vector_points_npy_path : str
        Path to NumPy .npy file containing shape (N, 4) with [point_index, x, y, crop_code].

    soil_raster_paths : List[str]
        List of file paths to soil raster layers (GeoTIFFs), each representing one soil feature.

    soil_raster_features : List[str]
        List of feature names corresponding to each raster. Must match order of raster paths.

    output_crs : int
        Desired CRS for the output GeoDataFrame.

    crs_points : int
        CRS of the raw points (e.g., 3857).

    crs_soil : int
        CRS that all soil rasters are assumed to be in (e.g., 5070).

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame with one row per point and columns for each soil feature, crop_code,
        point_index, and geometry.
    """
    assert len(soil_raster_paths) == len(
        soil_raster_features
    ), "Mismatch between rasters and feature names"

    # Load raw points
    data = np.load(vector_points_npy_path)
    point_indices = data[:, 0].astype(int)
    xs, ys = data[:, 1], data[:, 2]

    # Reproject coordinates to raster CRS
    transformer = Transformer.from_crs(crs_points, crs_soil, always_xy=True)
    xs_reproj, ys_reproj = transformer.transform(xs, ys)
    coords = np.column_stack([xs_reproj, ys_reproj])

    # Initialize feature matrix
    feature_matrix = np.full(
        (len(coords), len(soil_raster_paths)), np.nan, dtype="float32"
    )

    # Sample each raster
    for i, (raster_path, feature_name) in tqdm(
        enumerate(zip(soil_raster_paths, soil_raster_features)),
        total=len(soil_raster_features),
        desc="Sampling soil rasters",
    ):
        with rasterio.open(raster_path) as src:
            vals = np.array(list(src.sample(coords)), dtype="float32")[:, 0]
            if src.nodata is not None:
                nodata_mask = vals == src.nodata
                vals[nodata_mask] = np.nan
            feature_matrix[:, i] = vals

    # Create GeoDataFrame
    # geoms = gpd.points_from_xy(xs, ys)
    df = pd.DataFrame(
        {
            "point_index": point_indices,
            **{
                name: feature_matrix[:, i]
                for i, name in enumerate(soil_raster_features)
            },
        }
    )

    # # Save for later inspection (optional)
    # gdf.to_csv(
    #     "/Users/varunwadhwa/Desktop/crop-suitability-predictor/data/processed/intermediate_processed_files/soil/soil_features.csv",
    #     index=False,
    # )

    return df


def read_in_cleaned_soil_data(file_path: str):

    soil_df = pd.read_csv(file_path)
    # soil_df["drainage_class_int"] = pd.Categorical(
    #     soil_df["drainage_class_int"],
    #     categories=DRAINAGE_CLASS_VALS,
    #     ordered=True,
    # )
    # soil_df["texture_05"] = pd.Categorical(
    #     soil_df["texture_05"],
    #     categories=TEXTURE_VALS,
    #     ordered=True,
    # )
    # soil_df["texture_025"] = pd.Categorical(
    #     soil_df["texture_025"],
    #     categories=TEXTURE_VALS,
    #     ordered=True,
    # )
    # soil_df["texture_2550"] = pd.Categorical(
    #     soil_df["texture_2550"],
    #     categories=TEXTURE_VALS,
    #     ordered=True,
    # )

    return soil_df


def perform_pca_on_soil_features(
    soil_gdf_path: str,
    numerical_cols=List,
    n_components: int = 5,
) -> pd.DataFrame:
    """
    Reduces dimensionality of soil features using PCA.

    Parameters
    ----------
    soil_gdf : geopandas.GeoDataFrame
        GeoDataFrame containing soil features and geometry.

    n_components : int
        Number of principal components to retain.

    Returns
    -------
    geopandas.GeoDataFrame
        GeoDataFrame with PCA components added as columns (pca_1, pca_2, ...).
    """

    soil_df = pd.read_csv(soil_gdf_path)

    # Extract only the numeric columns (exclude 'geometry')

    feature_cols = numerical_cols
    for col in feature_cols:
        soil_df[col] = pd.to_numeric(soil_df[col], errors="coerce")
    feature_matrix = soil_df[feature_cols].values
    non_numeric_counts = {}

    # for col in soil_gdf[feature_cols]:
    #     # Attempt to coerce each value to numeric (NaNs will appear where coercion fails)
    #     coerced = pd.to_numeric(soil_gdf[col], errors="coerce")
    #     # Count non-numeric entries as those that were not NaN before but became NaN
    #     original = soil_gdf[col]
    #     non_numeric_mask = coerced.isna() & original.notna()
    #     non_numeric_counts[col] = non_numeric_mask.sum()

    # # Display only columns that had any non-numeric entries
    # non_numeric_counts = {k: v for k, v in non_numeric_counts.items() if v > 0}
    # print("Non-numeric entry counts:")
    # for col, count in non_numeric_counts.items():
    #     print(f"{col}: {count}")

    # Handle missing values (e.g., via imputation or dropping rows)
    # For simplicity, drop rows with NaNs
    valid_mask = ~np.isnan(feature_matrix).any(axis=1)
    clean_features = feature_matrix[valid_mask]

    # Standardize features
    scaler = StandardScaler()
    standardized = scaler.fit_transform(clean_features)

    # Perform PCA
    pca = PCA(n_components=n_components)
    pca_components = pca.fit_transform(standardized)

    # Create new columns for PCA results
    pca_cols = [f"pca_{i+1}" for i in range(n_components)]
    pca_df = pd.DataFrame(
        pca_components, columns=pca_cols, index=soil_df[valid_mask].index
    )

    # Merge PCA components back into GeoDataFrame
    soil_df_pca = soil_df.copy()
    for col in pca_cols:
        soil_df_pca.loc[pca_df.index, col] = pca_df[col]
    soil_df_pca.drop(columns=numerical_cols, inplace=True)

    return soil_df_pca


def merge_soil_data(
    cumulative_data_path: str, soil_data_path: str, crs: ConvertibleToCRS
):
    cumulative_df = load_points(crop_points_file_path=cumulative_data_path, crs=crs)
    soil_df = pd.read_csv(soil_data_path)
    cumulative_df.merge(soil_df, how="left", on="point_index")


def assing_suitability_score(
    points_path: str,
    points_crs: ConvertibleToCRS,
    country_prod_data_path: str,
    point_acre_conversion: float,
    coverage_ratio: float = 1,
) -> pd.DataFrame:

    # load points data
    points_df = load_points(crop_points_file_path=points_path, crs=points_crs)
    # load county yield data
    county_prod_df = pd.read_csv(
        country_prod_data_path,
        usecols=[
            "CDL Code",
            "Harvested Acres",
            "Production",
            "Unit",
            "County",
            "Max Yield",
            "Max Unit",
        ],
    )
    county_prod_df = county_prod_df.rename(
        columns={
            "CDL Code": "crop_code",
            "Harvested Acres": "harvested_acres",
            "Production": "total_production",
            "Unit": "unit",
            "County": "county",
            "Max Yield": "max_value",
            "Max Unit": "max_unit",
        }
    )

    # filter out not matching points via df merge, keeping only matches of county name and crop crode
    valid_keys = county_prod_df[["crop_code", "county"]].drop_duplicates()

    points_df["crop_code"] = points_df["crop_code"].astype(int)
    points_df["county"] = points_df["county"].str.strip()

    county_prod_df["crop_code"] = county_prod_df["crop_code"].astype(int)
    county_prod_df["county"] = county_prod_df["county"].astype(str).str.strip()

    county_prod_df["total_production"] = county_prod_df["total_production"].astype(
        float
    )

    points_df = points_df.merge(
        how="inner", right=valid_keys, on=["crop_code", "county"]
    )
    valid_keys = points_df[["crop_code", "county"]].drop_duplicates()
    county_prod_df = county_prod_df.merge(
        how="inner", right=valid_keys, on=["crop_code", "county"]
    )

    # function to run in parallel
    def process_group(crop_code, county):

        # get point matches
        point_matches = points_df[
            (points_df["crop_code"] == crop_code) & (points_df["county"] == county)
        ].copy()

        # if len(point_matches) == 0:
        #     print("❌ No match for:", repr(county), repr(crop_code))
        #     print(
        #         "→ Close counties:",
        #         points_df[points_df["crop_code"] == crop_code]["county"].unique(),
        #     )

        # get total ndvi
        total_ndvi = point_matches["mean_ndvi"].sum()

        # get altered total production (coverage ratio * total production)
        total_prod = (
            county_prod_df[
                (county_prod_df["crop_code"] == crop_code)
                & (county_prod_df["county"] == county)
            ]["total_production"].iloc[0]
            * coverage_ratio
        )
        max_yield = county_prod_df[
            (county_prod_df["crop_code"] == crop_code)
            & (county_prod_df["county"] == county)
        ]["max_value"].max()

        if total_ndvi == 0:
            point_matches["weight"] = 1 / len(point_matches)
        else:
            # assign weight for each point (point ndvi / total ndvi)
            point_matches["weight"] = point_matches["mean_ndvi"] / total_ndvi

        # assign production for each point, multiply weight by altered total production
        point_matches["assigned_prod"] = point_matches["weight"] * total_prod

        point_matches["estimated_yield"] = (
            point_matches["assigned_prod"] / point_acre_conversion
        )

        point_matches["suitability_score"] = (
            point_matches["estimated_yield"] / max_yield
        ).clip(lower=0.1, upper=0.95)

        # if ["weight", "assigned_prod", "estimated_yield"] in point_matches.columns:
        #     point_matches.drop(
        #         ["weight", "assigned_prod", "estimated_yield"], inplace=True
        #     )
        # else:
        #     print("not here")
        return point_matches

    # === Group county data and run in parallel ===

    grouped = county_prod_df.groupby(["county", "crop_code"])
    num_threads = min(32, multiprocessing.cpu_count())
    results = Parallel(n_jobs=num_threads, backend="threading")(
        delayed(process_group)(crop_code, county) for (county, crop_code), _ in grouped
    )

    # for (county, crop_code), _ in grouped:
    #     process_group(crop_code=crop_code, county=county)

    # === Filter out empty results, concatenate and save ===
    results = [r for r in results if r is not None]

    final_df = pd.concat(results, ignore_index=True)
    final_df = final_df[["point_index", "crop_code", "geometry", "suitability_score"]]
    return final_df
    # final_df.to_csv(output_path, index=False)
    # print(f"✅ Done! Saved results")


# def merge_soil_and_point_data(crop_data_path: str, crop_data_crs, soil_data_path: str, soil_data_crs):
#     crop_df = load_points(crop_points_file_path=crop_data_path, crs=crop_data_crs)
#     soil_df = load_points(soil_data_path, crs=soil_data_crs)
#     merged_df = crop_df.
