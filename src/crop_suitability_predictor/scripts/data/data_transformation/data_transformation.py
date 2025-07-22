import os
from crop_suitability_predictor.config.config import (
    ALL_FEATURES,
    CA_SHAPEFILE_CRS,
    CDL_CRS,
    CDL_RASTER_NPY_PATH,
    CDL_VECTOR_POINTS_FULL_FILE_PATH,
    COMMODITY_CODE_MAP_FILE_PATH,
    CROP_REPORT_FILE_PATH,
    FINAL_CRS,
    MAX_YIELD_DATA_PATH,
    N_JOBS,
    NUMERICAL_FEATURES,
    ORIGINAL_CDL_CRS,
    ORIGINAL_POINT_RESOLUTION,
    POINT_ACRES_CONVERSION,
    REDUCED_SOIL_FEATURES_FILE_PATH,
    SAMPLED_SOIL_FILE_PATH,
    SOIL_CRS,
    SOIL_FILE_PATHS_DICT,
    DOWN_SAMPLED_POINTS_FILE_PATH,
    DOWN_SAMPLE_GRID_SIZE,
    INTERMEDIATE_NDVI_FILE_PATH,
    MERGED_CROP_AND_NDVI_DATA_FILE_PATH,
    NDVI_TILES_FOLDER_PATH,
    CA_SHAPEFILE_FILE_PATH,
    CDL_CROP_LABEL_MAP_FILE_PATH,
    CDL_RASTER_PATH,
    CROP_REPORTS_FOLDER_PATH,
    DOWN_SAMPLING_CHUNK_SIZE,
    MERGED_CROP_AND_NDVI_DATA_FILE_PATH,
    MERGED_CROP_AND_YIELD_DATA_FILE_PATH,
    DOWN_SAMPLED_POINTS_FILE_PATH,
    DOWN_SAMPLED_POINTS_FILE_NAME,
    SOIL_RASTER_PATHS,
    SUITABILITY_SCORE_ALL_POINTS_FILE_PATH,
    SUITABILITY_SCORE_PER_POINT_FILE_NAME,
    SUITABILITY_SCORE_PER_POINT_FILE_PATH,
    VECTOR_POINTS_NPY_PATH,
)
from crop_suitability_predictor.modules.data.common_data_utils import (
    load_points,
    check_file_exists,
)
from crop_suitability_predictor.modules.data.data_transformation_utils import (
    assing_suitability_score,
    compute_and_merge_mean_ndvi,
    compute_point_estimates_and_suitability_scores,
    compute_weighted_suitability_scores,
    convert_cdl_raster_to_vector_points,
    get_crop_report_cdl_codes,
    get_crop_report_counties,
    initialize_output_file,
    merge_crop_and_max_prod_data,
    down_sample_crop_points,
    perform_pca_on_soil_features,
    process_all_scenes,
    sample_soil_data,
)


def compute_suitability_scores():
    # merge soil
    compute_point_estimates_and_suitability_scores()


def compute_and_merge_ndvi():

    if not check_file_exists(INTERMEDIATE_NDVI_FILE_PATH):

        initialize_output_file(output_path=INTERMEDIATE_NDVI_FILE_PATH)

        process_all_scenes(
            band_folder=NDVI_TILES_FOLDER_PATH,
            points_file_path=DOWN_SAMPLED_POINTS_FILE_PATH,
            points_crs=CDL_CRS,
            output_file=INTERMEDIATE_NDVI_FILE_PATH,
        )

    if not check_file_exists(MERGED_CROP_AND_NDVI_DATA_FILE_PATH):
        merged_ndv_gdf = compute_and_merge_mean_ndvi(
            points_path=DOWN_SAMPLED_POINTS_FILE_PATH,
            points_crs=CDL_CRS,
            ndvi_values_path=INTERMEDIATE_NDVI_FILE_PATH,
        )

        merged_ndv_gdf.to_csv(MERGED_CROP_AND_NDVI_DATA_FILE_PATH, index=False)


def pre_process_crop_points():

    if not check_file_exists(MERGED_CROP_AND_YIELD_DATA_FILE_PATH):
        merged_crop_and_yield_data = merge_crop_and_max_prod_data(
            crop_report_data_path=CROP_REPORT_FILE_PATH,
            max_yield_data_path=MAX_YIELD_DATA_PATH,
            cdl_commodity_code_map_path=COMMODITY_CODE_MAP_FILE_PATH,
        )
        merged_crop_and_yield_data.to_csv(MERGED_CROP_AND_YIELD_DATA_FILE_PATH)

    if not check_file_exists(DOWN_SAMPLED_POINTS_FILE_PATH):
        crop_codes = get_crop_report_cdl_codes(
            crop_reports_dir=CROP_REPORTS_FOLDER_PATH
        )
        counties = get_crop_report_counties(crop_reports_dir=CROP_REPORTS_FOLDER_PATH)
        convert_cdl_raster_to_vector_points(
            down_sample_points=True,
            original_raster_resolution=ORIGINAL_POINT_RESOLUTION,
            sampling_grid_resolution=DOWN_SAMPLE_GRID_SIZE,
            n_jobs=N_JOBS,
            cdl_raster_path=CDL_RASTER_PATH,
            cdl_npy_output_path=CDL_RASTER_NPY_PATH,
            output_cdl_crs=CDL_CRS,
            county_boundary_shape_file=CA_SHAPEFILE_FILE_PATH,
            county_shapefile_crs=CA_SHAPEFILE_CRS,
            output_file_path=DOWN_SAMPLED_POINTS_FILE_PATH,
            filter_crop_codes=True,
            filter_counties=True,
            included_crop_codes=crop_codes,
            included_counties=counties,
        )


def pre_process_soil_data():

    if not check_file_exists(SAMPLED_SOIL_FILE_PATH):
        soil_df = sample_soil_data(
            vector_points_npy_path=VECTOR_POINTS_NPY_PATH,
            soil_raster_paths=SOIL_RASTER_PATHS,
            soil_raster_features=ALL_FEATURES,
            output_crs=FINAL_CRS,
            crs_points=ORIGINAL_CDL_CRS,
            crs_soil=SOIL_CRS,
        )
        soil_df.to_csv(SAMPLED_SOIL_FILE_PATH, index=False)

    if not check_file_exists(REDUCED_SOIL_FEATURES_FILE_PATH):
        reduced_soil_gdf = perform_pca_on_soil_features(
            soil_gdf_path=SAMPLED_SOIL_FILE_PATH,
            numerical_cols=NUMERICAL_FEATURES,
        )
        reduced_soil_gdf.to_csv(REDUCED_SOIL_FEATURES_FILE_PATH, index=False)


def compute_suitability():

    if not check_file_exists(SUITABILITY_SCORE_PER_POINT_FILE_PATH):
        suitability_points_df = assing_suitability_score(
            points_path=MERGED_CROP_AND_NDVI_DATA_FILE_PATH,
            points_crs=CDL_CRS,
            country_prod_data_path=MERGED_CROP_AND_YIELD_DATA_FILE_PATH,
            point_acre_conversion=POINT_ACRES_CONVERSION,
            coverage_ratio=0.065,
        )
        suitability_points_df.to_csv(SUITABILITY_SCORE_PER_POINT_FILE_PATH, index=False)

    if not check_file_exists(SUITABILITY_SCORE_ALL_POINTS_FILE_PATH):
        suitability_all_points_df = compute_weighted_suitability_scores(
            soil_features_path=REDUCED_SOIL_FEATURES_FILE_PATH,
            suitability_scores_path=SUITABILITY_SCORE_PER_POINT_FILE_PATH,
            cdl_map_path=CDL_CROP_LABEL_MAP_FILE_PATH,
        )
        suitability_all_points_df.to_csv(
            SUITABILITY_SCORE_ALL_POINTS_FILE_PATH, index=False
        )


def main() -> None:
    pre_process_crop_points()
    pre_process_soil_data()
    compute_and_merge_ndvi()
    compute_suitability()
    # pre_process_soil_data(
    #     raw_vector_points_npy=raw_path,
    #     soil_raster_paths=SOIL_RASTER_PATHS,
    #     soil_raster_features=ALL_FEATURES,
    #     output_crs=CDL_CRS,
    # )
    # soil_gdf_path = "/Users/varunwadhwa/Desktop/crop-suitability-predictor/data/processed/intermediate_processed_files/soil/soil_features.csv"
    # perform_pca_on_soil_features(
    #     soil_gdf_path=soil_gdf_path, crs=3857, numerical_cols=NUMERICAL_FEATURES
    # )


if __name__ == "__main__":
    main()
