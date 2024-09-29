import os
import boto3
import zipfile
import tempfile
import rasterio
import h3
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union
from rasterstats import zonal_stats
import pandas as pd
from rasterio.warp import calculate_default_transform, reproject, Resampling
import shutil
import uuid
import logging
from test_s3 import process_raster_and_csv_from_s3
from get_data import file_names_without_extension

bucket_name = 'gsr-landfire'
prefix = '2023/lf/categorical /input/'
profile_name = 'se'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

file_names = file_names_without_extension(bucket_name, prefix, profile_name)

s3_input_bucket = 'gsr-landfire'
s3_output_bucket = 'gsr-landfire'
shapefile_s3_key = '2023/lf/conus_poly/cb_2023_us_nation_5m (1)/cb_2023_us_nation_5m.shp'
s3_output_folder = '2023/lf/categorical /output/'

for x in file_names:
    try:
        zip_key = f'2023/lf/categorical /input/{x}.zip'
        output_file = process_raster_and_csv_from_s3(s3_input_bucket, zip_key, shapefile_s3_key, s3_output_bucket, s3_output_folder)
        logger.info(f"Process completed. Output file: {x}")
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")