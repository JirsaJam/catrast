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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS session
session = boto3.Session(profile_name='se')

default_crs = 'EPSG:4326'

# Function to download and extract zip files from S3
def download_and_extract_from_s3(s3_bucket, s3_key, local_dir):
    """Download a zip file from S3 and extract its contents."""
    s3 = session.client('s3')
    local_zip_path = os.path.join(local_dir, os.path.basename(s3_key))

    # Debugging: Print the S3 bucket and key being used
    print(f"Attempting to download from s3://{s3_bucket}/{s3_key} to {local_zip_path}")
    
    # Download the zip file from S3
    s3.download_file(s3_bucket, s3_key, local_zip_path)
    
    # Extract the zip file
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(local_dir)
    
    print(f"File {s3_key} downloaded and extracted to {local_dir}.")
    return local_dir, os.path.basename(local_zip_path).replace('.zip', '')

# Function to download a shapefile to a temporary directory
def download_shapefile_to_temp(s3_bucket, shapefile_key, temp_dir):
    """Download a shapefile and its associated files from S3 to a temporary directory."""
    s3 = session.client('s3')
    
    shapefile_base = os.path.splitext(os.path.basename(shapefile_key))[0]
    shapefile_extensions = ['.shp', '.shx', '.dbf', '.prj']
    
    for ext in shapefile_extensions:
        key = shapefile_key.replace('.shp', ext)
        local_file_path = os.path.join(temp_dir, os.path.basename(key))
        print(f"Downloading {key} to {local_file_path}")
        s3.download_file(s3_bucket, key, local_file_path)
    
    return os.path.join(temp_dir, shapefile_base + '.shp')

# Function to process the shapefile from a local path
def process_shapefile_from_temp(local_shapefile_path):
    """Read and process the shapefile from the local temp directory."""
    usa = gpd.read_file(local_shapefile_path)
    usa = usa.to_crs(epsg=3395)
    usa = usa.buffer(2000)  # Buffer of 2000 meters (2 km)
    usa = usa.to_crs(epsg=4326)
    return unary_union(usa)

# Function to convert polygon to H3 indices
def polygon_to_h3(polygon, resolution):
    """Converts a shapely Polygon or MultiPolygon to a list of H3 indices at the specified resolution."""
    all_h3_indices = set()
    if isinstance(polygon, MultiPolygon):
        for poly in polygon.geoms:
            h3_indices = polygon_to_h3_single(poly, resolution)
            all_h3_indices.update(h3_indices)
    else:
        h3_indices = polygon_to_h3_single(polygon, resolution)
        all_h3_indices.update(h3_indices)
    return list(all_h3_indices)

def polygon_to_h3_single(polygon, resolution):
    """Helper function to convert a single Polygon to H3 indices."""
    exterior_coords = list(polygon.exterior.coords)
    geojson_polygon = {
        'type': 'Polygon',
        'coordinates': [[[lng, lat] for lat, lng in exterior_coords]]
    }
    return h3.polyfill(geojson_polygon, resolution)

# Function to check and reproject raster
def check_georeferencing(raster_path):
    """Checks the georeferencing information of a raster file."""
    with rasterio.open(raster_path) as src:
        crs = src.crs
        transform = src.transform
        bounds = src.bounds
        if crs is None or transform == rasterio.Affine.identity():
            print(f"Raster {raster_path} has no valid georeferencing.")
            return None
        print(f"Raster {raster_path} has CRS: {crs} and bounds: {bounds}")
        return crs

def reproject_raster(src_path, dst_crs='EPSG:4326', output_path=None):
    """Reprojects the raster to the specified CRS."""
    with rasterio.open(src_path) as src:
        transform, width, height = calculate_default_transform(src.crs, dst_crs, src.width, src.height, *src.bounds)
        kwargs = src.meta.copy()
        kwargs.update({'crs': dst_crs, 'transform': transform, 'width': width, 'height': height})
        if not output_path:
            output_path = '/tmp/reprojected.tif'
        with rasterio.open(output_path, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(rasterio.band(src, i), rasterio.band(dst, i), src_transform=src.transform, src_crs=src.crs,
                          dst_transform=transform, dst_crs=dst_crs, resampling=Resampling.nearest)
    return output_path

# Function to compute zonal statistics
def compute_zonal_statistics(zone_gdf, raster_path):
    """Compute zonal statistics for the given zones and raster data."""
    dataset = rasterio.open(raster_path)
    arr = dataset.read(1)
    affine = dataset.transform
    zone_gdf = zone_gdf.to_crs(dataset.crs)
    stats = zonal_stats(zone_gdf, arr, affine=affine, categorical=True, all_touched=True, geojson_out=True, band=1, nodata_value=-9999)
    return stats

# Function to merge zonal statistics with CSV data
def merge_stats_with_csv(stats, csv_path):
    """Merges the zonal statistics with the CSV data."""
    data = []
    for feature in stats:
        h3_index = feature['properties'].get('h3_index')
        categories = {k: v for k, v in feature['properties'].items() if k != 'h3_index'}
        total = sum(categories.values())
        for category, value in categories.items():
            percentage = (value / total) if total > 0 else 0
            data.append({'h3_index': h3_index, 'category': category, 'value': percentage})
    df = pd.DataFrame(data)

    # Load CSV and merge
    csv_data = pd.read_csv(csv_path)
    csv_data = csv_data.iloc[:,:2]
    merged_df = df.merge(csv_data, left_on='category', right_on=csv_data.columns[0], how='left')
    return merged_df

# Function to upload file to S3
def upload_to_s3(local_file_path, s3_bucket, s3_output_key):
    """Uploads a file to the specified S3 bucket."""
    s3 = session.client('s3')
    s3.upload_file(local_file_path, s3_bucket, s3_output_key)
    print(f"File uploaded to s3://{s3_bucket}/{s3_output_key}")

# Main function to download the shapefile to a temporary directory and process it
def process_shapefile(s3_bucket, shapefile_key):
    """Download the shapefile from S3 into a temporary directory and process it."""
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Using temporary directory: {temp_dir}")
        
        # Download the shapefile and its associated files
        local_shapefile_path = download_shapefile_to_temp(s3_bucket, shapefile_key, temp_dir)
        
        # Process the shapefile
        usa_boundary = process_shapefile_from_temp(local_shapefile_path)
        
        return usa_boundary

# Function to recursively search for files
def find_file_by_extension(root_dir, extension):
    """Recursively search for a file with a specific extension."""
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(extension):
                return os.path.join(dirpath, filename)
    raise FileNotFoundError(f"No file with extension {extension} found in {root_dir}")

# Main function to run the full process

def process_raster_and_csv_from_s3(s3_bucket, zip_key, shapefile_s3_key, s3_output_bucket, s3_output_prefix):
    """Download zip from S3, extract, process raster, and merge with CSV."""
    # Create a unique temporary directory
    temp_dir = tempfile.mkdtemp(prefix=f'raster_process_{uuid.uuid4()}_')
    try:
        logger.info(f"Using temporary directory: {temp_dir}")

        # Step 1: Download and extract zip file from S3
        try:
            extracted_dir, zip_name = download_and_extract_from_s3(s3_bucket, zip_key, temp_dir)
            logger.info(f"Files in {extracted_dir}: {os.listdir(extracted_dir)}")
        except Exception as e:
            logger.error(f"Error in downloading and extracting from S3: {str(e)}")
            raise

        # Step 2: Download the shapefile from S3 into a temporary directory and process it
        try:
            usa_boundary = process_shapefile(s3_bucket, shapefile_s3_key)
        except Exception as e:
            logger.error(f"Error in processing shapefile: {str(e)}")
            raise

        # Step 3: Convert USA boundary to H3 indices
        try:
            h3_indices = polygon_to_h3(usa_boundary, 7)
            h3_gdf = gpd.GeoDataFrame(h3_indices, columns=['h3_index'])
            h3_gdf['geometry'] = h3_gdf['h3_index'].apply(lambda x: Polygon(h3.h3_to_geo_boundary(x, geo_json=True)))
            h3_gdf = gpd.GeoDataFrame(h3_gdf, crs="EPSG:4326", geometry='geometry')
        except Exception as e:
            logger.error(f"Error in converting to H3 indices: {str(e)}")
            raise

        # Step 4: Find raster and CSV in the extracted folder
        try:
            raster_path = find_file_by_extension(extracted_dir, '.tif')
            csv_path = find_file_by_extension(extracted_dir, '.csv')
            logger.info(f"Found raster file: {raster_path}")
            logger.info(f"Found CSV file: {csv_path}")
        except FileNotFoundError as e:
            logger.error(str(e))
            raise

        # Step 5: Check and reproject the raster
        try:
            crs = check_georeferencing(raster_path)
            if crs != 'EPSG:4326':
                raster_path = reproject_raster(raster_path)
            logger.info(f"Raster CRS: {crs}")
        except Exception as e:
            logger.error(f"Error in checking or reprojecting raster: {str(e)}")
            raise

        # Step 6: Compute zonal statistics
        try:
            stats = compute_zonal_statistics(h3_gdf, raster_path)
        except Exception as e:
            logger.error(f"Error in computing zonal statistics: {str(e)}")
            raise

        # Step 7: Merge stats with CSV data
        try:
            final_df = merge_stats_with_csv(stats, csv_path)
        except Exception as e:
            logger.error(f"Error in merging stats with CSV: {str(e)}")
            raise

        # Step 8: Save final output
        output_file = os.path.join(temp_dir, f'{zip_name}.csv')
        final_df.to_csv(output_file, index=False)
        logger.info(f"Final merged data saved at {output_file}")

        # Step 9: Upload final output to S3
        try:
            s3_output_key = os.path.join(s3_output_prefix, f'{zip_name}.csv')
            upload_to_s3(output_file, s3_output_bucket, s3_output_key)
        except Exception as e:
            logger.error(f"Error in uploading to S3: {str(e)}")
            raise

        return output_file

    except Exception as e:
        logger.error(f"An error occurred during processing: {str(e)}")
        raise

    finally:
        # Clean up the temporary directory
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.info(f"Temporary directory {temp_dir} has been removed.")

# Define your S3 bucket and paths
s3_input_bucket = 'gsr-landfire'
s3_output_bucket = 'gsr-landfire'
shapefile_s3_key = '2023/lf/conus_poly/cb_2023_us_nation_5m (1)/cb_2023_us_nation_5m.shp'
zip_key = '2023/lf/categorical /input/LF2023_CBD_240_CONUS.zip'
s3_output_folder = '2023/lf/categorical /output/'

# Run the main process
try:
    output_file = process_raster_and_csv_from_s3(s3_input_bucket, zip_key, shapefile_s3_key, s3_output_bucket, s3_output_folder)
    logger.info(f"Process completed. Output file: {output_file}")
except Exception as e:
    logger.error(f"Process failed: {str(e)}")
