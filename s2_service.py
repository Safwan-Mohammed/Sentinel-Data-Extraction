import os
import json
import logging
import ee
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("app.log", mode="w")]
)
logger = logging.getLogger(__name__)

july_s2 = august_s2 = september_s2 = october_s2 = november_s2 = december_s2 = None

def create_monthwise_s2_collection(ee, year):
    global july_s2, august_s2, september_s2, october_s2, november_s2, december_s2
    
    GEOMETRY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Tumkur.geojson")
    with open(GEOMETRY_PATH, "r") as f:
        geojson = json.load(f)
    region = ee.Geometry(geojson["geometry"])
    
    def add_cloud_bands(img):
        try:
            cld_prb = ee.Image(img.get('s2cloudless')).select('probability')
            is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename('clouds')
            return img.addBands(ee.Image([cld_prb, is_cloud]))
        except Exception as e:
            logger.error("Error in add_cloud_bands: %s", e)
            raise

    def add_shadow_bands(img):
        try:
            not_water = img.select('SCL').neq(6)
            SR_BAND_SCALE = 1e4
            dark_pixels = img.select('B8').lt(NIR_DRK_THRESH * SR_BAND_SCALE).multiply(not_water).rename('dark_pixels')
            shadow_azimuth = ee.Number(90).subtract(ee.Number(img.get('MEAN_SOLAR_AZIMUTH_ANGLE')))
            cld_proj = (img.select('clouds').directionalDistanceTransform(shadow_azimuth, CLD_PRJ_DIST * 10)
                .reproject(crs=img.select(0).projection(), scale=100)
                .select('distance')
                .mask()
                .rename('cloud_transform'))
            shadows = cld_proj.multiply(dark_pixels).rename('shadows')
            return img.addBands(ee.Image([dark_pixels, cld_proj, shadows]))
        except Exception as e:
            logger.error("Error in add_shadow_bands: %s", e)
            raise

    def add_cld_shdw_mask(img):
        try:
            img_cloud = add_cloud_bands(img)
            img_cloud_shadow = add_shadow_bands(img_cloud)
            is_cld_shdw = img_cloud_shadow.select('clouds').add(img_cloud_shadow.select('shadows')).gt(0)
            is_cld_shdw = (is_cld_shdw.focalMin(2).focalMax(BUFFER * 2 / 20)
                .reproject(crs=img.select(0).projection(), scale=20)
                .rename('cloudmask'))
            return img_cloud_shadow.addBands(is_cld_shdw)
        except Exception as e:
            logger.error("Error in add_cld_shdw_mask: %s", e)
            raise

    def apply_cld_shdw_mask(img):
        try:
            not_cld_shdw = img.select('cloudmask').Not()
            return img.select(['B2', 'B3', 'B4', 'B5', 'B8', 'B11', 'B12']).updateMask(not_cld_shdw)
        except Exception as e:
            logger.error("Error in apply_cld_shdw_mask: %s", e)
            raise

    def get_s2_sr_cld_col(aoi, start_date, end_date, CLOUD_FILTER):
        try:
            s2_sr_col = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
                .filterBounds(aoi)
                .filterDate(start_date, end_date)
                .select(['B2', 'B3', 'B4', 'B5', 'B8', 'B11', 'B12', 'SCL'])
                .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', CLOUD_FILTER)))
            s2_cloudless_col = (ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
                .filterBounds(aoi)
                .filterDate(start_date, end_date))
            return ee.ImageCollection(ee.Join.saveFirst('s2cloudless').apply(**{
                'primary': s2_sr_col,
                'secondary': s2_cloudless_col,
                'condition': ee.Filter.equals(**{
                    'leftField': 'system:index',
                    'rightField': 'system:index'
                })
            }))
        except Exception as e:
            logger.error("Error in get_s2_sr_cld_col: %s", e)
            raise

    CLOUD_FILTER, CLD_PRB_THRESH, NIR_DRK_THRESH, CLD_PRJ_DIST, BUFFER = 70, 70, 0.15, 1, 40
    months = ["07", "08", "09", "10", "11", "12"]
    month_vars = ["july_s2", "august_s2", "september_s2", "october_s2", "november_s2", "december_s2"]
    
    for i, month in enumerate(months):
        start_date = f"{year}-{month}-01"
        end_date = f"{year}-{month}-30" if month in ["09", "11"] else f"{year}-{month}-31"
        
        s2_collection = get_s2_sr_cld_col(region, start_date, end_date, CLOUD_FILTER)
        s2_processed = s2_collection.map(add_cld_shdw_mask).map(apply_cld_shdw_mask)
        median_image = s2_processed.median().clip(region)
        globals()[month_vars[i]] = compute_indices(median_image)  # Compute indices server-side
        logger.info("Created Sentinel-2 median with indices for month %s", month)

def compute_indices(image):
    nir = image.select('B8')
    red = image.select('B4')
    blue = image.select('B2')
    green = image.select('B3')
    rededge = image.select('B5')
    swir1 = image.select('B11')
    
    ndvi = nir.subtract(red).divide(nir.add(red)).rename('NDVI')
    evi = nir.subtract(red).multiply(2.5).divide(nir.add(red.multiply(6)).subtract(blue.multiply(7.5)).add(1)).rename('EVI')
    gndvi = nir.subtract(green).divide(nir.add(green)).rename('GNDVI')
    savi = nir.subtract(red).multiply(1.5).divide(nir.add(red).add(0.5)).rename('SAVI')
    ndwi = green.subtract(nir).divide(green.add(nir)).rename('NDWI')
    ndmi = nir.subtract(swir1).divide(nir.add(swir1)).rename('NDMI')
    rendvi = rededge.subtract(red).divide(rededge.add(red)).rename('RENDVI')
    
    return image.addBands([ndvi, evi, gndvi, savi, ndwi, ndmi, rendvi])

def export_sentinel_2_data(ee, fc: ee.FeatureCollection, month: str, batch_idx: int, output_dir: str):
    global july_s2, august_s2, september_s2, october_s2, november_s2, december_s2
    month_vars = {
        "July": july_s2, "August": august_s2, "September": september_s2,
        "October": october_s2, "November": november_s2, "December": december_s2
    }
    
    s2_img = month_vars.get(month)
    if s2_img is None:
        logger.error("Sentinel-2 image for %s not initialized", month)
        return

    try:
        # Sample all points at once
        sampled_fc = s2_img.sampleRegions(
            collection=fc,
            properties=["id"],
            scale=10,
            projection=s2_img.projection(),
            geometries=True
        )
        sampled_size = sampled_fc.size().getInfo()
        sampled_data = sampled_fc.getInfo()
        features = sampled_data["features"]
        
        # Convert to DataFrame
        data_list = []
        for feature in features:
            props = feature["properties"]
            geom = feature["geometry"]["coordinates"]
            data_list.append({
                "id": props["id"],
                "Longitude": geom[0],
                "Latitude": geom[1],
                "NDVI": props.get("NDVI", 0),
                "EVI": props.get("EVI", 0),
                "GNDVI": props.get("GNDVI", 0),
                "SAVI": props.get("SAVI", 0),
                "NDWI": props.get("NDWI", 0),
                "NDMI": props.get("NDMI", 0),
                "RENDVI": props.get("RENDVI", 0)
            })
        
        df = pd.DataFrame(data_list)
        output_file = os.path.join(output_dir, f"s2_{month.lower()}_2019_batch{batch_idx}.csv")
        df.to_csv(output_file, index=False)
        logger.info("Sampled features for %s, batch %d: %d\nSaved Sentinel-2 data for %s, batch %d to %s", month, batch_idx, sampled_size, month, batch_idx, output_file)        
    except ee.EEException as e:
        logger.error("Error processing Sentinel-2 data for %s, batch %d: %s", month, batch_idx, e)

# service_account = 'gee-service-account@wise-scene-427306-q3.iam.gserviceaccount.com'
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# KEY_PATH = os.path.join(BASE_DIR, "gee-key.json")
# INPUT_CSV_PATH = os.path.join(BASE_DIR, "Input", "ragi_2021.csv")
# OUTPUT_DIR = os.path.join(BASE_DIR, "Output")
# os.makedirs(OUTPUT_DIR, exist_ok=True)  # Create Output folder if it doesn’t exist

# # Initialize GEE
# credentials = ee.ServiceAccountCredentials(service_account, KEY_PATH)
# try:
#     ee.Initialize(credentials)
#     logger.info("GEE successfully initialized")
# except Exception as e:
#     logger.error("Error initializing GEE: %s", e)
#     exit(1)

# create_monthwise_s2_collection(ee, 2021)

# features = [ee.Feature(ee.Geometry.Point(76.9430421,12.8601999), {"id": 0})]
# fc = ee.FeatureCollection(features)
# export_sentinel_2_data(ee, fc, "August", 0, OUTPUT_DIR)

# import os
# import json
# import logging
# import ee

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[logging.StreamHandler(), logging.FileHandler("app.log", mode="w")]
# )
# logger = logging.getLogger(__name__)

# july_s2 = august_s2 = september_s2 = october_s2 = november_s2 = december_s2 = None

# def create_monthwise_s2_collection(ee, year):
#     global july_s2, august_s2, september_s2, october_s2, november_s2, december_s2
    
#     GEOMETRY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Tumkur.geojson")
#     with open(GEOMETRY_PATH, "r") as f:
#         geojson = json.load(f)
    
#     region = ee.Geometry(geojson["geometry"])
    
#     def add_cloud_bands(img):
#         try:
#             cld_prb = ee.Image(img.get('s2cloudless')).select('probability')
#             is_cloud = cld_prb.gt(CLD_PRB_THRESH).rename('clouds')
#             return img.addBands(ee.Image([cld_prb, is_cloud]))
#         except Exception as e:
#             logger.error("Error in add_cloud_bands: %s", e)
#             raise

#     def add_shadow_bands(img):
#         try:
#             not_water = img.select('SCL').neq(6)
#             SR_BAND_SCALE = 1e4
#             dark_pixels = img.select('B8').lt(NIR_DRK_THRESH * SR_BAND_SCALE).multiply(not_water).rename('dark_pixels')
#             shadow_azimuth = ee.Number(90).subtract(ee.Number(img.get('MEAN_SOLAR_AZIMUTH_ANGLE')))
#             cld_proj = (img.select('clouds').directionalDistanceTransform(shadow_azimuth, CLD_PRJ_DIST * 10)
#                 .reproject(crs=img.select(0).projection(), scale=100)
#                 .select('distance')
#                 .mask()
#                 .rename('cloud_transform'))
#             shadows = cld_proj.multiply(dark_pixels).rename('shadows')
#             return img.addBands(ee.Image([dark_pixels, cld_proj, shadows]))
#         except Exception as e:
#             logger.error("Error in add_shadow_bands: %s", e)
#             raise

#     def add_cld_shdw_mask(img):
#         try:
#             img_cloud = add_cloud_bands(img)
#             img_cloud_shadow = add_shadow_bands(img_cloud)
#             is_cld_shdw = img_cloud_shadow.select('clouds').add(img_cloud_shadow.select('shadows')).gt(0)
#             is_cld_shdw = (is_cld_shdw.focalMin(2).focalMax(BUFFER * 2 / 20)
#                 .reproject(crs=img.select(0).projection(), scale=20)
#                 .rename('cloudmask'))
#             return img_cloud_shadow.addBands(is_cld_shdw)
#         except Exception as e:
#             logger.error("Error in add_cld_shdw_mask: %s", e)
#             raise

#     def apply_cld_shdw_mask(img):
#         try:
#             not_cld_shdw = img.select('cloudmask').Not()
#             return img.select(['B2', 'B3', 'B4', 'B5', 'B8', 'B11', 'B12']).updateMask(not_cld_shdw)
#         except Exception as e:
#             logger.error("Error in apply_cld_shdw_mask: %s", e)
#             raise

#     def get_s2_sr_cld_col(aoi, start_date, end_date, CLOUD_FILTER):
#         try:
#             s2_sr_col = (ee.ImageCollection('COPERNICUS/S2_SR_HARMONIZED')
#                 .filterBounds(aoi)
#                 .filterDate(start_date, end_date)
#                 .select(['B2', 'B3', 'B4', 'B5', 'B8', 'B11', 'B12', 'SCL'])
#                 .filter(ee.Filter.lte('CLOUDY_PIXEL_PERCENTAGE', CLOUD_FILTER)))

#             s2_cloudless_col = (ee.ImageCollection('COPERNICUS/S2_CLOUD_PROBABILITY')
#                 .filterBounds(aoi)
#                 .filterDate(start_date, end_date))

#             return ee.ImageCollection(ee.Join.saveFirst('s2cloudless').apply(**{
#                 'primary': s2_sr_col,
#                 'secondary': s2_cloudless_col,
#                 'condition': ee.Filter.equals(**{
#                     'leftField': 'system:index',
#                     'rightField': 'system:index'
#                 })
#             }))
#         except Exception as e:
#             logger.error("Error in get_s2_sr_cld_col: %s", e)
#             raise

#     CLOUD_FILTER, CLD_PRB_THRESH, NIR_DRK_THRESH, CLD_PRJ_DIST, BUFFER = 70, 70, 0.15, 1, 40
    
#     months = ["07", "08", "09", "10", "11", "12"]
#     month_vars = ["july_s2", "august_s2", "september_s2", "october_s2", "november_s2", "december_s2"]
    
#     for i, month in enumerate(months):
#         start_date = f"{year}-{month}-01"
#         end_date = f"{year}-{month}-30" if month in ["09", "11"] else f"{year}-{month}-31"
        
#         s2_collection = get_s2_sr_cld_col(region, start_date, end_date, CLOUD_FILTER)
        
#         collection_size = s2_collection.size().getInfo()
        
#         s2_processed = s2_collection.map(add_cld_shdw_mask).map(apply_cld_shdw_mask)
#         median_image = s2_processed.median().clip(region)
#         count_image = s2_processed.select('B2').count().rename('count').clip(region)
#         combined_image = median_image.addBands(count_image)
#         globals()[month_vars[i]] = combined_image
        
# def compute_indices(image):
#     nir = image.select('B8')
#     red = image.select('B4')
#     blue = image.select('B2')
#     green = image.select('B3')
#     rededge = image.select('B5')
#     swir1 = image.select('B11')

#     ndvi = nir.subtract(red).divide(nir.add(red)).rename('NDVI')
#     evi = nir.subtract(red).multiply(2.5).divide(nir.add(red.multiply(6)).subtract(blue.multiply(7.5)).add(1)).rename('EVI')
#     gndvi = nir.subtract(green).divide(nir.add(green)).rename('GNDVI')
#     savi = nir.subtract(red).multiply(1.5).divide(nir.add(red).add(0.5)).rename('SAVI')
#     ndwi = green.subtract(nir).divide(green.add(nir)).rename('NDWI')
#     ndmi = nir.subtract(swir1).divide(nir.add(swir1)).rename('NDMI')
#     rendvi = rededge.subtract(red).divide(rededge.add(red)).rename('RENDVI')
    
#     return image.addBands([ndvi, evi, gndvi, savi, ndwi, ndmi, rendvi])

# def get_sentinel_2_data(ee, coordinate: tuple):
#     global july_s2, august_s2, september_s2, october_s2, november_s2, december_s2
    
#     point = ee.Geometry.Point(coordinate)
    
#     with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "Tumkur.geojson"), "r") as f:
#         geojson = json.load(f)
#     region = ee.Geometry(geojson["geometry"])
#     if not region.contains(point).getInfo():
#         logger.error(f"Coordinate {coordinate} is outside the Tumkur region")
#         return {}
    
#     months = ["July", "August", "September", "October", "November", "December"]
#     month_vars = [july_s2, august_s2, september_s2, october_s2, november_s2, december_s2]
#     results = {}

#     for i, month in enumerate(months):
#         s2_img = month_vars[i]
#         if s2_img is None:
#             logger.warning(f"Sentinel-2 data for {month} is not initialized.")
#             continue
        
#         s2_img = compute_indices(s2_img)
#         s2_values = s2_img.reduceRegion(
#             reducer=ee.Reducer.mean(),
#             geometry=point,
#             scale=10,
#             maxPixels=1e9,
#             bestEffort=True
#         ).getInfo()

#         output_file = "s2_data.json"  
#         with open(output_file, "w") as f:
#             json.dump(s2_values, f, indent=4)
        
#         if s2_values:
#             results[month] = {
#                 "NDVI": s2_values.get("NDVI", 0),
#                 "EVI": s2_values.get("EVI", 0),
#                 "GNDVI": s2_values.get("GNDVI", 0),
#                 "SAVI": s2_values.get("SAVI", 0),
#                 "NDWI": s2_values.get("NDWI", 0),
#                 "NDMI": s2_values.get("NDMI", 0),
#                 "RENDVI": s2_values.get("RENDVI", 0)
#             }
#         else:
#             logger.warning(f"No data returned for {month}")
    
#     return results