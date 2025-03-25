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

# Global variables for monthly Sentinel-1 images
july_s1 = august_s1 = september_s1 = october_s1 = november_s1 = december_s1 = None

def create_monthwise_s1_collection(ee, year):
    global july_s1, august_s1, september_s1, october_s1, november_s1, december_s1

    GEOMETRY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Tumkur.geojson")
    try:
        with open(GEOMETRY_PATH, "r") as f:
            geojson = json.load(f)
        region = ee.Geometry(geojson["geometry"])
        logger.info("Loaded Tumkur region geometry")
    except Exception as e:
        logger.error("Failed to load GeoJSON: %s", e)
        return

    months = ["07", "08", "09", "10", "11", "12"]
    month_vars = ["july_s1", "august_s1", "september_s1", "october_s1", "november_s1", "december_s1"]

    for i, month in enumerate(months):
        start_date = f"{year}-{month}-01"
        end_date = f"{year}-{month}-30" if month in ["09", "11"] else f"{year}-{month}-31"

        try:
            s1_collection = (
                ee.ImageCollection("COPERNICUS/S1_GRD")
                .filterBounds(region)
                .filterDate(ee.Date(start_date), ee.Date(end_date))
                .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
                .filter(ee.Filter.eq("instrumentMode", "IW"))
                .filter(ee.Filter.eq("resolution_meters", 10))
                .filter(ee.Filter.eq("orbitProperties_pass", "DESCENDING"))
                .select(["VV", "VH"])
            )
            median_image = s1_collection.median().clip(region)
            # Compute VH/VV server-side
            vh_vv = median_image.select("VH").divide(median_image.select("VV")).rename("VH_VV")
            globals()[month_vars[i]] = median_image.addBands(vh_vv)
            logger.info("Created Sentinel-1 median for month %s", month)
        except Exception as e:
            logger.error("Error creating Sentinel-1 data for month %s: %s", month, e)

def export_sentinel_1_data(ee, fc: ee.FeatureCollection, month: str, batch_idx: int, output_dir: str):
    global july_s1, august_s1, september_s1, october_s1, november_s1, december_s1
    month_vars = {
        "July": july_s1, "August": august_s1, "September": september_s1,
        "October": october_s1, "November": november_s1, "December": december_s1
    }
    
    s1_img = month_vars.get(month)
    if s1_img is None:
        logger.error("Sentinel-1 image for %s not initialized", month)
        return

    try:
        # Sample all points at once
        sampled_fc = s1_img.sampleRegions(
            collection=fc,
            properties=["id"],
            scale=10,
            projection=s1_img.projection(),
            geometries=True
        )
        
        # Fetch data client-side
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
                "VV": props.get("VV", 0),
                "VH": props.get("VH", 0),
                "VH_VV": props.get("VH_VV", 0)
            })
        
        df = pd.DataFrame(data_list)
        output_file = os.path.join(output_dir, f"s1_{month.lower()}_2019_batch{batch_idx}.csv")
        df.to_csv(output_file, index=False)
        logger.info("Saved Sentinel-1 data for %s, batch %d to %s", month, batch_idx, output_file)
        
    except ee.EEException as e:
        logger.error("Error processing Sentinel-1 data for %s, batch %d: %s", month, batch_idx, e)

# import os
# import json
# import logging
# import ee

# # Configure logging
# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", handlers=[ logging.StreamHandler(), logging.FileHandler("app.log", mode="w")])
# logger = logging.getLogger(__name__)

# # Global variables to store Sentinel-1 images for each month
# july_s1 = None
# august_s1 = None
# september_s1 = None
# october_s1 = None
# november_s1 = None
# december_s1 = None

# def create_monthwise_s1_collection(ee, year):
#     """
#     Fetches Sentinel-1 image collections for each month and stores the median image globally.
#     """
#     global july_s1, august_s1, september_s1, october_s1, november_s1, december_s1

#     # Load GeoJSON region
#     GEOMETRY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Tumkur.geojson")
#     try:
#         with open(GEOMETRY_PATH, "r") as f:
#             geojson = json.load(f)
#     except Exception as e:
#         logger.error("Failed to load GeoJSON file: %s", e)
#         return
#     region = ee.Geometry(geojson["geometry"])

#     months = ["07", "08", "09", "10", "11", "12"]
#     month_vars = ["july_s1", "august_s1", "september_s1", "october_s1", "november_s1", "december_s1"]

#     for i, month in enumerate(months):
#         start_date = f"{year}-{month}-01"
#         end_date = f"{year}-{month}-30" if month in ["09", "11"] else f"{year}-{month}-31"

#         try:
#             s1_collection = (
#                 ee.ImageCollection("COPERNICUS/S1_GRD")
#                 .filterBounds(region)
#                 .filterDate(ee.Date(start_date), ee.Date(end_date))
#                 .filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
#                 .filter(ee.Filter.eq("instrumentMode", "IW"))
#                 .filter(ee.Filter.eq("resolution_meters", 10))
#                 .filter(ee.Filter.eq("orbitProperties_pass", "DESCENDING"))
#                 .select(["VV", "VH"])  # Select radar bands
#             )

#             median_image = s1_collection.median().clip(region)
#             globals()[month_vars[i]] = median_image

#         except Exception as e:
#             logger.error("Error processing Sentinel-1 data for month %s: %s", month, e)

# def get_sentinel_1_data(ee, coordinate: tuple):

#     global july_s1, august_s1, september_s1, october_s1, november_s1, december_s1
    
#     point = ee.Geometry.Point(coordinate)
    
#     months = ["July", "August", "September", "October", "November", "December"]
#     month_vars = [july_s1, august_s1, september_s1, october_s1, november_s1, december_s1]
    
#     results = {}

#     for i, month in enumerate(months):
#         s1_img = month_vars[i]
#         if s1_img is None:
#             logger.warning("No Sentinel-1 data available for month: %s", month)
#             continue

#         try:
#             s1_values = s1_img.sampleRegions(
#                 collection=ee.FeatureCollection([ee.Feature(point)]),
#                 scale=10,
#                 projection=s1_img.projection()
#             ).first()

#             if s1_values:
#                 s1_dict = s1_values.toDictionary()
#                 vv = s1_dict.get("VV")
#                 vh = s1_dict.get("VH")

#                 vv_value = vv.getInfo() if vv else 0
#                 vh_value = vh.getInfo() if vh else 0
#                 vh_vv = vh_value / vv_value if vv_value != 0 else 0

#                 results[month] = {
#                     "VV": vv_value,
#                     "VH": vh_value,
#                     "VH/VV": vh_vv
#                 }
#             else:
#                 logger.warning("No valid data found for month: %s", month)


#         except Exception as e:
#             logger.error("Error retrieving Sentinel-1 data for month %s: %s", month, e)

#     return results