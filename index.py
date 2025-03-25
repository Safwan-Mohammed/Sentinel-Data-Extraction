import pandas as pd
import ee
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from s1_service import create_monthwise_s1_collection, export_sentinel_1_data
from s2_service import create_monthwise_s2_collection, export_sentinel_2_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("app.log", mode="w")]
)
logger = logging.getLogger(__name__)

# Paths and credentials
service_account = 'gee-service-account@wise-scene-427306-q3.iam.gserviceaccount.com'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KEY_PATH = os.path.join(BASE_DIR, "gee-key.json")
INPUT_CSV_PATH = os.path.join(BASE_DIR, "Input", "2019_non_ragi_downsampled_cleaned.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "Output")
os.makedirs(OUTPUT_DIR, exist_ok=True)  # Create Output folder if it doesn’t exist

# Initialize GEE
credentials = ee.ServiceAccountCredentials(service_account, KEY_PATH)
try:
    ee.Initialize(credentials)
    logger.info("GEE successfully initialized")
except Exception as e:
    logger.error("Error initializing GEE: %s", e)
    exit(1)

# Read input CSV
df_input = pd.read_csv(INPUT_CSV_PATH)
logger.info("Loaded %d coordinates from %s", len(df_input), INPUT_CSV_PATH)

# Precompute monthly collections
logger.info("Creating Sentinel-1 and Sentinel-2 collections for 2019")
create_monthwise_s1_collection(ee, 2019)
create_monthwise_s2_collection(ee, 2019)

# Batch processing parameters
batch_size = 4000  # ~1.2–1.6 MB per batch, under 10 MB limit
num_batches = (len(df_input) + batch_size - 1) // batch_size
logger.info("Processing %d coordinates in %d batches of %d", len(df_input), num_batches, batch_size)

months = ["July", "August", "September", "October", "November", "December"]

# Process in batches
def process_batch(batch_idx, batch_df, months, ee, output_dir):
    # Create FeatureCollection for this batch
    features = [
        ee.Feature(ee.Geometry.Point([row["Longitude"], row["Latitude"]]), {"id": index})
        for index, row in batch_df.iterrows()
    ]
    fc = ee.FeatureCollection(features)
    logger.info("Created batch %d/%d with %d points", batch_idx + 1, num_batches, len(batch_df))

    # Export data for this batch to local Output folder
    for month in months:
        logger.info("Processing Sentinel-1 data for %s, batch %d", month, batch_idx + 1)
        export_sentinel_1_data(ee, fc, month, batch_idx, output_dir)
        logger.info("Processing Sentinel-2 data for %s, batch %d", month, batch_idx + 1)
        export_sentinel_2_data(ee, fc, month, batch_idx, output_dir)

# Process batches in parallel
with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust max_workers as needed
    futures = []
    for batch_idx in range(num_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(df_input))
        batch_df = df_input.iloc[start_idx:end_idx]
        futures.append(executor.submit(process_batch, batch_idx, batch_df, months, ee, OUTPUT_DIR))

    # Handle results and catch exceptions
    for future in as_completed(futures):
        try:
            future.result()  # This will raise any exceptions that occurred in the thread
        except Exception as e:
            logger.error("Error in batch processing: %s", e)

logger.info("Processing complete. CSVs saved in %s", OUTPUT_DIR)

# Post-processing (manual step after downloading)
# Example: Combine CSVs locally
"""
combined_dfs = []
for month in months:
    s1_df = pd.read_csv(f"s1_{month.lower()}_2021.csv")
    s2_df = pd.read_csv(f"s2_{month.lower()}_2021.csv")
    merged_df = pd.merge(s1_df, s2_df, on=["id", "Longitude", "Latitude"])
    merged_df["Month"] = month
    combined_dfs.append(merged_df)
df_output = pd.concat(combined_dfs, ignore_index=True)
df_output.to_csv(OUTPUT_CSV_PATH, index=False)
logger.info("Combined output saved to %s with %d rows", OUTPUT_CSV_PATH, len(df_output))
"""


# import pandas as pd
# import ee
# import os
# import logging

# from s1_service import create_monthwise_s1_collection, get_sentinel_1_data
# from s2_service import create_monthwise_s2_collection, get_sentinel_2_data

# service_account = 'gee-service-account@wise-scene-427306-q3.iam.gserviceaccount.com'
# BASE_DIR = os.path.dirname(os.path.abspath(__file__))  
# KEY_PATH = os.path.join(BASE_DIR, "gee-key.json")

# credentials = ee.ServiceAccountCredentials(service_account, KEY_PATH)

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s",
#     handlers=[
#         logging.StreamHandler(),
#         logging.FileHandler("app.log", mode="w")
#     ]
# )

# logger = logging.getLogger(__name__)

# try:
#     ee.Initialize(credentials)
# except Exception as e:
#     logger.error("Error in initializing GEE: %s", e)

# INPUT_CSV_PATH = os.path.join(BASE_DIR, "Input", "ragi_2021.csv")
# OUTPUT_CSV_PATH = os.path.join(BASE_DIR, "Output", "ragi_features_2021.csv")

# df_input = pd.read_csv(INPUT_CSV_PATH)
# features = [
#     ee.Feature(ee.Geometry.Point([row["Longitude"], row["Latitude"]]), {"id": index})
#     for index, row in df_input.iterrows()
# ]
# fc = ee.FeatureCollection(features)
# # df_output = pd.DataFrame(columns=["Longitude", "Latitude", "Month", "VV", "VH", "VH/VV", "NDVI", "EVI", "GNDVI", "SAVI", "NDWI", "NDMI", "RENDVI"])

# create_monthwise_s1_collection(ee, 2021)
# create_monthwise_s2_collection(ee, 2021)

# months = ["July", "August", "September", "October", "November", "December"]

# for index, row in df_input.iterrows():
#     logging.info("Processing row %d", index)
#     lat = row["Latitude"]
#     lon = row["Longitude"]
#     coordinate = (lon, lat)
    
#     try:
#         s1_data = get_sentinel_1_data(ee, coordinate)
#         s2_data = get_sentinel_2_data(ee, coordinate)
#     except Exception as e:
#         logger.error("Error fetching data for coordinate %s: %s", coordinate, e)
#         continue
    
#     for month in months:
#         s1_month = s1_data.get(month, {"VV": 0, "VH": 0, "VH/VV": 0})
#         vv = s1_month.get("VV", 0) if s1_month.get("VV") is not None else 0
#         vh = s1_month.get("VH", 0) if s1_month.get("VH") is not None else 0
#         vh_vv = s1_month.get("VH/VV", 0) if s1_month.get("VH/VV") is not None else 0
        
#         s2_month = s2_data.get(month, {"NDVI": 0, "EVI": 0, "GNDVI": 0, "SAVI": 0, "NDWI": 0, "NDMI": 0, "RENDVI": 0})
#         ndvi = s2_month.get("NDVI", 0) if s2_month.get("NDVI") is not None else 0
#         evi = s2_month.get("EVI", 0) if s2_month.get("EVI") is not None else 0
#         gndvi = s2_month.get("GNDVI", 0) if s2_month.get("GNDVI") is not None else 0
#         savi = s2_month.get("SAVI", 0) if s2_month.get("SAVI") is not None else 0
#         ndwi = s2_month.get("NDWI", 0) if s2_month.get("NDWI") is not None else 0
#         ndmi = s2_month.get("NDMI", 0) if s2_month.get("NDMI") is not None else 0
#         rendvi = s2_month.get("RENDVI", 0) if s2_month.get("RENDVI") is not None else 0
        
#         new_row = pd.DataFrame([{"Longitude": lon, "Latitude": lat, "Month": month, "VV": vv, "VH": vh, "VH/VV": vh_vv, "NDVI": ndvi, "EVI": evi, "GNDVI": gndvi, "SAVI": savi, "NDWI": ndwi, "NDMI": ndmi, "RENDVI": rendvi }])
        
#         if new_row.empty:
#             logger.warning(f"Empty DataFrame encountered for month: {month}, skipping concatenation.")
#         else:
#             df_output = pd.concat([df_output, new_row], ignore_index=True)

# df_output.to_csv(OUTPUT_CSV_PATH, index=False)
# logger.info("Processing complete. Output saved with %d rows.", len(df_output))


# """ TESTING """
 
# # coordinate = (77.26087166666666,13.769433333333332)
# # lon, lat = coordinate

# # s1_data = get_sentinel_1_data(ee, coordinate)
# # s2_data = get_sentinel_2_data(ee, coordinate)
    
# # for month in months:
# #         s1_month = s1_data.get(month, {"VV": 0, "VH": 0, "VH/VV": 0})
# #         vv = s1_month.get("VV", 0) if s1_month.get("VV") is not None else 0
# #         vh = s1_month.get("VH", 0) if s1_month.get("VH") is not None else 0
# #         vh_vv = s1_month.get("VH/VV", 0) if s1_month.get("VH/VV") is not None else 0
        
# #         s2_month = s2_data.get(month, {"NDVI": 0, "EVI": 0, "GNDVI": 0, "SAVI": 0, "NDWI": 0, "NDMI": 0, "RENDVI": 0})
# #         ndvi = s2_month.get("NDVI", 0) if s2_month.get("NDVI") is not None else 0
# #         evi = s2_month.get("EVI", 0) if s2_month.get("EVI") is not None else 0
# #         gndvi = s2_month.get("GNDVI", 0) if s2_month.get("GNDVI") is not None else 0
# #         savi = s2_month.get("SAVI", 0) if s2_month.get("SAVI") is not None else 0
# #         ndwi = s2_month.get("NDWI", 0) if s2_month.get("NDWI") is not None else 0
# #         ndmi = s2_month.get("NDMI", 0) if s2_month.get("NDMI") is not None else 0
# #         rendvi = s2_month.get("RENDVI", 0) if s2_month.get("RENDVI") is not None else 0
        
# #         new_row = pd.DataFrame([{"Longitude": lon, "Latitude": lat, "Month": month, "VV": vv, "VH": vh, "VH/VV": vh_vv, "NDVI": ndvi, "EVI": evi, "GNDVI": gndvi, "SAVI": savi, "NDWI": ndwi, "NDMI": ndmi, "RENDVI": rendvi }])
        
# #         if new_row.empty:
# #             print(f"Empty DataFrame encountered for month: {month}, skipping concatenation.")
# #         else:
# #             df_output = pd.concat([df_output, new_row], ignore_index=True)
# # df_output.to_csv(OUTPUT_CSV_PATH, index=False)
# # logger.info("Processing complete. Output saved with %d rows.", len(df_output))

# # s1_data = get_sentinel_1_data(ee, coordinate)
# # s2_data = get_sentinel_2_data(ee, coordinate)