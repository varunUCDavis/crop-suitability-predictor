import pandas as pd
import requests
import time

API_KEY = "B72C4408-27DC-3FCA-89B1-3EFA69741C84"
BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET/"

# Load crop list
crop_df = pd.read_csv("usda_crop_list_final.csv")

results = []
failures = []

for _, row in crop_df.iterrows():
    crop_name = row["Crop Name"]
    crop_code = row["CDL Code"]

    params = {
        "key": API_KEY,
        "commodity_desc": crop_name.upper(),
        "statisticcat_desc": "YIELD",
        "agg_level_desc": "STATE",
        "state_alpha": "CA",
        "format": "JSON"
    }

    try:
        time.sleep(5)
        res = requests.get(BASE_URL, params=params, timeout=20)
        res.raise_for_status()
        data = res.json().get("data", [])

        if not data:
            failures.append({"Crop Name": crop_name, "CDL Code": crop_code, "Reason": "No data returned"})
            continue

        df = pd.DataFrame(data)
        df["Value"] = pd.to_numeric(df["Value"].str.replace(",", ""), errors="coerce")
        df = df.dropna(subset=["Value"])

        if df.empty:
            failures.append({"Crop Name": crop_name, "CDL Code": crop_code, "Reason": "No numeric values found"})
            continue

        print(f"Successful for {crop_name}")

        for desc in df["short_desc"].unique():
            sub_df = df[df["short_desc"] == desc]
            max_val = sub_df["Value"].max()
            results.append((crop_name, crop_code, desc, max_val))

    except requests.exceptions.HTTPError as e:
        print(f"HTTPError for {crop_name}")
        if "Bad Request for url" in str(e):
            failures.append({"Crop Name": crop_name, "CDL Code": crop_code, "Reason": "Bad Request"})
        else:
            failures.append({"Crop Name": crop_name, "CDL Code": crop_code, "Reason": str(e)})
    except Exception as e:
        print(f"Non HTTPError for {crop_name}")
        failures.append({"Crop Name": crop_name, "CDL Code": crop_code, "Reason": str(e)})

    time.sleep(1.1)

# Convert to DataFrame with MultiIndex
result_df = pd.DataFrame(results, columns=["Crop Name", "CDL Code", "Short Description", "Max Value"])
result_df.set_index(["Crop Name", "CDL Code"], inplace=True)

# Save results
result_df.to_csv("crop_short_descs_with_max_multiindex.csv")

# Save failed crops
failures_df = pd.DataFrame(failures)
failures_df.to_csv("failed_crops_2.csv", index=False)

print("✅ Finished. Results saved as 'crop_short_descs_with_max_multiindex.csv' and failures in 'failed_crops.csv'")





















# import requests
# import pandas as pd
# API_KEY = "B72C4408-27DC-3FCA-89B1-3EFA69741C84"



# params = {
#     "key": API_KEY,
#     "commodity_desc": "OATS",
#     "statisticcat_desc": "YIELD",
#     #"unit_desc": "TONS / ACRE",
#     "agg_level_desc": "STATE",
#     "state_alpha": "CA",
#     "format": "JSON"
# }



# res = requests.get("https://quickstats.nass.usda.gov/api/api_GET/", params=params)
# res.raise_for_status()

# data = res.json()["data"]
# breakpoint()
# df = pd.DataFrame(data)
# breakpoint()
# # Clean and convert "Value" to numeric
# df["Value"] = pd.to_numeric(df["Value"].str.replace(",", ""), errors="coerce")
# df = df.dropna(subset=["Value"])

# # Optional: sort by highest yield
# df_sorted = df.sort_values("Value", ascending=False)

# # Print max yield info
# top_row = df_sorted.iloc[0]
# print(f"🏆 Max Wheat Yield: {top_row['Value']} bu/acre")
# print(f"📍 Location: {top_row['location_desc']} ({top_row['year']})")









####
# import pandas as pd
# import requests
# from urllib.parse import urlencode

# # --------------------------- CONFIGURATION --------------------------- #

# API_KEY = "B72C4408-27DC-3FCA-89B1-3EFA69741C84"  # Replace with your actual USDA QuickStats API key
# BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET/"

# def query_yield_data(crop_name: str, unit_filter="TONS / ACRE"):
#     """Query USDA QuickStats API for historical yield data for a crop (including subcrops)."""
#     params = {
#         "key": API_KEY,
#         "commodity_desc": crop_name.upper(),
#         "statisticcat_desc": "YIELD",
#         "year__GE": "2000",
#         "agg_level_desc": "COUNTY",
#         "source_desc": "SURVEY",
#         "format": "JSON",
#     }
#     breakpoint()
#     url = f"{BASE_URL}?{urlencode(params)}"
#     response = requests.get(url)
#     if response.status_code != 200:
#         raise Exception(f"Request failed: {response.status_code} {response.text}")

#     data = response.json()["data"]
#     df = pd.DataFrame(data)
#     breakpoint()
#     # Convert yield to numeric
#     df["yield_value"] = pd.to_numeric(df["Value"].str.replace(",", ""), errors="coerce")
#     df = df[df["unit_desc"] == unit_filter]
#     df = df.dropna(subset=["yield_value"])

#     return df

# def get_max_yield_for_crop(crop_name: str):
#     """Get the maximum historical yield (tons/acre) for a crop (including subcrop items)."""
#     try:
#         df = query_yield_data(crop_name)
#         max_yield = df["yield_value"].max()
#         return round(max_yield, 3)
#     except Exception as e:
#         return f"Error: {e}"

# # Example usage for grapes
# max_grape_yield = get_max_yield_for_crop("Grapes")
# print(max_grape_yield)
