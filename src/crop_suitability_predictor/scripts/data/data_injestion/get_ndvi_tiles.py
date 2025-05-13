from pystac_client import Client
import planetary_computer
import geopandas as gpd
import os

# Load California shapefile
ca = gpd.read_file("../supporting_files/CA_Counties.shp").to_crs("EPSG:4326")
ca_geom = ca.unary_union

# Connect to STAC
catalog = Client.open("https://planetarycomputer.microsoft.com/api/stac/v1")

search = catalog.search(
    collections=["landsat-8-c2-l2"],
    intersects=ca_geom,
    datetime="2022-01-01/2022-12-31",
    query={"eo:cloud_cover": {"lt": 20}},
    max_items=5000
)

items = list(search.get_all_items())
print(f"Found {len(items)} scenes")

os.makedirs("landsat_tiles", exist_ok=True)

for item in items:
    scene_id = item.id
    signed_item = planetary_computer.sign(item)  # 🧠 THIS IS THE FIX

    b4_url = signed_item.assets["SR_B4"].href
    b5_url = signed_item.assets["SR_B5"].href

    b4_path = f"../supporting_files/landsat_tiles/{scene_id}_B4.TIF"
    b5_path = f"../supporting_files/landsat_tiles/{scene_id}_B5.TIF"

    def needs_download(path):
        return not os.path.exists(path) or os.path.getsize(path) < 1000  # skip if file is too small

    if needs_download(b4_path):
        os.system(f"wget -q -O {b4_path} '{b4_url}'")
    if needs_download(b5_path):
        os.system(f"wget -q -O {b5_path} '{b5_url}'")

