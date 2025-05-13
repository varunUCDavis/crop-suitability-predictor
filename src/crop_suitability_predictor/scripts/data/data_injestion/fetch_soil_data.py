import requests

# URL of the file to download
url = "https://soilmap2-1.lawr.ucdavis.edu/800m_grids/rasters/caco3_kg_sq_m.tif"
output_file = "caco3_kg_sq_m.tif"

try:
    print(f"Downloading file from {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()

    # Save the file in chunks to avoid memory issues
    with open(output_file, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

    print(f"✅ Download complete. File saved as {output_file}")

except requests.RequestException as e:
    print(f"❌ Error downloading the file: {e}")