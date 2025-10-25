import pandas as pd
import requests

# --- Configuration ---
INPUT_FILE = "goupc-inputbarcodes.csv"
FOUND_OUTPUT = "foundgoupc_products.xlsx"
NOT_FOUND_OUTPUT = "not_foundgoupc_barcodes.xlsx"
API_BASE_URL = "https://go-upc.com/api/v1/code/"
API_KEY = ''
# --- Step 1: Read CSV safely, preserve leading zeros ---
df = pd.read_csv(INPUT_FILE, dtype={'barcode': str})
barcodes = df['barcode'].dropna().astype(str).str.strip()

# --- Step 2: Prepare containers ---
found_records = []
not_found_barcodes = []

# --- Step 3: Iterate and fetch API data ---
for idx, barcode in enumerate(barcodes, start=1):
    url = f"{API_BASE_URL}{barcode}"
    headers = {
        "Authorization": f"Bearer {API_KEY}"
    }

    print(f"({idx}/{len(barcodes)}) Fetching data for barcode: {barcode}")

    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            data = response.json()
            product = data.get("product", {})

            # Only add if 'product' exists
            if product:
                found_records.append({
                    "barcode": data.get("code", ""),
                    "product_name": product.get("name", ""),
                    "brand": product.get("brand", ""),
                    "upc": product.get("upc", ""),
                    "image_url": product.get("imageUrl", "")
                })
            else:
                print("Not found adding to list-")
                not_found_barcodes.append({"go_upc_barcodes": barcode})
        else:
            not_found_barcodes.append({"go_upc_barcodes": barcode})
    except Exception as e:
        print(f"⚠️ Error fetching {barcode}: {e}")
        not_found_barcodes.append({"go_upc_barcodes": barcode})


# --- Step 4: Save to Excel files ---
if found_records:
    found_df = pd.DataFrame(found_records)
    found_df.to_excel(FOUND_OUTPUT, index=False)
    print(f"✅ Saved found products to {FOUND_OUTPUT}")

if not_found_barcodes:
    not_found_df = pd.DataFrame(not_found_barcodes)
    not_found_df.to_excel(NOT_FOUND_OUTPUT, index=False)
    print(f"⚠️ Saved not found barcodes to {NOT_FOUND_OUTPUT}")
