import os
import io
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pyzbar.pyzbar import decode
from PIL import Image
import pickle

# ====== CONFIGURATION ======
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
FOLDER_ID = ''   # 🔹 Replace with your shared folder ID
OUTPUT_FILE = 'scanned_barcodes.xlsx'
TEMP_FOLDER = './temp_images'              # Temp local storage for images
# ============================

# Get the script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Full paths
OUTPUT_PATH = os.path.join(SCRIPT_DIR, OUTPUT_FILE)
DOWNLOAD_PATH = os.path.join(SCRIPT_DIR, TEMP_FOLDER)


def authenticate_google_drive():
    """Authenticate and create a Drive service object."""
    creds = None
    token_path = os.path.join(SCRIPT_DIR, 'token.pickle')
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.join(SCRIPT_DIR, 'credentials.json'), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    service = build('drive', 'v3', credentials=creds)
    return service


def list_images_in_folder(service, folder_id):
    """List image files from a Google Shared Drive folder."""
    images = []
    page_token = None

    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents and mimeType contains 'image/' and trashed=false",
            fields="nextPageToken, files(id, name, mimeType)",
            pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            corpora='drive',
            driveId=folder_id,
            pageToken=page_token
        ).execute()

        images.extend(response.get('files', []))
        page_token = response.get('nextPageToken', None)
        if not page_token:
            break

    print(f"✅ Found {len(images)} image files in the specified shared folder.")
    return images


def download_image(service, file_id, file_name):
    """Download an image file from Drive."""
    if not os.path.exists(DOWNLOAD_PATH):
        os.makedirs(DOWNLOAD_PATH)
    file_path = os.path.join(DOWNLOAD_PATH, file_name)

    request = service.files().get_media(fileId=file_id)
    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return file_path


def scan_barcode_from_image(image_path):
    """Scan and decode barcode from an image."""
    try:
        img = Image.open(image_path)
        barcodes = decode(img)
        results = [barcode.data.decode('utf-8') for barcode in barcodes]
        return results
    except Exception as e:
        print(f"Error scanning {image_path}: {e}")
        return []


def main():
    service = authenticate_google_drive()

    print("📂 Fetching image list from shared drive folder...")
    images = list_images_in_folder(service, FOLDER_ID)

    barcodes_set = set()
    for i, img in enumerate(images, start=1):
        print(f"📸 [{i}/{len(images)}] Downloading and scanning: {img['name']}")
        local_path = download_image(service, img['id'], img['name'])
        barcodes = scan_barcode_from_image(local_path)
        if barcodes:
            print(f"   ➜ Found barcodes: {barcodes}")
            barcodes_set.update(barcodes)
        os.remove(local_path)  # Clean up local image after scanning

    # Save to Excel in the script directory
    if barcodes_set:
        df = pd.DataFrame({'Scanned Barcodes': list(barcodes_set)})
        df.to_excel(OUTPUT_PATH, index=False)
        print(f"\n✅ Barcode scan complete. {len(barcodes_set)} unique barcodes saved to {OUTPUT_PATH}")
    else:
        print("\n⚠️ No barcodes detected in the scanned images.")


if __name__ == "__main__":
    main()
