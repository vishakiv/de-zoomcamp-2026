import os
import sys
import argparse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time

## This script downloads the yellow and green taxi trip data (in parquet format) for specified years and months (default 2019 and 2020), uploads them to a GCS bucket, and verifies the uploads. 
# It uses parallel processing to speed up both downloading and uploading.

# Change this to your bucket name
BUCKET_NAME = "kestra-zoomcamp-vishaki-demo"

# If you authenticated through the GCP SDK you can comment out these two lines
CREDENTIALS_FILE = "service-account.json"
client = storage.Client.from_service_account_json(CREDENTIALS_FILE)
# If commented initialize client with the following
#client = storage.Client(project='zoomcamp-mod3-datawarehouse')


BASE_URL_TEMPLATE = "https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-"
MONTHS = [f"{i:02d}" for i in range(1, 13)]
DOWNLOAD_DIR = "data"

CHUNK_SIZE = 8 * 1024 * 1024

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

bucket = client.bucket(BUCKET_NAME)


def download_file(task):
    color = task["color"]
    year = task["year"]
    month = task["month"]

    url = f"{BASE_URL_TEMPLATE.format(color=color, year=year)}{month}.parquet"
    filename = f"{color}_tripdata_{year}-{month}.parquet"
    file_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, file_path)
        print(f"Downloaded: {file_path}")
        return file_path
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return None


def create_bucket(bucket_name):
    try:
        # Get bucket details
        bucket = client.get_bucket(bucket_name)

        # Check if the bucket belongs to the current project
        project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
        if bucket_name in project_bucket_ids:
            print(
                f"Bucket '{bucket_name}' exists and belongs to your project. Proceeding..."
            )
        else:
            print(
                f"A bucket with the name '{bucket_name}' already exists, but it does not belong to your project."
            )
            sys.exit(1)

    except NotFound:
        # If the bucket doesn't exist, create it
        bucket = client.create_bucket(bucket_name)
        print(f"Created bucket '{bucket_name}'")
    except Forbidden:
        # If the request is forbidden, it means the bucket exists but you don't have access to see details
        print(
            f"A bucket with the name '{bucket_name}' exists, but it is not accessible. Bucket name is taken. Please try a different bucket name."
        )
        sys.exit(1)


def verify_gcs_upload(blob_name):
    return storage.Blob(bucket=bucket, name=blob_name).exists(client)


def upload_to_gcs(file_path, max_retries=3):
    blob_name = os.path.basename(file_path)
    blob = bucket.blob(blob_name)
    blob.chunk_size = CHUNK_SIZE

    create_bucket(BUCKET_NAME)

    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_path} to {BUCKET_NAME} (Attempt {attempt + 1})...")
            blob.upload_from_filename(file_path)
            print(f"Uploaded: gs://{BUCKET_NAME}/{blob_name}")

            if verify_gcs_upload(blob_name):
                print(f"Verification successful for {blob_name}")
                return
            else:
                print(f"Verification failed for {blob_name}, retrying...")
        except Exception as e:
            print(f"Failed to upload {file_path} to GCS: {e}")

        time.sleep(5)

    print(f"Giving up on {file_path} after {max_retries} attempts.")


def parse_csv_list(value):
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download taxi trip parquet files and upload to GCS")
    parser.add_argument("--colors", type=str, default="yellow,green", help="Comma-separated colors (yellow,green)")
    parser.add_argument("--years", type=str, default="2019,2020", help="Comma-separated years to download (e.g. 2019,2020)")
    parser.add_argument("--months", type=str, default=",".join(MONTHS), help="Comma-separated months (01..12) or leave default for all")
    parser.add_argument("--download-dir", type=str, default=DOWNLOAD_DIR, help="Local download directory")
    parser.add_argument("--max-workers", type=int, default=4, help="Number of parallel workers for download/upload")

    args = parser.parse_args()

    colors = parse_csv_list(args.colors)
    years = parse_csv_list(args.years)
    months = parse_csv_list(args.months)
    DOWNLOAD_DIR = args.download_dir

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    create_bucket(BUCKET_NAME)

    # Build task list: each task is a dict with color/year/month
    tasks = []
    for color in colors:
        for year in years:
            for month in months:
                tasks.append({"color": color, "year": year, "month": month})

    # Download
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        file_paths = list(executor.map(download_file, tasks))

    # Upload
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        executor.map(upload_to_gcs, filter(None, file_paths))  # Remove None values

    print("All files processed and verified.")