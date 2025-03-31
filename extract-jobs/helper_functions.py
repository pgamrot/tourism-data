import os
import time
import logging
from typing import Optional
from google.cloud import storage
from google.api_core import exceptions

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def verify_gcs_upload(
        blob_name: str,
        bucket_name: str,
        storage_client: Optional[storage.Client] = None
) -> bool:
    """
    Verify if a blob exists in the specified GCS bucket.

    Args:
        blob_name (str): Name of the blob to verify.
        bucket_name (str): Name of the bucket.
        storage_client (Optional[storage.Client]): Google Cloud Storage client.
                                                   If not provided, a new client will be created.

    Returns:
        bool: True if the blob exists, False otherwise.
    """
    try:
        # Create a new client if not provided
        if storage_client is None:
            storage_client = storage.Client()

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except exceptions.GoogleAPIError as e:
        logger.error(f"Verification error: {e}")
        return False


def upload_to_gcs(
        file_path: str,
        bucket_name: str,
        bucket_prefix: str,
        max_retries: int = 3,
        remove_on_success: bool = True
) -> bool:
    """
    Upload a file to Google Cloud Storage with retry mechanism and verification.

    Args:
        file_path (str): Path to the local file to upload.
        bucket_name (str): Name of the destination bucket.
        bucket_prefix (str): Prefix (folder) in the bucket to upload to.
        max_retries (int, optional): Maximum number of upload attempts. Defaults to 3.
        remove_on_success (bool, optional): Whether to remove local file after successful upload. Defaults to True.

    Returns:
        bool: True if upload was successful, False otherwise.
    """
    # Validate input file exists
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False

    storage_client = storage.Client()
    blob_name = f"{bucket_prefix.rstrip('/')}/{os.path.basename(file_path)}"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    for attempt in range(max_retries):
        try:
            logger.info(f"Uploading {file_path} to {bucket_name} (Attempt {attempt + 1})...")

            # Upload the file
            blob.upload_from_filename(file_path)
            logger.info(f"Uploaded: gs://{bucket_name}/{blob_name}")

            # Verify upload
            if verify_gcs_upload(blob_name, bucket_name, storage_client):
                logger.info(f"Verification successful for {blob_name}")

                # Remove local file if specified
                if remove_on_success:
                    try:
                        os.remove(file_path)
                        logger.info(f"Removed {file_path}")
                    except OSError as remove_error:
                        logger.warning(f"Could not remove {file_path}: {remove_error}")

                return True
            else:
                logger.warning(f"Verification failed for {blob_name}, retrying...")

        except exceptions.GoogleAPIError as e:
            logger.error(f"Failed to upload {file_path} to GCS: {e}")

        # Exponential backoff
        time.sleep(2 ** attempt)

    logger.error(f"Giving up on {file_path} after {max_retries} attempts.")
    return False


def download_from_gcs(
        bucket_name: str,
        bucket_prefix: str,
        source_blob_name: str,
        destination_file_name: str
) -> bool:
    """
    Download a blob from Google Cloud Storage.

    Args:
        bucket_name (str): Name of the source bucket.
        bucket_prefix (str): Prefix (folder) in the bucket.
        source_blob_name (str): Name of the blob to download.
        destination_file_name (str): Local path to save the downloaded file.

    Returns:
        bool: True if download was successful, False otherwise.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # Construct full blob path
        full_blob_name = f"{bucket_prefix.rstrip('/')}/{source_blob_name}"
        blob = bucket.blob(full_blob_name)

        # Download the file
        blob.download_to_filename(destination_file_name)

        logger.info(
            f"Downloaded storage object {source_blob_name} from bucket {bucket_name} "
            f"to local file {destination_file_name}."
        )
        return True

    except exceptions.GoogleAPIError as e:
        logger.error(f"Failed to download {source_blob_name}: {e}")
        return False