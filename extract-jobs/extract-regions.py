import os
import geojson
import requests
import pandas as pd
import country_converter as coco
from helper_functions import upload_to_gcs

DOWNLOAD_DIR = "./tmp"


def download_file(theme: str = "NUTS", spatial_type="LB", resolution="01M", year="2024", projection="4326", level=2):
    base_url = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/"
    filename = f"{theme}_{spatial_type}{'_' + resolution if spatial_type != "LB" else ''}_{year}_{projection}_LEVL_{level}.geojson"
    url = base_url + filename

    # Output file name
    output_file = f"{DOWNLOAD_DIR}/{filename}"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for failed requests

        # Save the file
        if not os.path.exists(DOWNLOAD_DIR):
            os.makedirs(DOWNLOAD_DIR)
        with open(output_file, "wb") as file:
            file.write(response.content)

        print(f"File downloaded successfully: {output_file}")
        return output_file

    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return None


def transform_file(filepath):
    with open(filepath, 'r', encoding='utf8') as f:
        geojson2 = geojson.load(f)
    df = pd.json_normalize(geojson2.features)
    df[['longitude', 'latitude']] = df['geometry.coordinates'].apply(pd.Series)
    df = df[['properties.NUTS_ID', 'properties.CNTR_CODE', 'properties.LEVL_CODE', 'properties.NAME_LATN',
             'properties.NUTS_NAME', 'longitude', 'latitude', 'properties.MOUNT_TYPE', 'properties.URBN_TYPE',
             'properties.COAST_TYPE']]
    cc = coco.CountryConverter()
    df['country_name'] = cc.pandas_convert(series=df['properties.CNTR_CODE'], to='name_short')
    df.columns.to_list()
    df = df.rename(columns={
        'properties.NUTS_ID': 'nuts_id',
        'properties.CNTR_CODE': 'country_code',
        'properties.LEVL_CODE': 'code_level',
        'properties.NAME_LATN': 'latin_name',
        'properties.NUTS_NAME': 'nuts_name',
        'properties.MOUNT_TYPE': 'mount_type',
        'properties.URBN_TYPE': 'urban_type',
        'properties.COAST_TYPE': 'coast_type'
    })
    df = df.fillna(0)
    path = f'{DOWNLOAD_DIR}/dim_regions.csv'
    df.to_csv(path, index=False)
    return path


if __name__ == '__main__':
    raw_file = download_file()
    transformed_file = transform_file(raw_file)
    if raw_file:
        upload_to_gcs(file_path=raw_file, bucket_name="raw-data-td1313", bucket_prefix='regions')
    if transformed_file:
        upload_to_gcs(file_path=transformed_file, bucket_name="staging-data-td1313", bucket_prefix='regions')
