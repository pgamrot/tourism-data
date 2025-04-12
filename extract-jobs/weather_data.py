import os
import sys
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import datetime
from helper_functions import download_from_gcs
import time

DOWNLOAD_DIR = "./tmp"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

date_parameter = sys.argv[1]

def download_weather_data(dataframe: pd.DataFrame, date_param: str) -> str:
    date_param = pd.to_datetime(date_param).date()
    print(f"Running script on: {date_param}")

    first_day_current_month = date_param.replace(day=1)
    last_day_previous_month = first_day_current_month - datetime.timedelta(days=1)
    first_day_previous_month = last_day_previous_month.replace(day=1)

    start_date = first_day_previous_month.isoformat()
    end_date = last_day_previous_month.isoformat()
    print(f"Will get data from: {start_date} to {end_date}")
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    dfs = []
    for index, row in dataframe.iterrows():
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": row["latitude"],
            "longitude": row["longitude"],
            "start_date": start_date,
            "end_date": end_date,
            "daily": ["weather_code", "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
                      "daylight_duration", "precipitation_sum", "wind_speed_10m_max"],
        }
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        daily = response.Daily()
        daily_weather_code = daily.Variables(0).ValuesAsNumpy()
        daily_temperature_2m_mean = daily.Variables(1).ValuesAsNumpy()
        daily_temperature_2m_max = daily.Variables(2).ValuesAsNumpy()
        daily_temperature_2m_min = daily.Variables(3).ValuesAsNumpy()
        daily_daylight_duration = daily.Variables(4).ValuesAsNumpy()
        daily_precipitation_sum = daily.Variables(5).ValuesAsNumpy()
        daily_wind_speed_10m_max = daily.Variables(6).ValuesAsNumpy()

        daily_data = {"date": pd.date_range(
            start=pd.to_datetime(daily.Time(), unit="s", utc=True),
            end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=daily.Interval()),
            inclusive="left"
        ), "weather_code": daily_weather_code, "temperature_2m_mean": daily_temperature_2m_mean,
            "temperature_2m_max": daily_temperature_2m_max, "temperature_2m_min": daily_temperature_2m_min,
            "daylight_duration": daily_daylight_duration, "precipitation_sum": daily_precipitation_sum,
            "wind_speed_10m_max": daily_wind_speed_10m_max}

        daily_dataframe = pd.DataFrame(data=daily_data)
        daily_dataframe['nuts_id'] = row['nuts_id']
        dfs.append(daily_dataframe)

    df_all = pd.concat(dfs)
    path = f"tmp/weather_data.parquet"
    df_all.to_parquet(path,coerce_timestamps='us')
    return path


download_from_gcs('staging-data-td1313', 'regions', 'dim_regions.csv', f"{DOWNLOAD_DIR}/dim_regions.csv")
df = pd.read_csv(f'{DOWNLOAD_DIR}/dim_regions.csv')
file = download_weather_data(df, date_parameter)
time.sleep(60)