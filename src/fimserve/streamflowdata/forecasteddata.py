import os
import re
import requests
import pandas as pd
from pathlib import Path
import netCDF4 as nc
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from ..datadownload import setup_directories


def download_nc_files(date_str, current_hour, download_dir, url_base, forecast_range):
    date_obj = datetime.strptime(date_str, "%Y%m%d")
    start_date = datetime(2018, 9, 17)
    end_date = datetime(2019, 6, 18)

    # Adjust forecast_type based on the date range
    if start_date <= date_obj <= end_date:
        forecast_type = re.sub(
            r"(?i)mediumrange|medium[-\s]?range", "medium_range", forecast_range
        )
    else:
        forecast_type = re.sub(
            r"(?i)mediumrange|medium[-\s]?range", "medium_range_mem1", forecast_range
        )

    forecast_type = re.sub(
        r"(?i)shortrange|short[-\s]?range", "short_range", forecast_type
    )
    forecast_type = re.sub(
        r"(?i)longrange|long[-\s]?range", "long_range_mem1", forecast_type
    )

    url = f"{url_base}/nwm.{date_str}/{forecast_type}/"

    date_output_dir = os.path.join(download_dir, "netCDF", date_str)
    os.makedirs(date_output_dir, exist_ok=True)

    # Possible File patterns for each forecast type
    if forecast_type == "short_range":
        forecast_range_files = [
            f"nwm.t{current_hour:02d}z.short_range.channel_rt.f{hour:03d}.conus.nc"
            for hour in range(1, 18)
        ]
    elif forecast_type == "medium_range":
        forecast_range_files = [
            f"nwm.t{current_hour:02d}z.medium_range.channel_rt.f{hour:03d}.conus.nc"
            for hour in range(3, 240, 3)
        ]
    elif forecast_type == "medium_range_mem1":
        forecast_range_files = [
            f"nwm.t{current_hour:02d}z.medium_range.channel_rt_1.f{hour:03d}.conus.nc"
            for hour in range(3, 240, 3)
        ]
    elif forecast_type == "long_range_mem1":
        forecast_range_files = [
            f"nwm.t{current_hour:02d}z.long_range.channel_rt_1.f{hour:03d}.conus.nc"
            for hour in range(6, 720, 6)
        ]

    successful_downloads = []

    for forecast_file in forecast_range_files:
        file_url = os.path.join(url, forecast_file)
        file_path = os.path.join(date_output_dir, forecast_file)

        try:
            download_public_file(file_url, file_path)
            successful_downloads.append(forecast_file)
        except requests.exceptions.RequestException as e:
            print(f"Failed to download {forecast_file}: {e}")

    if not successful_downloads:
        return False, date_output_dir
    return True, date_output_dir


def download_public_file(url, destination_path):
    response = requests.get(url)
    if response.status_code == 404:
        return
    response.raise_for_status()
    with open(destination_path, "wb") as f:
        f.write(response.content)


# %%
# Process netcf and get the file in CSV format and extract all feature_is's discharge data
def process_netcdf_file(netcdf_file_path, filter_df, output_folder_path):
    base_filename = os.path.basename(netcdf_file_path).replace(".nc", "")
    output_csv_file_path = os.path.join(output_folder_path, f"{base_filename}.csv")

    try:
        ds = nc.Dataset(netcdf_file_path, "r")
        streamflow_data = ds.variables["streamflow"][:]
        feature_ids = ds.variables["feature_id"][:]
        ds.close()
    except Exception as e:
        print(f"Error reading NetCDF file {netcdf_file_path}: {e}")
        return

    if len(streamflow_data) == 0 or len(feature_ids) == 0:
        print(f"No data found in {netcdf_file_path}")
        return

    data_df = pd.DataFrame({"feature_id": feature_ids, "discharge": streamflow_data})

    filtered_df = data_df[data_df["feature_id"].isin(filter_df["feature_id"])]
    merged_df = pd.merge(filter_df[["feature_id"]], filtered_df, on="feature_id")
    merged_df.to_csv(output_csv_file_path, index=False)
    # print(f'Filtered DataFrame saved to {output_csv_file_path}')


# %%
def main(
    download_dir,
    output_csv_filename,
    HUC,
    data_dir,
    output_dir,
    forecast_range,
    forecast_date=None,
    hour=None,
    sort_by="maximum",
    url_base="https://storage.googleapis.com/national-water-model",
):
    if not hour:
        hour = datetime.utcnow().hour

    if forecast_date:
        date_obj = datetime.strptime(forecast_date, "%Y-%m-%d")
        forecast_date = date_obj.strftime("%Y%m%d")
    else:
        forecast_date = datetime.utcnow().strftime("%Y%m%d")
    print(f"Downloading forecast data for {forecast_date} at {hour:02d}Z")

    success = False
    attempts = 0

    while not success and attempts < 24:
        attempts += 1
        success, date_output_dir = download_nc_files(
            forecast_date, hour, download_dir, url_base, forecast_range
        )
        if not success:
            current_hour = (hour - 1) % 24
            if current_hour == 23:
                today = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")

    if not success:
        print("No recent forecast data found. Exiting.")
        return

    filter_csv_file_path = os.path.join(output_dir, output_csv_filename)
    output_folder_path = os.path.join(download_dir, "csvFiles")
    os.makedirs(output_folder_path, exist_ok=True)
    filter_df = pd.read_csv(filter_csv_file_path)

    if os.path.exists(date_output_dir):
        for root, _, files in os.walk(date_output_dir):
            for filename in files:
                if filename.endswith(".nc"):
                    netcdf_file_path = os.path.join(root, filename)
                    process_netcdf_file(netcdf_file_path, filter_df, output_folder_path)

    csv_directory = output_folder_path
    csv_files = [file for file in os.listdir(csv_directory) if file.endswith(".csv")]

    if not csv_files:
        print("No CSV files found after processing NetCDF files.")
        return

    combined_df = pd.concat(
        [
            pd.read_csv(os.path.join(csv_directory, file))[["feature_id", "discharge"]]
            for file in csv_files
        ]
    )

    combined_df = (
        combined_df.pivot_table(index="feature_id", values="discharge", aggfunc=list)
        .apply(pd.Series.explode)
        .reset_index()
    )
    combined_df["discharge"] = combined_df["discharge"].astype(float)
    combined_df = (
        combined_df.groupby("feature_id")["discharge"].apply(list).reset_index()
    )

    discharge_values = pd.DataFrame(
        combined_df["discharge"].tolist(), index=combined_df.index
    )
    discharge_values.columns = [
        f"discharge_{i+1}" for i in range(discharge_values.shape[1])
    ]

    combined_df = pd.concat(
        [combined_df.drop(columns=["discharge"]), discharge_values], axis=1
    )
    output_file = os.path.join(download_dir, "combined_streamflow.csv")
    combined_df.to_csv(output_file, index=False)

    if sort_by == "minimum":
        discharge_stat_df = (
            combined_df.set_index("feature_id").min(axis=1).reset_index()
        )
    elif sort_by == "median":
        discharge_stat_df = (
            combined_df.set_index("feature_id").median(axis=1).reset_index()
        )
    else:
        discharge_stat_df = (
            combined_df.set_index("feature_id").max(axis=1).reset_index()
        )

    discharge_stat_df.columns = ["feature_id", "discharge"]
    output_file = os.path.join(data_dir, f"{forecast_range}_{HUC}.csv")
    discharge_stat_df.to_csv(output_file, index=False)
    print(f"The final discharge values saved to {output_file}")


def getNWMForecasteddata(
    huc, forecast_range, forecast_date=None, hour=None, sort_by="maximum"
):
    code_dir, data_dir, output_dir = setup_directories()
    download_dir = os.path.join(
        output_dir, f"flood_{huc}", "discharge", f"{forecast_range}_forecast"
    )
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    featureIDs = Path(output_dir, f"flood_{huc}", "feature_IDs.csv")
    main(
        download_dir,
        featureIDs,
        huc,
        data_dir,
        output_dir,
        forecast_range,
        forecast_date,
        hour,
        sort_by,
    )
