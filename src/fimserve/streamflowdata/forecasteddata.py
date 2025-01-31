import os
import re
import shutil
import requests
import pandas as pd
from pathlib import Path
import netCDF4 as nc
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

from ..datadownload import setup_directories


def adjust_hour(hour, forecast_range):
    if forecast_range == "shortrange":
        return min(hour, 23)
    elif forecast_range in ["mediumrange", "longrange"]:
        valid_hours = [0, 6, 12, 18]
        adjusted_hour = max(h for h in valid_hours if h <= hour)
        return adjusted_hour
    return hour


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


# Process netcf and get the file in CSV format and extract all feature_is's discharge data
def processnetCDF(netcdf_file_path, filter_df, output_folder_path):
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


def ProcessForecasts(CSVFILES, forecast_date, hour, forecast_range, sort_by, data_dir):
    merge_folder = os.path.join(CSVFILES, "mergedAndSorted")
    os.makedirs(merge_folder, exist_ok=True)

    # Get all CSV files in the output folder
    if forecast_range == "longrange":
        csv_files = sorted(
            [
                file
                for file in os.listdir(CSVFILES)
                if file.endswith(".csv")
                and file.startswith(f"nwm.t{hour:02d}z.long_range")
            ]
        )
    elif forecast_range == "mediumrange":
        csv_files = sorted(
            [
                file
                for file in os.listdir(CSVFILES)
                if file.endswith(".csv")
                and file.startswith(f"nwm.t{hour:02d}z.medium_range")
            ]
        )

    elif forecast_range == "shortrange":
        csv_files = sorted(
            [
                file
                for file in os.listdir(CSVFILES)
                if file.endswith(".csv")
                and file.startswith(f"nwm.t{hour:02d}z.short_range")
            ]
        )
        # Process each file for shortrange
        pattern = re.compile(r"\.f(\d{3})\.")
        current_date = datetime.strptime(forecast_date, "%Y%m%d")
        base_hour = hour
        day_shift = 0

        for csv_file in csv_files:
            match = pattern.search(csv_file)
            if match:
                forecast_hour = int(match.group(1))
                adjusted_forecast_hour = base_hour + forecast_hour
                if adjusted_forecast_hour >= 24:
                    adjusted_forecast_hour %= 24
                    if adjusted_forecast_hour == 0:
                        day_shift += 1
                adjusted_date = (current_date + timedelta(days=day_shift)).strftime(
                    "%Y%m%d"
                )

                sorted_file_name = f"{forecast_range}_{adjusted_date}_{adjusted_forecast_hour:02d}UTC.csv"
                sorted_file_path = os.path.join(data_dir, sorted_file_name)
                original_file_path = os.path.join(CSVFILES, csv_file)
                os.rename(original_file_path, sorted_file_path)
        return

    # Calculating the day offset based on the forecast hour
    pattern = re.compile(r"\.f(\d{3})\.")
    current_date = datetime.strptime(forecast_date, "%Y%m%d")
    daily_groups = {}

    for csv_file in csv_files:
        match = pattern.search(csv_file)
        if match:
            forecast_hour = int(match.group(1))
            day_offset = (forecast_hour + hour) // 24
            group_date = (current_date + timedelta(days=day_offset)).strftime("%Y%m%d")

            if group_date not in daily_groups:
                daily_groups[group_date] = []
            daily_groups[group_date].append(csv_file)
        else:
            print(f"Filename does not match expected pattern: {csv_file}")

    # Merge and sort files for each day
    for group_date, group_files in daily_groups.items():
        combined_df = pd.concat(
            [pd.read_csv(os.path.join(CSVFILES, file)) for file in group_files]
        )
        if sort_by == "minimum":
            sorted_df = (
                combined_df.groupby("feature_id")["discharge"].min().reset_index()
            )
        elif sort_by == "median":
            sorted_df = (
                combined_df.groupby("feature_id")["discharge"].median().reset_index()
            )
        else:
            sorted_df = (
                combined_df.groupby("feature_id")["discharge"].max().reset_index()
            )

        sorted_file_name = f"{hour:02d}UTC_{forecast_range}_{group_date}.csv"
        sorted_file_path = os.path.join(data_dir, sorted_file_name)
        sorted_df.to_csv(sorted_file_path, index=False)


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
    if forecast_date:
        date_obj = datetime.strptime(forecast_date, "%Y-%m-%d")
        forecast_date = date_obj.strftime("%Y%m%d")
    else:
        forecast_date = datetime.utcnow().strftime("%Y%m%d")

    if hour is None:
        hour = datetime.utcnow().hour

    # Adjust the hour based on the forecast range
    hour = adjust_hour(hour, forecast_range)

    print(f"Downloading forecast data for {forecast_date} at {hour:02d}Z")

    success = False
    attempts = 0

    while not success and attempts < 24:
        attempts += 1
        success, date_output_dir = download_nc_files(
            forecast_date, hour, download_dir, url_base, forecast_range
        )
        if not success:
            hour = (hour - 1) % 24
            if hour == 23:
                forecast_date = (datetime.utcnow() - timedelta(days=1)).strftime(
                    "%Y%m%d"
                )
    # Check if there are any .nc files
    if not any(file.endswith(".nc") for file in os.listdir(date_output_dir)):
        print("No discharge data found, try few hours or a day back!")
        try:
            shutil.rmtree(download_dir)
        except Exception as e:
            print(f"Error removing CSV files directory: {e}")
        return

    filter_csv_file_path = os.path.join(output_dir, output_csv_filename)
    CSVFILES = os.path.join(download_dir, "csvFiles")
    os.makedirs(CSVFILES, exist_ok=True)
    filter_df = pd.read_csv(filter_csv_file_path)

    if os.path.exists(date_output_dir):
        for root, _, files in os.walk(date_output_dir):
            for filename in files:
                if filename.endswith(".nc"):
                    netcdf_file_path = os.path.join(root, filename)
                    processnetCDF(netcdf_file_path, filter_df, CSVFILES)

    # Merge and sort the csv data in daily basis for long range and medium range where as gives hourly streamflow for short range
    if (
        forecast_range == "longrange"
        or forecast_range == "mediumrange"
        or forecast_range == "shortrange"
    ):
        ProcessForecasts(
            CSVFILES, forecast_date, hour, forecast_range, sort_by, data_dir
        )
        print(f"The final discharge values saved to {data_dir}")
        try:
            shutil.rmtree(CSVFILES)
        except Exception as e:
            print(f"Error removing CSV files directory: {e}")


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
