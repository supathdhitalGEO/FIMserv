#Function to download the data from AWS S3 bucket
#%%
import os
import shutil
import sys
import subprocess
import pandas as pd
import shutil
import xarray as xr
import numpy as np
import s3fs
import csv
import tempfile
from pathlib import Path
import geemap
import geopandas as gpd
import rasterio
from datetime import datetime, timedelta
from dotenv import load_dotenv
import fsspec
import glob
from ipyleaflet import WidgetControl
from ipywidgets import HTML
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import netCDF4 as nc
#%%
def download_data(huc_number, base_dir):
    output_dir = os.path.join(base_dir, f'flood_{huc_number}', str(huc_number))

    # Create the directory structure if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    #For the CIROH
    cmd = f"aws s3 sync s3://ciroh-owp-hand-fim/hand_fim_4_5_2_11/{huc_number}/ {output_dir} --no-sign-request "
    
    # Run the AWS CLI command
    os.system(cmd)
    print(f"Data for HUC {huc_number}")

    #Now copying the hydrotable.csv to the outside of directory as fim_inputs.csv
    hydrotable_path = os.path.join(output_dir, 'branch_ids.csv')
    fim_inputs_path = os.path.join(base_dir, f'flood_{huc_number}', 'fim_inputs.csv')
    
    # Read the first row from branch_ids.csv
    with open(hydrotable_path, 'r') as infile:
        reader = csv.reader(infile)
        first_row = next(reader)  # Get the first row

    # Write the first row to fim_inputs.csv
    with open(fim_inputs_path, 'w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(first_row)

    print(f"Copied the first row of {hydrotable_path} to {fim_inputs_path} as fim_inputs.csv.")
#%%
# Gets all the unique feature IDs from the hydrotable.csv file
def uniqueFID(hydrotable, discharge_dir):
    hydrotable_df = pd.read_csv(hydrotable)

    # Get unique feature IDs
    unique_FIDs = hydrotable_df['feature_id'].drop_duplicates()

    # Create a DataFrame with the unique feature IDs
    unique_FIDs_df = pd.DataFrame(unique_FIDs, columns=['feature_id'])
    
    unique_FIDs_df.to_csv(discharge_dir, index=False)
    print(f"Row numbers added and CSV file saved successfully to {discharge_dir}.")

#%%
#Function to get the final discharge from the combined discharge file and save it into inputs
def getDischargeforSpecifiedTime(csv_file_path, data_dir, huc):
    df = pd.read_csv(csv_file_path)

    # Ask the user for a specific date and time in the required format
    date_time_input = input("Enter the date and time within the specified daterange in the format 'yyyy-mm-dd HH:MM:SS': ")

    # Convert 'time' column to datetime format
    df['time'] = pd.to_datetime(df['time'])

    # Checking the input date and time in the dataframe
    matching_row = df[df['time'] == date_time_input]
    
    #Final filename as specifieddate_huc.csv and save it in input dischage directory
    datetime_obj = datetime.strptime(date_time_input, '%Y-%m-%d %H:%M:%S')
    formatted_datetime = datetime_obj.strftime('%Y%m%d%H%M%S')

    finalHANDdischarge_dir = os.path.join(data_dir, f'{formatted_datetime}_{huc}.csv')

    if matching_row.empty:
        print(f"No matching data found for the provided time: {date_time_input}")
    else:
        matching_row = matching_row[['feature_id', 'discharge']]
        matching_row.reset_index(drop=True, inplace=True)

        # Save the results to a CSV file
        matching_row.to_csv(finalHANDdischarge_dir, index=False)
        print(f"Final desired discharge of your specified time is saved in {finalHANDdischarge_dir}")
#%%
def DownloadDischargeData(HUC_dir, huc, data_dir, featureIDs, start_datetime, end_datetime, batch_size=30, max_workers=20):
    # Load the CHRTOUT dataset from the AWS bucket
    ds_chrtout = xr.open_zarr(
        fsspec.get_mapper('s3://noaa-nwm-retrospective-3-0-pds/CONUS/zarr/chrtout.zarr', anon=True),
        consolidated=True
    )

    # Select the time range for streamflow data
    ds2 = ds_chrtout.sel(time=slice(start_datetime, end_datetime))

    # Output directory to save the individual feature ID's time series
    output_dir1 = os.path.join(HUC_dir, f'{huc}_run')
    os.makedirs(output_dir1, exist_ok=True)

    featureIDs = pd.read_csv(featureIDs)
    
    # Split the featureIDs into batches
    batches = [featureIDs[i:i + batch_size] for i in range(0, len(featureIDs), batch_size)]

    # Function to download data for a batch of feature IDs
    def download_batch(batch):
        flow_dfs = []
        for index, row in batch.iterrows():
            feature_id = row['feature_id']
            try:
                # Select streamflow data for the feature_id
                Forecast_point = ds2['streamflow'].sel(feature_id=int(feature_id))

                # Convert the data to a DataFrame and include the time index
                Forecast_point_df = Forecast_point.to_dataframe().reset_index()[['time', 'streamflow']]

                # Rename columns appropriately
                Forecast_point_df.rename(columns={'streamflow': 'discharge'}, inplace=True)
                Forecast_point_df['feature_id'] = feature_id

                # Save the individual CSV file for the feature_id
                Forecast_point_df.to_csv(os.path.join(output_dir1, f'{feature_id}.csv'), index=False)

                # Append the individual DataFrame to the list
                flow_dfs.append(Forecast_point_df)
            except Exception as e:
                print(f'\nError with feature ID {feature_id}: {e}')
                continue
        return flow_dfs

    # Use ThreadPoolExecutor to download batches concurrently
    combined_dfs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(download_batch, batch) for batch in batches]
        for future in concurrent.futures.as_completed(futures):
            combined_dfs.extend(future.result())

    # Combine all DataFrames into a single DataFrame
    if combined_dfs:  # Check if there are any DataFrames to combine
        combined_df = pd.concat(combined_dfs, ignore_index=True)

        # Save the combined DataFrame to a CSV
        MergedDischarge_dir = os.path.join(output_dir1, f'concatDischarge_{huc}.csv')
        combined_df.to_csv(MergedDischarge_dir, index=False, encoding='utf-8')

        print(f"Combined discharge data saved to {MergedDischarge_dir}")
    else:
        print("No data was downloaded.")
    
    #Call to save the final discharge for the specified time
    getDischargeforSpecifiedTime(MergedDischarge_dir, data_dir, huc)
#%%
#Function to run the inundation mapping for the NWM data
def runOWPFIMonNWMdata(code_dir, output_dir,  HUC_code, data_dir):
    # Set up paths
    tools_path = os.path.join(code_dir, "tools")
    src_path = os.path.join(code_dir, "src")

    # Change to the tools directory
    os.chdir(tools_path)

    # Load environment variables from .env file located in the inundation-mapping directory
    dotenv_path = os.path.join(code_dir, ".env")
    load_dotenv(dotenv_path)

    # Add src and repository_path to the Python path
    sys.path.append(src_path)
    sys.path.append(code_dir)

    # Convert HUC_code to string
    HUC_code = str(HUC_code)

    #******************************************************************************
    HUC_dir = os.path.join(output_dir, f'flood_{HUC_code}')    # Output directory for the specific HUC

    #Get the discharge file path
    discharge = os.path.join(data_dir, f'*_{HUC_code}.csv')
    csv_path = glob.glob(discharge)
    csv_path = csv_path[0]

    #Inundation and depth file paths
    inundation_file = os.path.join(HUC_dir, f'{HUC_code}_inundation/inundation.tif')
    # depth_file = os.path.join(HUC_dir, f'{HUC_code}_inundation/depth.tif')

    # Command to run inside the Docker container
    Command = [
    sys.executable,
    "inundate_mosaic_wrapper.py",
    "-y", HUC_dir,
    "-u", HUC_code,
    "-f", csv_path,
    "-i", inundation_file
    # "-d", depth_file
    ]
    # Run the command with the correct working directory and PYTHONPATH
    env = os.environ.copy()
    env['PYTHONPATH'] = f"{src_path}{os.pathsep}{code_dir}"

    result = subprocess.run(Command, cwd=tools_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Print the output and error (if any)
    print(result.stdout.decode())
    if result.stderr:
        print(result.stderr.decode())

    # Check if the command was successful
    if result.returncode == 0:
        print(f"Inundation mapping for {HUC_code} completed successfully.")
    else:
        print(f"Failed to complete inundation mapping for {HUC_code}.")
#%%
def runOWPFIMonForecastedData(code_dir, output_dir,  HUC_code, data_dir):
    # Set up paths
    tools_path = os.path.join(code_dir, "tools")
    src_path = os.path.join(code_dir, "src")

    # Change to the tools directory
    os.chdir(tools_path)

    # Load environment variables from .env file located in the inundation-mapping directory
    dotenv_path = os.path.join(code_dir, ".env")
    load_dotenv(dotenv_path)

    # Add src and repository_path to the Python path
    sys.path.append(src_path)
    sys.path.append(code_dir)

    # Convert HUC_code to string
    HUC_code = str(HUC_code)

    #******************************************************************************
    HUC_dir = os.path.join(output_dir, f'flood_{HUC_code}')    # Output directory for the specific HUC

    #Get the discharge file path
    discharge = os.path.join(data_dir, f'forecasted_{HUC_code}.csv')
    csv_path = glob.glob(discharge)
    csv_path = csv_path[0]

    #Inundation and depth file paths
    inundation_file = os.path.join(output_dir, f'flood_{HUC_code}', f'Forecasted_streamflow/{HUC_code}_inundation/inundation.tif')
    # depth_file = os.path.join(HUC_dir, f'{HUC_code}_inundation/depth.tif')

    # Command to run inside the Docker container
    Command = [
    sys.executable,
    "inundate_mosaic_wrapper.py",
    "-y", HUC_dir,
    "-u", HUC_code,
    "-f", csv_path,
    "-i", inundation_file
    # "-d", depth_file
    ]
    # Run the command with the correct working directory and PYTHONPATH
    env = os.environ.copy()
    env['PYTHONPATH'] = f"{src_path}{os.pathsep}{code_dir}"

    result = subprocess.run(Command, cwd=tools_path, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # Print the output and error (if any)
    print(result.stdout.decode())
    if result.stderr:
        print(result.stderr.decode())

    # Check if the command was successful
    if result.returncode == 0:
        print(f"Inundation mapping for {HUC_code} completed successfully.")
    else:
        print(f"Failed to complete inundation mapping for {HUC_code}.")  
#%%
#Function to visualize the Flood Inundation Maps 
def FIMVizualizer(raster_path, catchment_gpkg, zoom_level):
    with rasterio.open(raster_path) as src:
        data = src.read(1) 
        binary_data = np.where(data > 0, 1, 0)  # Convert to binary

        # Creating a new raster with binary data
        new_raster_path = raster_path.replace('.tif', '_binary.tif')
        with rasterio.open(new_raster_path, 'w', driver='GTiff', height=src.height,
                           width=src.width, count=1, dtype=np.uint8, crs=src.crs, transform=src.transform) as dst:
            dst.write(binary_data.astype(np.uint8), 1)

    # Dissolve catchments into one boundary extent from the GeoPackage
    catchment_gdf = gpd.read_file(catchment_gpkg)
    dissolved_catchment = catchment_gdf.dissolve()

    # Initialize the map
    Map = geemap.Map()
    Map.add_basemap('SATELLITE', layer_name='Google Satellite')  # Add Google Satellite layer
    
    # HUC Boundary
    Map.add_gdf(dissolved_catchment, layer_name='HUC Boundary', style={'fillColor': 'none', 'color': 'Red', 'weight': 2.5, 'dashArray': '5, 5'})

    # Binary Raster
    Map.add_raster(new_raster_path, colormap='Blues', layer_name='Flood Inundation Extent', nodata=0) 
    
    # Set the zoom level
    center = dissolved_catchment.geometry.centroid.iloc[0]
    Map.set_center(center.x, center.y, zoom=zoom_level)
    
        # Create a custom HTML for the dashed rectangle in the legend
    legend_html = """
    <div style="font-size: 16px; line-height: 1.5;">
        <strong>Legend</strong><br>
        <div><span style="display:inline-block; width: 25px; height: 15px; background-color: #1c83eb; border: 1px solid #000;"></span>FIM Extent</div>
        <div><span style="display:inline-block; width: 25px; height: 15px; border: 2px dashed red; margin-right: 5px;"></span>HUC8  Boundary</div>
    </div>
    """

    # Add the HTML legend to the map
    legend_widget = HTML(value=legend_html)
    legend_control = WidgetControl(widget=legend_widget, position='bottomright')
    Map.add_control(legend_control)
    
    return Map
#%%
# Function to visualize multiple rasters of FIM in an interactive map
def MultipleFIMVizualizer(raster_dict, colormaps, boundary_shp, zoom_level):
    Map = geemap.Map()
    Map.add_basemap('SATELLITE', layer_name='Google Satellite')

    # Add the boundary shapefile if provided
    if boundary_shp is not None:
        boundary_gdf = gpd.read_file(boundary_shp)
        Map.add_gdf(boundary_gdf, layer_name='HUC Boundary', style={'fillColor': 'none', 'color': 'Red', 'weight': 2.5, 'dashArray': '5, 5'})

    for layer_name, raster in raster_dict.items():
        if isinstance(raster, Path):
            raster = str(raster)
        
        # Reclassify the raster within the loop: 2 -> 1, all others -> 0
        with rasterio.open(raster) as src:
            data = src.read(1) 
            reclassified_data = np.where(data == 2, 1, 0)
            transform = src.transform 
            crs = src.crs
            
            # Save the reclassified raster to a temporary file
            with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmpfile:
                tmp_raster = tmpfile.name
                with rasterio.open(
                    tmp_raster, 'w',
                    driver='GTiff',
                    height=reclassified_data.shape[0],
                    width=reclassified_data.shape[1],
                    count=1,
                    dtype=rasterio.uint8,
                    crs=crs,
                    transform=transform
                ) as dst:
                    dst.write(reclassified_data, 1)

        # Add the reclassified raster directly to the map
        Map.add_raster(tmp_raster, colormap=colormaps.get(layer_name), layer_name=layer_name, nodata=0)
    
    # Set the center and zoom level of the map
    if boundary_shp is not None:
        center = boundary_gdf.geometry.centroid.iloc[0]
        Map.set_center(center.x, center.y, zoom=zoom_level)
            
    # Create a custom HTML for the dashed rectangle in the legend
    legend_html = """
    <div style="font-size: 16px; line-height: 1.5;">
        <strong>Legend</strong><br>
        <div><span style="display:inline-block; width: 25px; height: 15px; background-color: #a4490e; border: 1px solid #000;"></span> Benchmark FIM</div>
        <div><span style="display:inline-block; width: 25px; height: 15px; background-color: #1c83eb; border: 1px solid #000;"></span> Candidate FIM</div>
        <div><span style="display:inline-block; width: 25px; height: 15px; border: 2px dashed red; margin-right: 5px;"></span>Study Area</div>
    </div>
    """

    # Add the HTML legend to the map
    legend_widget = HTML(value=legend_html)
    legend_control = WidgetControl(widget=legend_widget, position='bottomright')
    Map.add_control(legend_control)
    return Map
#%%
#Check whether athe download directory is clear or not
def clear_download_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f"Failed to delete {file_path}. Reason: {e}")
#%%
#Downloading the short range forecast data
def download_nc_files(date_str, current_hour, download_dir, url_base):
    url = f"{url_base}/nwm.{date_str}/short_range/"
    date_output_dir = os.path.join(download_dir, date_str)
    os.makedirs(date_output_dir, exist_ok=True)

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    pattern = re.compile(rf'nwm\.t{current_hour:02d}z\.short_range\.channel_rt\.f\d{{3}}\.conus\.nc')
    nc_files = [link['href'] for link in soup.find_all('a', href=True) if pattern.search(link['href'])]

    if not nc_files:
        return False, date_output_dir

    hour_output_dir = os.path.join(date_output_dir, f'{current_hour:02d}')
    os.makedirs(hour_output_dir, exist_ok=True)

    for nc_file in nc_files:
        file_url = url + nc_file
        file_path = os.path.join(hour_output_dir, nc_file)
        
        print(f'Downloading {file_url} to {file_path}')
        file_response = requests.get(file_url)
        with open(file_path, 'wb') as f:
            f.write(file_response.content)
        # print(f'Download completed for {file_path}')

    # print('All downloads completed for the date:', date_str)
    return True, hour_output_dir
#%%
#Process netcf and get the file in CSV format and extract all feature_is's discharge data
def process_netcdf_file(netcdf_file_path, filter_df, output_folder_path):
    base_filename = os.path.basename(netcdf_file_path).replace('.nc', '')
    output_csv_file_path = os.path.join(output_folder_path, f'{base_filename}.csv')

    try:
        ds = nc.Dataset(netcdf_file_path, 'r')
        streamflow_data = ds.variables['streamflow'][:]
        feature_ids = ds.variables['feature_id'][:]
        ds.close()
    except Exception as e:
        print(f"Error reading NetCDF file {netcdf_file_path}: {e}")
        return

    if len(streamflow_data) == 0 or len(feature_ids) == 0:
        print(f"No data found in {netcdf_file_path}")
        return

    data_df = pd.DataFrame({
        'feature_id': feature_ids,
        'discharge': streamflow_data
    })

    filtered_df = data_df[data_df['feature_id'].isin(filter_df['feature_id'])]
    merged_df = pd.merge(filter_df[['feature_id']], filtered_df, on='feature_id')
    merged_df.to_csv(output_csv_file_path, index=False)
    # print(f'Filtered DataFrame saved to {output_csv_file_path}')
#%%
def main(download_dir, output_csv_filename, HUC, data_dir, output_dir, url_base):
    # Clear download directory before downloading new files
    clear_download_directory(download_dir)

    today = datetime.utcnow().strftime('%Y%m%d')
    current_hour = datetime.utcnow().hour

    success = False
    attempts = 0

    while not success and attempts < 24:
        attempts += 1
        success, date_output_dir = download_nc_files(today, current_hour, download_dir, url_base)
        if not success:
            current_hour = (current_hour - 1) % 24
            if current_hour == 23:
                today = (datetime.utcnow() - timedelta(days=1)).strftime('%Y%m%d')

    if not success:
        print("No recent forecast data found. Exiting.")
        return
    
    filter_csv_file_path = os.path.join(output_dir, output_csv_filename)
    output_folder_path = os.path.join(download_dir, "Data")
    os.makedirs(output_folder_path, exist_ok=True)

    filter_df = pd.read_csv(filter_csv_file_path)

    if os.path.exists(date_output_dir):
        for root, _, files in os.walk(date_output_dir):
            for filename in files:
                if filename.endswith('.nc'):
                    netcdf_file_path = os.path.join(root, filename)
                    process_netcdf_file(netcdf_file_path, filter_df, output_folder_path)
    
    csv_directory = output_folder_path
    csv_files = [file for file in os.listdir(csv_directory) if file.endswith('.csv')]

    if not csv_files:
        print("No CSV files found after processing NetCDF files.")
        return

    combined_df = pd.concat([pd.read_csv(os.path.join(csv_directory, file))[['feature_id', 'discharge']] for file in csv_files])

    combined_df = combined_df.pivot_table(index='feature_id', values='discharge', aggfunc=list).apply(pd.Series.explode).reset_index()
    combined_df['discharge'] = combined_df['discharge'].astype(float)
    combined_df = combined_df.groupby('feature_id')['discharge'].apply(list).reset_index()
    for i in range(1, len(combined_df['discharge'][0]) + 1):
        combined_df[f'discharge_{i}'] = combined_df['discharge'].apply(lambda x: x[i-1] if i-1 < len(x) else None)
    combined_df.drop(columns=['discharge'], inplace=True)

    output_file = os.path.join(download_dir, "combined_streamflow.csv")
    combined_df.to_csv(output_file, index=False)

    # Extract maximum discharge for each feature_id
    max_discharge_df = combined_df.set_index('feature_id').max(axis=1).reset_index()
    max_discharge_df.columns = ['feature_id', 'discharge']

    max_output_file = os.path.join(data_dir, f"forecasted_{HUC}.csv")
    max_discharge_df.to_csv(max_output_file, index=False)

    print(f'Maximum discharge values saved to {max_output_file}')
#%%
#Reset Current Directory if Change
def reset_working_directory(parent_dir):
    current_directory = Path(os.getcwd())
    if current_directory != parent_dir:
        os.chdir(parent_dir)
        print(f"Working directory reset to {parent_dir}")
    else:
        print(f"Working directory is already {parent_dir}")
