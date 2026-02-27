import fimserve as fm
import pandas as pd


# Look for the benchmark FIM data for the HUC8 and event date
def test_bm_fimlookup():
    out = fm.fim_lookup(
        HUCID="11110205",
        # date_input="2017-05-12",  # If user is more specific then they can pass date (with hour if known) along with HUC8
        # start_date="2017-06-20", #If user is not sure of the exact date then they can pass a range of dates
        # end_date="2024-06-25",
        return_period=100,  # For Tier_4; BLE based benchmark, user can pass return period to genrate OWP HAND FIM to evaluate, we have 100 and 500 yr flows saves in AWS
        run_handfim=True,  # It will look for the owp hand fim for the mentioned HUC8 and date, if not found it will download and generate the owp hand fim; default is False
        # file_name= "BLE_10_0m_500_921957W351837N_BM.tif", #If user pass the specific filename, it will download that and assume that this is the right benchmark, else based on exact match of date it will look for the benchmark
        out_dir="../test_FIMeval",  # Required if user wants to download the benchmark FIM data
    )
    print(out)


# # After finalizing the benchmark FIM data user can run evaluation
# def test_run_fimeval():
#     fm.run_evaluation(
#         Main_dir="../test_FIMeval",  # If user uses their own input directory where FIM outputs; basically out_dir in fim_lookup is Main_dir here
#         output_dir=None,  # Folder where evaluation results will be saved
#         shapefile_path=None,  # AOI shapefile or vector file used to clip data during evaluation. Internally uses the geopackage within folder.
#         PWB_dir=None,  # Directory containing the Permanent Water Bodies.
#         building_footprint=None,  # Local building footprint dataset (GeoJSON/Shapefile) for building-level exposure evaluation., else it will use the arcgis online hosted building footprint using REST API
#         target_crs=None,  # CRS to reproject FIM rasters to (e.g., "EPSG:3857").
#         target_resolution=None,  # Output raster resolution (units depend on CRS).
#         method_name=None,  # By default it will use 'AOI'; to explore different methods pass here
#         print_graphs=True,  # If True, generates and saves contingency maps and evaluation metric plots.
#         Evalwith_BF=True,  # If True, run evaluation with building footprint
#     )
