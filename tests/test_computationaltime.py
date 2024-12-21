import fimserve as fm
import pandas as pd
import time

huc_dir = pd.read_csv("../HUC.csv", dtype={"HUC8": str})

# Start and end time for all the HUC8
start_date = "2020-01-01"
end_date = "2020-01-03"
value_time = ["2020-01-02 00:00:00"]

# To record the time
downloadHUC8Raster_time = []
downloadNWMretrospective_time = []
generatingFIM_time = []

for i in huc_dir["HUC8"]:
    # Getting time for HUC8 raster data downloading
    start_time = time.time()
    fm.DownloadHUC8(i)
    end_time = time.time()
    downloadHUC8Raster_time.append(end_time - start_time)

    # Getting time for NWM retrospective data downloading
    start_time = time.time()
    fm.getNWMretrospectivedata(start_date, end_date, i, value_time)
    end_time = time.time()
    downloadNWMretrospective_time.append(end_time - start_time)

    # Generating FIM
    start_time = time.time()
    fm.runOWPHANDFIM(i)
    end_time = time.time()
    generatingFIM_time.append(end_time - start_time)

# Download times as a new column
huc_dir["HUC8 raster download time"] = downloadHUC8Raster_time
huc_dir["NWM retrospective download time"] = downloadNWMretrospective_time
huc_dir["Generating FIM time"] = generatingFIM_time

huc_dir.update(huc_dir)
huc_dir.to_csv("../HUCs_Time.csv", index=False)
