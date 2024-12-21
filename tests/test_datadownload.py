import fimserve as fm
import pandas as pd
import time

huc_dir = pd.read_csv("../HUCs.csv")
SubSetHUC8 = huc_dir[:2].copy()

# To record the time
download_time = []
for i in SubSetHUC8["HUC8"]:
    start_time = time.time()
    fm.DownloadHUC8(i)
    end_time = time.time()
    download_time.append(end_time - start_time)

# Download times as a new column
SubSetHUC8["Downloading raster time"] = download_time
huc_dir.update(SubSetHUC8)
huc_dir.to_csv("../HUCs_updated.csv", index=False)


# Download the data
# fm.DownloadHUC8(huc)

# If user wants to update the streamorder

# stream_order = [6, 5]
# fm.DownloadHUC8(huc, stream_order)
