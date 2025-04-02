import os
import teehr
from pathlib import Path
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
from teehr.fetching.usgs.usgs import usgs_to_parquet

from ..datadownload import setup_directories
from ..plot import getUSGSdata

def getusgs_discharge(
    start_date,
    end_date,
    usgs_sites,
    output_root,
):
    output_dir = Path(output_root) / "discharge" / "usgs_streamflow"
    output_dir.mkdir(parents=True, exist_ok=True)

    usgs_to_parquet(
        start_date=start_date,
        end_date=end_date,
        sites=usgs_sites,
        output_parquet_dir=output_dir,
        overwrite_output=True,
    )
    print(f"USGS discharge data saved to {output_dir}.")



def getUSGSsitedata(start_date, end_date, usgs_sites, huc, value_times=None):
    code_dir, data_dir, output_dir = setup_directories()

    HUC_dir = os.path.join(output_dir, f"flood_{huc}")
    getusgs_discharge(start_date, end_date, usgs_sites, HUC_dir)
    
    