import fimserve as fm

huc = "03020202"

start_date = "2015-01-01"
end_date = "2022-01-03"

value_times = ["2015-01-01 00:00:00"]

# fm.DownloadHUC8(huc)

# For 03020202
feature_id = ["11239241", "11239465"]
# fm.getNWMretrospectivedata(huc, start_date, end_date, value_times)

# To plot the USGS, First download the USGS site data for date range in sites
usgs_sites = ["02091814", "02089500"]
# fm.getUSGSsitedata(huc, start_date, end_date, usgs_sites)

fm.CalculateStatistics(huc, feature_id[0], usgs_sites[0], start_date, end_date)
fm.CalculateStatistics(huc, feature_id[1], usgs_sites[1], start_date, end_date)
