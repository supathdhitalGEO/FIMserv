import fimserve as fm

huc = "03020202"

start_date = "2016-01-01"
end_date = "2016-12-30"

# For 03020202
feature_id = ["11239079", "11239241", "11239465", "8791643"]
usgs_site = ["0209205053", "02091814", "02089500", "02089000"]

fm.CalculateStatistics(huc, feature_id[2], usgs_site[2], start_date, end_date)
