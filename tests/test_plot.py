import fimserve as fm


huc = "03020202"

# Mention the data range  so that it will plot that specific range
start_date = "2016-01-01"
end_date = "2016-12-30"

# For 03020202
feature_id = ["11239079", "11239241", "11239465", "8791643"]
usgs_sites = ["0209205053", "02091814", "02089500", "02089000"]


# fm.getNWMretrospectivedata(start_date, end_date, huc)

# If user didnot provide the feature_id then it will plot the data for the feature_id with maximum discharge
# fm.plotNWMStreamflow(huc,start_date, end_date)

# If user provide the feature_id then it will plot the data for the provided feature_id
fm.plotNWMStreamflow(huc, start_date, end_date, feature_id)

# Get USGS stations on that particular huc, and their corresponding feature_id
# fm.GetUSGSIDandCorrFID(huc)

# To plot the USGS, First download the USGS site data for date range in sites
# fm.getUSGSsitedata(start_date, end_date, usgs_sites, huc)
# fm.plotUSGSStreamflow(huc, usgs_sites, start_date, end_date)

# # To compare the discharge data between NWM and USGS
# fm.CompareNWMnUSGSStreamflow(huc, feature_id[2], usgs_sites[2], start_date, end_date)

# Calculate the statistics
# fm.CalculateStatistics(huc, feature_id[0], usgs_sites[0], start_date, end_date)

# to plot the SRC data user need the hydroID and corresponding branc ID
hydro_id = ["11640001", "11640020"]
hydro_id2 = ["24660615"]
branch_id = ["0", "0"]

feature_id = 5490541
feature_id2 = 5490493
dischargevalue = 10000

# User can pass the discharge value to get the stage value for that discharge
# fm.plotSRC(huc, hydro_id, branch_id, dischargevalue)
# fm.plotSRC(huc, hydro_id2, branch_id, feature_id2, dischargevalue)
