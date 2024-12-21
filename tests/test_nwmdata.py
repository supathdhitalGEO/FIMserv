import fimserve as fm

huc = "03020202"

start_date = "2020-01-01"
end_date = "2020-01-03"

# fm.getNWMretrospectivedata(start_date, end_date, huc)

# for fixed date or day data
value_times = ["2020-01-01 00:00:00"]
fm.getNWMretrospectivedata(start_date, end_date, huc, value_times)
