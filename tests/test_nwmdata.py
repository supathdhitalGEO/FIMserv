import fimserve as fm

huc = "03020202"

start_date = "2020-01-01"
end_date = "2020-01-03"

# fm.getNWMretrospectivedata(start_date, end_date, huc)

# to work with only huc or for getting a retrospective streamflow for the evaluation or something
value_times = ["2020-01-01 00:00:00"]
fm.getNWMretrospectivedata(start_date, end_date, huc, value_times)


# For the multiple watersheds with multiple events at the same time
huc_event_dict = {
    "03020202": ["2016-10-08 15:00:00", "2016-10-08"],
    "12060102": ["2016-10-09 15:00:00", "2016-10-09 16:00:00", "2016-10-09 17:00:00"],
}
fm.getNWMretrospectivedata(huc_event_dict=huc_event_dict)
