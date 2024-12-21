# Operational Flood Inundation Mapping(FIM) and Evaluation for CONUS

[![Version](https://img.shields.io/github/v/release/sdmlua/fimserve)](https://github.com/sdmlua/fimserve/releases)
[![Issues](https://img.shields.io/github/issues/sdmlua/fimserve)](https://github.com/sdmlua/fimserve/issues)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://opensource.org/licenses/GPL-3.0)
![Views](https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https://github.com/sdmlua/fimserve&count_bg=%2379C83D&title_bg=%23555555&icon=github.svg&icon_color=%23E7E7E7&title=Views&edge_flat=false)
[![PyPI version](https://badge.fury.io/py/fimserve.svg)](https://pypi.org/project/fimserve/0.1.62/)


### **Flood Inundation Mapping as a Service (fimserve)**
| | |
| --- | --- |
| <a href="https://sdml.ua.edu"><img src="https://sdml.ua.edu/wp-content/uploads/2023/01/SDML_logo_Sq_grey.png" alt="SDML Logo" width="300"></a> | This package presents a sophisticated and user friendly approach to generate Operational flood inundation map using NOAA_OWP Height Above Nearest Drainage (HAND) method-FIM model using National Water Model retrospective and forecasted streamflow data.It is developed under Surface Dynamics Modeling Lab (SDML). |


### **Background**
NOAA-Office of Water Predictions (OWP) use HAND FIM model for operational flood forecasting across CONUS (https://github.com/NOAA-OWP/inundation-mapping). It is a terrain based model that uses the Discharge and reach avergaed synthetic rating curves (SRCs) to generate the inundation and depth rasters at HUC-8 scale (Hydrologic Unit Code-8). The model is capable to produce flood maps less than a minute for all order streams within the watershed. The HUC-8 watersheds have catchment area more than 1000 sqkm , that makes this framework scalable and computationaly efficient. It is a fluvial flood model and doesnot have urban flood compponant.The last released version of the model is 4.5and has gone through significant improvements.The present notebook is user freindly and able to run the HAND FIM model from cloud and capable of running mutiple HUC-8s simulteniously.This model can run at any temporal resolution(hourly, daily, monthly etc).It uses the NHDPlus unique river identifiers and assign the streamflow for each of the segment. 

### **Package structures**
This version ([![Version](https://img.shields.io/github/v/release/sdmlua/fimserve)](https://github.com/sdmlua/fimserve/releases)) of code is available in fimserve 0.1.62 on [PyPI](https://pypi.org/project/fimserve/0.1.62/).
```bash
fimserve/
├── docs/(contain the code usage))
├── GeoGLOWS/(contain the streamflow download using GeoGLOWS hydrofabrics))
├── src/fimserve/
│           ├── streamflowdata/
                ├── nwmretrospectivedata.py
                └── forecasteddata.py
│           ├── plots/ (Contains different plotting functionality)
            ├── FIMsubset/
                ├── xycoord.py
                └── shpsubset.py
            ├── statistics/
                ├── calculatestatistics.py (calculates statistics between NWM and USGS gauge data)
            ├── datadownload.py (Includes all HUC8 raster things)
            ├── runFIM.py (OWPHAND model)
            ├── vizualization.py (In the Local jupyter notebook, it can be used to vizualize the user defined any inundation file interactively.)
└── tests/(includes test cases for each functionality)
```
### **Usage**
To use this code, 

```bash
#Using pip
pip install fimserve

#OR add using poetry to your framework deployment
poetry add fimserve
```

Once it installed, import it like 
```bash
import fimserve as fm

#to download HUC8
fm.DownloadHUC8(HUC)    #Like this it contains multiples functionality.
```
Then there are a lot of different modules, call it to work. For reference to run, [Here (docs/code_usage.ipynb)](./docs/code_usage.ipynb) is the sample usuage of this code and different functionality. 
 
### **Acknowledgements**
| | |
| --- | --- |
| ![alt text](https://ciroh.ua.edu/wp-content/uploads/2022/08/CIROHLogo_200x200.png) | Funding for this project was provided by the National Oceanic & Atmospheric Administration (NOAA), awarded to the Cooperative Institute for Research to Operations in Hydrology (CIROH) through the NOAA Cooperative Agreement with The University of Alabama (#Funding ID). |
| | We would like to acknowledge the TEEHR script developed by RTI International (https://github.com/RTIInternational/teehr). We use this script to get NWM discharge quickly.|

### **For More Information**
#### **Contact**

<a href="https://geography.ua.edu/people/sagy-cohen/" target="_blank">Dr. Sagy Cohen</a>
 (sagy.cohen@ua.edu),
Dr Anupal Baruah,(abaruah@ua.edu), Supath Dhital (sdhital@crimson.ua.edu)