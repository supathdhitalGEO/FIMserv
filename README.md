# OWP HAND-FIM 'as a service' (FIMserv)
<hr style="border: 1px solid black; margin: 0;">  

[![Version](https://img.shields.io/github/v/release/sdmlua/fimserve)](https://github.com/sdmlua/fimserve/releases)
[![Issues](https://img.shields.io/github/issues/sdmlua/fimserve)](https://github.com/sdmlua/fimserve/issues)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://opensource.org/licenses/GPL-3.0)
![Views](https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https://github.com/sdmlua/fimserve&count_bg=%2379C83D&title_bg=%23555555&icon=github.svg&icon_color=%23E7E7E7&title=Views&edge_flat=false)
[![PyPI version](https://badge.fury.io/py/fimserve.svg)](https://badge.fury.io/py/fimserve)
[![PyPI Downloads](https://static.pepy.tech/badge/fimserve)](https://pepy.tech/projects/fimserve)



### **Flood Inundation Mapping as a Service (fimserve)**
<hr style="border: 1px solid black; margin: 0;">  

| | |
| --- | --- |
| <a href="https://sdml.ua.edu"><img src="https://sdml.ua.edu/wp-content/uploads/2023/01/SDML_logo_Sq_grey.png" alt="SDML Logo" width="300"></a> | This package presents a streamlined, user-friendly and cloud-enabled pipeline to generate Operational flood inundation map using the NOAA Office of Water Prediction (OWP) Height Above Nearest Drainage (HAND) Flood Inundation MApping (FIM) framework using the National Water Model retrospective and forecasted streamflow. It is developed under the Surface Dynamics Modeling Lab (SDML) as part of a project funded by the Cooperative Institute for Research to Operations in Hydrology (CIROH). |


### **Background**
<hr style="border: 1px solid black; margin: 0;">  

OWP HAND-FIM is a national-scale operational flood forecasting framework (https://github.com/NOAA-OWP/inundation-mapping). It is a terrain-based fluvial flooding model that uses model-predicted streamflow and reach-averaged Synthetic Rating Curves (SRCs) to generate inundation extent and depth rasters at HUC-8 scale (Hydrologic Unit Code-8). The model can produce FIMs for all order streams within the watershed at a very low computational cost. This notebook streamline the FIM generation process or the OWP HAND-FIM framework on the cloud. It allow users to run over mutiple HUC-8s simultaneously. This model can run using any temporal resolution available from the input streamflow data (hourly, daily, monthly etc). 

### **Package structures**
<hr style="border: 1px solid black; margin: 0;">  

This version ([![Version](https://img.shields.io/github/v/release/sdmlua/fimserve)](https://github.com/sdmlua/fimserve/releases)) of code is available on [PyPI](https://pypi.org/project/fimserve/0.1.62/).
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
**The structure of the framework consisting its applications and connection between different functionalities.** The right figure, **b)**, is the directory structure used in this package (for e.g. after using this code by following [docs/code_usage.ipynb](./docs/code_usage.ipynb)) to download and process one or multiple hucs. 

<img src="https://github.com/supathdhitalGEO/fimserve/blob/main/images/flowchart.jpg"/>

<em>Figure: Flow chart of the framework (a) complete workflow demonstrating how the framework is designed, and (b) the directory structure on the user's end after runing the code.</em>
### **Usage**
<hr style="border: 1px solid black; margin: 0;">  

Although not mandatory, 
**we strongly recommend users create a virtual environment and install this package on that virtual environment to avoid the conflict between system dependencies and package dependencies.**
```bash
#creating a virtual environment using conda
conda create --name fimserve python==3.10

#Activate environment
conda activate fimserve
```
**OR user can directly install it without a virtual environment**
```bash
#Using pip
pip install fimserve

#OR add using poetry to your framework deployment
poetry add fimserve
```

Once it is installed, import it like 
```bash
import fimserve as fm

#to download HUC8
fm.DownloadHUC8(HUC)    #Like this it contains multiples functionality.
```
Then there are a lot of different modules, call it to work. For reference to run, [Here (docs/code_usage.ipynb)](./docs/code_usage.ipynb) is the sample usage of this code and different functionality. 

For quick usage: Use Google Colab. Here is **fimserve  in google colab**: [![Google Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1rwyoHmZJzCdvfn9pK-4csuXg7SVTeG-q?usp=sharing)


**Different HUC8 IDs, USGS gauge stations and flowline information that might be required to further understand/running this framework can be found in this <a href="https://ualabama.maps.arcgis.com/apps/instant/basic/index.html?appid=88789b151b50430d8e840d573225b36b" target="_blank">ArcGIS Instant App</a>.** 

 
### **Acknowledgements**
<hr style="border: 1px solid black; margin: 0;">  

| | |
| --- | --- |
| ![alt text](https://ciroh.ua.edu/wp-content/uploads/2022/08/CIROHLogo_200x200.png) | Funding for this project was provided by the National Oceanic & Atmospheric Administration (NOAA), awarded to the Cooperative Institute for Research to Operations in Hydrology (CIROH) through the NOAA Cooperative Agreement with The University of Alabama (#Funding ID). |
| | We would like to acknowledge the TEEHR script developed by RTI International (https://github.com/RTIInternational/teehr). We use this script to get NWM discharge quickly.|

### **For More Information**
<hr style="border: 1px solid black; margin: 0;">  

#### **Contact**

<a href="https://geography.ua.edu/people/sagy-cohen/" target="_blank">Dr. Sagy Cohen</a>
 (sagy.cohen@ua.edu),
Dr Anupal Baruah,(abaruah@ua.edu), Supath Dhital (sdhital@crimson.ua.edu)
