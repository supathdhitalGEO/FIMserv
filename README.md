# Operational Flood Inundation Mapping(FIM) for CONUS

[![Version](https://img.shields.io/github/v/release/sdmlua/OperationalOWPFIM)](https://github.com/sdmlua/OperationalOWPFIM/releases)
[![Issues](https://img.shields.io/github/issues/sdmlua/OperationalOWPFIM)](https://github.com/sdmlua/OperationalOWPFIM/issues)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://opensource.org/licenses/GPL-3.0)
![Views](https://hits.seeyoufarm.com/api/count/incr/badge.svg?url=https://github.com/sdmlua/OperationalOWPFIM&count_bg=%2379C83D&title_bg=%23555555&icon=github.svg&icon_color=%23E7E7E7&title=Views&edge_flat=false)



### **Flood Inundation Mapping as a Service**
| | |
| --- | --- |
| <a href="https://sdml.ua.edu"><img src="https://sdml.ua.edu/wp-content/uploads/2023/01/SDML_logo_Sq_grey.png" alt="SDML Logo" width="300"></a> | This repository presents a sophisticated and user friendly approach to generate Operational flood inundation map using NOAA_OWP Height Above Nearest Drainage (HAND) method-FIM model using National Water Model retrospective and forecasted streamflow data.It is developed under Surface Dynamics Modeling Lab (SDML). |




### **Background**
NOAA-Office of Water Predictions (OWP) use HAND FIM model for operational flood forecasting across CONUS (https://github.com/NOAA-OWP/inundation-mapping). It is a terrain based model that uses the Discharge and reach avergaed synthetic rating curves (SRCs) to generate the inundation and depth rasters at HUC-8 scale (Hydrologic Unit Code-8). The model is capable to produce flood maps less than a minute for all order streams within the watershed. The HUC-8 watersheds have catchment area more than 1000 sqkm , that makes this framework scalable and computationaly efficient. It is a fluvial flood model and doesnot have urban flood compponant.The last released version of the model is 4.5and has gone through significant improvements.The present notebook is user freindly and able to run the HAND FIM model from cloud and capable of running mutiple HUC-8s simulteniously.This model can run at any temporal resolution(hourly, daily, monthly etc).It uses the NHDPlus unique river identifiers and assign the streamflow for each of the segment. 

### **Repository Structure**
Once the user clone the repository and run it, it will create required folder setup automatically and after running the code the structure looks like below.
The user need to put the HUC-8 (Hydrologic Unit Code-8) IDs in HUC.csv. 
**Final folder structure after you run the code**
```bash
OperationalOWPFIM/
├── code/
│   └── inundation-mapping/ (contains scripts of the flood model)
├── data/
│   └── inputs/(NWM Discharge value will be saved here in a format of STH_HUC8code)
├── outputs/
│   ├── flood_1stHUC8Code/
        ├── 1stHUC8Code(HAND Model files downloaded from CIROH s3 Bucket)/
        └── 1stHUC8Code_inundation/ (Contains final FIM as inundation.tif)
│   ├── flood_2ndHUC8Code/
│   ├── |     |    |   |
│   └── flood_nthHUC8Code(It will depend upon how many HUC id user put in HUC.csv)/
│   
├── OWPFIMProductionsfunctions.py (It contains all functions associated with notebook)
├── OWPHANDfim.ipynb (It is the main notebook code to get FIM)
├── HUC.csv
├── environment.yml (Contains the environment dependencies)
└── README.md
```

### **Usage**
To use this code, 

**Clone the repository:**
```bash
git clone https://github.com/sdmlua/OperationalOWPFIM.git
```
**Create a virtual environment:**
Be aware if your terminal is in different directory then user need to give the path of .yml file.
```bash
conda env create -f environment.yml
```
After Creating the virtual environment, It will create virtual environment in system named **OWPHANDFIM**
```bash
conda activate OWPHANDFIM
```
Now, you are good to go for jupyter notebook.

### **Acknowledgements**
| | |
| --- | --- |
| ![alt text](https://ciroh.ua.edu/wp-content/uploads/2022/08/CIROHLogo_200x200.png) | Funding for this project was provided by the National Oceanic & Atmospheric Administration (NOAA), awarded to the Cooperative Institute for Research to Operations in Hydrology (CIROH) through the NOAA Cooperative Agreement with The University of Alabama (#Funding ID). |
| | I would like to acknowledge Thanh Nhan Duc Tran (University of Virginia) and Hesham Elhaddad (Western Michigan University) for their contribution to the workflow. |

### **For More Information**
Contact <a href="https://geography.ua.edu/people/sagy-cohen/" target="_blank">Dr. Sagy Cohen</a>
 (sagy.cohen@ua.edu)
Dr Anupal Baruah,(abaruah@ua.edu)
