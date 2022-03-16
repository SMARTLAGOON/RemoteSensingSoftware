# RemoteSensingSoftware
This repo contains remote sensing software developed in SMARTLAGOON project.

# USES
This repo contains scripts that allows to download satelital files batches from NASA y ESA. 

# Available and tested platforms
Nasa
MODIS Terra, SNPP VIRRS, NOOA VIIRS for OC instrument and Level 2.

ESA
Sentinel-3 A y B for OLCI instrument and  Level 2.

# Requiriments
For ESA products (Sentinels): you should have an account in creodias.es (https://portal.creodias.eu/register.php). The user and password will be add to config.ini of this project.

For NASA product (MODIS, NOAA, SNPP, etc): you should have an account in https://cmr.earthdata.nasa.gov/. The user and password will be add to config.ini of this project.

The project uses ElasticSearch to control the downloaded files and checks whether the point requested exists in a preivius file downloaded. It is posible run the download.py with database field empty, but it is necesary to generate the csv and other processes. 

# MAPPING for create elasticsearch index
....

## Config.ini
This file contains the basic parameters to connect the APIs. The structure is:

[ESA]
    url_search:https://finder.creodias.eu/resto/api/collections/Sentinel3/search.json
    url_auth:https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token
    date_format: %%Y-%%m-%%dT%%H:%%M:%%S.%%fZ
    user_name: 
    password: 


[NASA]
    url_search:https://cmr.earthdata.nasa.gov/search/granules.umm_json
    date_format: %%Y-%%m-%%dT%%H:%%M:%%S.000Z
    user_name: 
    password: 

The user just need to fill user_name and passwords in the platforms to use.

# Project.json
The scripts are based on project. A project is a json file wherein is defined the parameters to requests the api. Also this file contains the path wherein NC files will be saved and csv files will be generated.


# Running without Elasticsearch
import esa
file = 'file_nc/zip'
e = esa.ESA()
e.work_path = "path_of_file"
info = e.extract_data(file, lat, lon)



