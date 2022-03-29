# RemoteSensingSoftware
This repo contains remote sensing software developed in SMARTLAGOON project.

## Uses
This repo contains scripts that allow to download satellite files from NASA y ESA in batches by date and points. 

## Available and tested platforms
### NASA
MODIS Terra, SNPP VIRRS, NOOA VIIRS for OC instrument and Level 2.

### ESA
Sentinel-3 A y B for OLCI instrument and  Level 2.

## Requiriments
For ESA products (Sentinels): you should have an account in creodias.es (https://portal.creodias.eu/register.php). The user and password will be add to config.ini of this project.

For NASA product (MODIS, NOAA, SNPP, etc): you should have an account in https://cmr.earthdata.nasa.gov/. The user and password will be add to config.ini of this project.

The project uses ElasticSearch to control the downloaded files and checks whether the point requested exists in a previous file downloaded. It is posible run the download.py with database field empty, but it is necesary to generate the csv and other processes. 

### Json for create elasticsearch index
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

## Files project.json
The scripts are based on project. A project is a json file wherein is defined the parameters to requests the api. Also this file contains the path wherein NC files will be saved and csv files will be generated.

### stations.csv
Free to add field. 
Required "name", "lat", "lon"

## Notes
### log
The scripts use loggin lib to write the results of the process. Also they print important messages (exceptions) in console.
Log files are created in root path using the format: name of satellite (from project.json) + script_name.

## Script definitions


## Running without Elasticsearch
### get data from downloaded files
import esa
file = 'file_nc/zip'
e = esa.ESA()
e.work_path = "path_of_file"
info = e.extract_data(file, lat, lon)



