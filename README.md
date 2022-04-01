# RemoteSensingSoftware
This repo contains remote sensing software developed in SMARTLAGOON project.

## Project Description
This repo contains scripts that allow to download and export to readable CSV format satellite data from NASA y ESA in batches by date and geopoints. 

## Available and tested platforms
Due to the script is parametrise to request the APIs, its can be use to download netCDF4 files from EarthData (NASA) and Sentinel-3 CopernicusHub  (ESA).
However it has been just tested in follows enviroments:
#### NASA
MODIS Aqua, SNPP VIRRS, NOOA VIIRS for OC instrument and Level 2.
#### ESA
Sentinel-3 A y B for OLCI instrument and Level 2.

## Requiriments
### Technical
Scripts have been developed and test using Python3.6 and above on Ubuntu 16-18.
* Use requirements.txt to install library dependencies of scripts. 
* You might to install some other external python dependencies to work with netCDF4 files.
* Script uses an elasticSearch instance to control downloaded files. It checks if requested point-date-satelliteProduct exists in a previous file downloaded.
- It is possible running the satellite_download.py with database field empty, but it is necessary to generate the csv and other processes. 

### Before to start
Public APIs used by the script provide basic authentication process to request, so you must to provide a username and password by using config.ini.

##### ESA 
For Sentinel product you should have an account in creodias.es:
        
        https://portal.creodias.eu/register.php

The user and password will be add to config.ini of this project.
##### NASA 
For NASA product MODIS, NOAA, SNPP: you should have an account in 

        https://cmr.earthdata.nasa.gov/ 

The user and password will be added to config.ini of the script.
Also, you must authorize your user to access "OB.DAAC Data Access" in End-User License Agreement(EULA) in your user profile on earthdata. See: 

     https://urs.earthdata.nasa.gov ,  menu Applications->"authorized apps". 

### ELASTICSEARCH 
#### MAPPING JSON for create  index
if you are able to work using elasticsearch, next describes the mapping to create the index:
       
     {
           "nc_files":{
              "mappings":{
                 "properties":{
                    "agency":{
                       "type":"keyword"
                    },
                    "boundingBox":{
                       "type":"geo_shape"
                    },
                    "cloud":{
                       "type":"float"
                    },
                    "coords":{
                       "type":"float"
                    },
                    "date":{
                       "type":"date",
                       "format":"yyyy-MM-dd"
                    },
                    "file":{
                       "type":"keyword"
                    },
                    "instrument":{
                       "type":"keyword"
                    },
                    "lat":{
                       "type":"float"
                    },
                    "lon":{
                       "type":"float"
                    },
                    "path":{
                       "type":"text",
                       "fields":{
                          "keyword":{
                             "type":"keyword",
                             "ignore_above":256
                          }
                       }
                    },
                    "polygonBox":{
                       "type":"geo_shape"
                    },
                    "satellite":{
                       "type":"keyword"
                    },
                    "time":{
                       "type":"date",
                       "format":"time"
                    },
                    "timeless":{
                       "type":"keyword"
                    },
                    "url":{
                       "type":"text"
                    },
                    "version":{
                       "type":"integer"
                    }
                 }
              }
           }
        }

## Config.ini 
This file contains the basic parameters to request data to APIs. The structure is:

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

Other platforms could be added here.

## Files project.json
The scripts are based on json configuration called project. 
A project is a json file wherein is defined the parameters to requests the api and more such as this file contains the path wherein NC files will be saved and csv files will be generated.
The name of this file is up to user, but the script call have to pass the project.json path as param.

### stations.csv
This file contains a list of point to search. The CSV name is free since it have to be indicated in the project file to load.
Required "name", "lat", "lon".
Free to add more fields for personal proposes. 

## Notes
### logging
The scripts use loggin lib to write the results of the process. Also they print important messages (exceptions) in console.
Log files are created in log/ path using the format: name of satellite (from project.json) + script_name.

## Script definitions


## Running without Elasticsearch
### get data from downloaded files

    
    import esa
    file = 'file_nc/zip'
    e = esa.ESA()
    \#set the path of nc files
    e.work_path = "path_of_file" 
    info = e.extract_data(file, lat, lon)



