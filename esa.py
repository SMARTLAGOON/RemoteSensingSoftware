import glob
import traceback
import zipfile

import requests as RQ
import netCDF4 as cd
import os
import numpy as np
from numpy import ravel
import datetime
import time
from shapely.geometry import Polygon
import configparser
import database.elasticSearch_adapter as DB
import json
import logging

class ESA:

    ZIP_NAME = "ESA.zip" #para descargar, extraer y eliminar el zip

    #def __init__(self, satellite, instrument, work_path, url_elastic):
    def __init__(self, project=None):
        config = configparser.ConfigParser()
        config.read("config.ini")
        self.agency = "ESA"
        self.user = config["ESA"]["user_name"]
        self.password = config["ESA"]["password"]
        self.User_Knows_not_auth = False
        self.search_url = config["ESA"]["url_search"]
        self.auth_url = config["ESA"]["url_auth"]
        self.token = None
        self.expired_token_timestamp = None
        self.date_format = config["ESA"]["date_format"]
        self.document = {}
        self.project = project
        if project:
            url_elastic = project['database']
            if url_elastic:
                try:
                    self.database = DB.Database(url_elastic)
                except Exception as e:
                    print("Error conecting to elastic", url_elastic)
                    exit(-1)
            else:
                self.database = None

            self.work_path = project['files']
            self.document = {'agency': project['agency'], 'satellite': project['satellite'], 'instrument': project['instrument'], 'path': self.work_path}
            self.download = True # permite la descarga de imagenes, usado a false para generar los csv de los archivos descargados.
            logging.info("ESA.py: ESA Project:")
            logging.info(self.document)
        else:
            print("No project loaded")
            self.work_path = ""

    # Invocado cuando se va a atacar la API
    def create_token(self):
        if self.User_Knows_not_auth:
            return True

        if self.token:
            if int(datetime.datetime.now().timestamp()) < self.expired_token_timestamp:
                logging.info(datetime.datetime.now(), self.expired_token_timestamp)
                return True

        getdata = "client_id=CLOUDFERRO_PUBLIC&username="+ self.user + "&password="+ self.password + "&grant_type=password"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        r = RQ.post(self.auth_url, data=getdata, headers=headers)
        if r.status_code == 200:
            data = r.json()
            now = int(datetime.datetime.now().timestamp()) # cogemos los segundos, la parte entera.
            self.token = data['access_token']
            self.expired_token_timestamp = now + data["expires_in"]
            return True
        else:
            logging.info("ESA.py: Error:" +  str(r.status_code) + " No token generated.")
            logging.info("ESA.py: ---> " + self.auth_url + "Params" + str(getdata) + " headers:" +  str(headers))
            exit(-1)
            #por ahora el codigo se queda sin interaccionar con el usuario
            var = input("Do you want to continue without download files? [Y=yes, otherwise=no] : ")
            if var == 'Y':
                self.User_Knows_not_auth = True
                return True
            else:
                exit(-1)
            logging.info("ESA.py: Error: " + r.text)
            exit(-1)

        # descarga el archivo correspondiente a la fecha, lat y lon.

    def get_file(self, lat, lon, date):

        dbquery = {"agency": self.project['agency'],
                 "date": date,
                 "lat": lat,
                 "lon": lon,
                 "satellite": self.project['satellite'],
                 "instrument": self.project['instrument']
                 }

        if self.database:
            file = self.database.exist_file(dbquery)
            if file:
                logging.info("ESA.py: DEBUG: file finded at DB for " + str(lat) + str(lon) + date)
                return file

        if not self.download:
            return None

        items = self.get_url_file((lat, lon), date)
        if items is None or len(items)==0:
            return None

        # en teoria por dia solo hay una imagen para la query. Al ordenar por NTC se coge la primera.
        # for item in items:
        try:
            item = items[0]
            url = item['properties']['services']['download']['url']
            self.document['file'] = item['properties']['title']

            startDate = item['properties']['startDate']
            try:
                dateTime = datetime.datetime.strptime(startDate, self.date_format)
            except:
                #algunos archivos no llevan milisegundos.
                dateTime = datetime.datetime.strptime(startDate, "%Y-%m-%dT%H:%M:%SZ")

            date = dateTime.date().strftime("%Y-%m-%d")
            time = dateTime.time().strftime("%H:%M:%S")
            self.document['date'] = date
            self.document['time'] = time
            self.document['url'] = url
            coords = item["geometry"]["coordinates"][0]
            self.document['coords'] = coords

            self.download_file(url)
            if self.database:
                self.database.add_file(self.document)

            return self.document['path'] + "/" + self.document['file']

        except Exception as ex:
            logging.info("ESA.py: ERROR: recovering file " +  ex.with_traceback())
            traceback.logging.info_exc()
            logging.info()
            return None

    #Llama a la API de CREOIDAS
    def get_url_file(self, point, fecha):
        fini = fecha + "T00:00:00"
        ffin = fecha + "T23:59:59"

        point_url = str(point[1]) + " " + str(point[0])  # lon, lat
        temporal = fini + "," + ffin
        params = {
            "maxRecords": 5,
            "processingLevel": self.project['processingLevel'],
            "instrument": self.project['instrument'], #SL
            "productType": self.project['productType'], #"OLCI", #LST
            "platform": self.project['satellite'],

            "geometry": "POINT(" + point_url + ")", #lon, lat
            # "sortParam":"timeliness", #Non-time-critical (NTC aparece el primero, son imagenes procesadas,
            #Minetras que STC RTC, slowtime y realtime, no están procesadas.
            # "sortOrder":"descending",
            # "status": "all",
            "dataset": self.project['agency'],
            "startDate": fini,
            "completionDate": ffin
        }

        # if self.project['timeliness']:
        #     params["timeliness"] = self.project['timeliness']
       # logging.info(str(point[0]) , self.search_url, params)
        response = RQ.get(self.search_url, params=params)
        if response.status_code == 200:
            items = []
            try:
                data = response.json()
                for element in data['features']:
                    items.append(element)
            except Exception as e:
                logging.info("ESA.py: WARNING: Reading " + self.search_url + ", Params:" + params)
                logging.infi("---->" + response.text)
                logging.info("ESA.py: ---->" + e)
                return None


            # crear array de files ulr
            return items
        else:
            logging.info("ESA.py: ERROR: API CREOIDAS: " + response.text)
            # exit(-1)
            return None


    def download_file(self, url):
        # Creamos el token solo en caso de necesitar descargar
        if not self.create_token():
            exit()

        url = url + "?token=" + self.token

        os.system("wget " + url + " -O " +  self.work_path + "/" + ESA.ZIP_NAME)
        with zipfile.ZipFile(self.work_path  + '/' +  ESA.ZIP_NAME, 'r') as zip_ref:
            zip_ref.extractall(self.work_path)
        os.remove(self.work_path  + '/' +  ESA.ZIP_NAME)

    def get_weather_point(self, lat, lon):
        try:
            dir = self.work_path + "/" + self.document['file']
            coord = cd.Dataset(dir + "/tie_geo_coordinates.nc", format='NETCDF4')
            lats = coord.variables["latitude"][:]
            lons = coord.variables["longitude"][:]

            lons_d = lons - lon
            lats_d = lats - lat
            lons_d = np.power(lons_d, 2)
            lats_d = np.power(lats_d, 2)
            dis_sq = lons_d + lats_d
            dist = np.sqrt(dis_sq)
            distancia = np.ravel(dist)[np.argmin(dist)]
            d = np.argmin(dist)
            punto = np.where(dist == np.ravel(dist)[d])
            idx = punto[0][0]
            idy = punto[1][0]

            return {"lat": coord.variables["latitude"][idx][idy], "lon": coord.variables["longitude"][idx][idy], "idx": idx,
                    "idy": idy, "dist": distancia}
        except:
            return None

    def get_closest_point(self, lat, lon):
        try:
            dir = self.work_path + "/" + self.document['file']
            coord = cd.Dataset(dir + "/geo_coordinates.nc", format='NETCDF4')
            lats = coord.variables["latitude"][:]
            lons = coord.variables["longitude"][:]

            dateTime = getattr(coord, 'start_time')
            dateTime = datetime.datetime.strptime(dateTime, "%Y-%m-%dT%H:%M:%S.%fZ")
            date = dateTime.date()
            time = dateTime.time()
            lons_d = lons - lon
            lats_d = lats - lat
            lons_d = np.power(lons_d, 2)
            lats_d = np.power(lats_d, 2)
            dis_sq = lons_d + lats_d
            dist = np.sqrt(dis_sq)
            distancia = np.ravel(dist)[np.argmin(dist)]
            d = np.argmin(dist)
            punto = np.where(dist == np.ravel(dist)[d])
            idx = punto[0][0]
            idy = punto[1][0]

            return {"date": date, "time": time, "lat": coord.variables["latitude"][idx][idy], "lon": coord.variables["longitude"][idx][idy], "idx": idx,
                    "idy": idy, "dist": distancia}
        except Exception as e:
            logging.info("ESA.py: DEBUG: PUNTO CERCANO ERROR" + str(e))
            print(str(e))
            return None

    def get_clorofila(self, idx, idy):
        # dirs = glob.glob(self.work_path + "/" + self.document["file"])
        dir =  self.work_path + "/" + self.document["file"]
        chl = cd.Dataset(dir + "/chl_nn.nc")
        chl_nn = chl.variables["CHL_NN"][idx][idy]
        chl_nn_err = chl.variables["CHL_NN_err"][idx][idy]

        chl = cd.Dataset(dir + "/chl_oc4me.nc")
        chl_oc = chl.variables["CHL_OC4ME"][idx][idy]
        chl_oc_err = chl.variables["CHL_OC4ME_err"][idx][idy]

        return {"chl_nn": chl_nn, 'chl_nn_err': chl_nn_err, 'chl_oc': chl_oc, 'chl_oc_err': chl_oc_err}

    def get_reflectancias(self, idx, idy):
        dir = self.work_path + "/" + self.document["file"]
        reflectances = glob.glob(dir + "/*_reflectance.nc")
        reflectancias = {}
        for rrs in reflectances:
            reflectance = cd.Dataset(rrs)
            var = rrs.split("/")
            var = var[len(var) - 1]
            var = var.split(".")[0]
            err = var + "_err"
            reflectancias[var] = reflectance.variables[var][idx][idy]
            reflectancias[err] = reflectance.variables[err][idx][idy]

        return reflectancias

    #Solidos en suspension
    def get_tsm(self, idx, idy):
        dir = self.work_path + "/" + self.document["file"]
        tsm = cd.Dataset(dir + "/tsm_nn.nc")
        tsm_nn = tsm.variables["TSM_NN"][idx][idy]
        tsm_nn_err = tsm.variables["TSM_NN_err"][idx][idy]

        return {"tsm_nn": tsm_nn, 'tsm_nn_err': tsm_nn_err}

    #Turbidez
    def get_TRSP(self, idx, idy):
        dir = self.work_path + "/" +self.document["file"]
        tsm = cd.Dataset(dir + "/trsp.nc")
        kd490 = tsm.variables["KD490_M07"][idx][idy]
        kd490_err = tsm.variables["KD490_M07_err"][idx][idy]

        return {"kd490": kd490, 'kd490_err': kd490_err}

    #Nubosidad
    def get_iwv(self, idx, idy):
        dir = self.work_path + "/" +self.document["file"]
        iwv_data = cd.Dataset(dir + "/iwv.nc")
        iwv = iwv_data.variables["IWV"][idx][idy]
        iwv_err = iwv_data.variables["IWV_err"][idx][idy]

        return {"iwv": iwv, 'iwv_er': iwv_err}

    #tiempo
    def get_weather(self, lat, lon):
        dir = self.work_path + "/" +self.document["file"]

        point = self.get_weather_point(lat, lon)
        weather = cd.Dataset(dir + "/tie_meteo.nc")

        idx = point['idx']
        idy = point['idy']

        wind_vector = weather.variables["horizontal_wind"][idx][idy]

        arctg = np.arctan(wind_vector[0]/ wind_vector[1])
        if wind_vector[1] > 0:
            arctg += 180
        elif (wind_vector[1] > 0) & (wind_vector[1] < 0):
            arctg += 180
        else:
            pass
        wind_direcction = arctg
        wind_velocity = np.sqrt(np.power(wind_vector[0],2) + np.power(wind_vector[1],2))

        pressure = weather.variables["sea_level_pressure"][idx][idy]
        ozone = weather.variables["total_ozone"][idx][idy]
        humidity = weather.variables["humidity"][idx][idy]
        pressure_ref = np.nan # weather.variables["reference_pressure_level"][idx][idy]
        temperature = weather.variables["atmospheric_temperature_profile"][idx][idy][0]
        vapour = weather.variables["total_columnar_water_vapour"][idx][idy]

        return {"wind_direcction": wind_direcction, 'wind_velocity':wind_velocity, 'pressure': pressure,"ozone": ozone,"humidity": humidity,
                "pressure_ref": pressure_ref,"temperature": temperature,"vapour": vapour}

    def extract_data(self, file, lat, lon):
        self.document['file'] = file
        punto = self.get_closest_point(lat, lon)
        if punto is None:
            logging.info("ESA.py: DEBUG: Extract data: PUNTO is not in FILE")
            print("CAGO EN DUOS")
            return None

        chloro = self.get_clorofila(punto['idx'], punto['idy'])
        reflectancias = self.get_reflectancias(punto['idx'], punto['idy'])
        tsm = self.get_tsm(punto['idx'], punto['idy'])
        trsp = self.get_TRSP(punto['idx'], punto['idy'])
        iwv = self.get_iwv(punto['idx'], punto['idy'])
        weather = self.get_weather(lat, lon)

        del punto['idx']
        del punto['idy']
        row = {**punto, **chloro, **reflectancias, **tsm, **trsp, **iwv, **weather}
        return row

    def get_data(self, fecha, lat, lon, download=False):
        self.download = download
        file = self.get_file(lat, lon, fecha)
        logging.info("ESA.py: DEBUG: File to read" + file)
        return self.extract_data(file, lat, lon)