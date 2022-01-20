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
import database as DB
import json

class ESA:

    ZIP_NAME = "ESA.zip" #para descargar, extraer y eliminar el zip

    def __init__(self, satellite, instrument, work_path, database_file):
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
        self.database = DB.Database(work_path + "/" + database_file)
        self.instrument = instrument
        self.satellite = satellite
        self.work_path = work_path
        self.document = {'agency': self.agency, 'satellite': satellite, 'instrument': instrument, 'path': work_path}


        print("ESA Project:")
        print(self.document)

    # Invocado cuando se va a atacar la API
    def create_token(self):
        if self.User_Knows_not_auth:
            return True

        if self.token:
            if int(datetime.datetime.now().timestamp()) < self.expired_token_timestamp:
                print(datetime.datetime.now(), self.expired_token_timestamp)
                return True

        getdata = "client_id=CLOUDFERRO_PUBLIC&username="+ self.user + "&password="+ self.password + "&grant_type=password"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        url = "https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token"
        r = RQ.post(self.auth_url, data=getdata, headers=headers)
        if r.status_code == 200:
            data = r.json()
            now = int(datetime.datetime.now().timestamp()) # cogemos los segundos, la parte entera.
            self.token = data['access_token']
            self.expired_token_timestamp = now + data["expires_in"]
            return True
        else:
            print(self.auth_url, getdata, headers)
            print("Error:",  r.status_code, " No token generated." )
            var = input("Do you want to continue without download files? [Y=yes, otherwise=no] : ")
            if var == 'Y':
                self.User_Knows_not_auth = True
                return True
            else:
                exit(-1)
            print(r.content)
            exit()

        # descarga el archivo correspondiente a la fecha, lat y lon.

    def get_file(self, lat, lon, date):

        dbquery = {"agency": self.agency,
                 "date": date,
                 "lat": lat,
                 "lon": lon,
                 "satellite": self.satellite,
                 "instrument": self.instrument
                 }

        file = self.database.exist_file(dbquery)
        if file:
            print("File finded at DB for", lat, lon, date)
            return file

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
            # to_excel(est, data, platformas[i])
            self.document['date'] = date
            self.document['time'] = time
            self.document['url'] = url
            coords = item["geometry"]["coordinates"][0]
            polygon = Polygon(coords)
            bounds = polygon.bounds #(minlon, minlat, maxlon, maxlat)

            self.document['North'] = bounds[3]
            self.document['South'] = bounds[1]
            self.document['West'] = bounds[0]
            self.document['East'] = bounds[2]
            self.document['coords'] = coords

            self.download_file(url)
            self.database.add_file(self.document)

            return self.document['path'] + "/" + self.document['file']

        except Exception as ex:
            print()
            print("Error recovering file", ex)
            traceback.print_exc()
            print()
            return None

    #Llama a la API de CREOI
    def get_url_file(self, point, fecha):
        fini = fecha + "T00:00:00"
        ffin = fecha + "T23:59:59"

        point_url = str(point[1]) + "," + str(point[0])  # lon, lat
        temporal = fini + "," + ffin
        params = {
            "maxRecords": 5,
            "processingLevel": "LEVEL2",
            "instrument": self.instrument,
            "productType": "WFR",
            "platform":self.satellite,
            "geometry": "POINT(" + str(point[1]) + " " + str(point[0]) + ")",
            "sortParam":"timeliness", #Non-time-critical (NTC aparece el primero, son imagenes procesadas,
            #Minetras que STC RTC, slowtime y realtime, no están procesadas.
            "sortOrder":"descending",
            "status": "all",
            "dataset": self.agency,
            "startDate": fini,
            "completionDate": ffin
        }
        # print(str(point[0]) , self.search_url, params)
        response = RQ.get(self.search_url, params=params)
        if response.status_code == 200:
            items = []
            try:
                data = response.json()
                for element in data['features']:
                    items.append(element)
            except Exception as e:
                print(e)
                exit()
                return None
            # crear array de files ulr
            return items
        else:
            print("ERROR LEYENDO CREODIAS API", response)
            exit(-1)
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

    def get_closest_point(self, lat, lon):
        dir = self.document['file']
        coord = cd.Dataset(dir + "/geo_coordinates.nc", format='NETCDF4')
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

    def get_clorofila(self, idx, idy):
        # dirs = glob.glob(self.work_path + "/" + self.document["file"])
        dir =  self.document["file"]
        chl = cd.Dataset(dir + "/chl_nn.nc")
        chl_nn = chl.variables["CHL_NN"][idx][idy]
        chl_nn_err = chl.variables["CHL_NN_err"][idx][idy]

        chl = cd.Dataset(dir + "/chl_oc4me.nc")
        chl_oc = chl.variables["CHL_OC4ME"][idx][idy]
        chl_oc_err = chl.variables["CHL_OC4ME_err"][idx][idy]

        return {"chl_nn": chl_nn, 'chl_nn_err': chl_nn_err, 'chl_oc': chl_oc, 'chl_oc_err': chl_oc_err}

    def get_reflectancias(self, idx, idy):
        reflectances = glob.glob(self.document["file"] + "/*_reflectance.nc")
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


    def extract_data(self, file, lat, lon):
        self.document['file'] = file
        punto = self.get_closest_point(lat, lon)
        data = self.get_clorofila(punto['idx'], punto['idy'])
        reflectancias = self.get_reflectancias(punto['idx'], punto['idy'])
        del punto['idx']
        del punto['idy']
        row = {**punto, **data, **reflectancias}
        return row

    def get_data(self, fecha, lat, lon):
        file = self.get_file(lat, lon, fecha)
        return self.extract_data(file, lat, lon)