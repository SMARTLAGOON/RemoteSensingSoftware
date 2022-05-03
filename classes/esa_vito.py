# coding=utf-8
# Author: Jose Gines Gimenez Manuel. 2021
# Python 3.5
# Descripcion:

# import sys
# sys.path.append("..")

import requests as RQ
import netCDF4 as cd
import os
import numpy as np
import configparser
from database import elasticSearch_adapter as DB
import logging
import time as TIME
import re


class Vito:

    def __init__(self, project=None):
        config = configparser.ConfigParser()
        config.read("config.ini")
        self.document = {}
        self.agency = "ESA"
        self.user = config["SSM"]["user_name"]
        self.password = config["SSM"]["password"]
        self.User_Knows_not_auth = False
        self.search_url = config["SSM"]["url_search"]
        # self.token = None #config["ESA_vito"]["token"]
        # self.token_requested = False #controla en caso de error que no entre en bucle regenerando tokens: crea uno y si falla hay otro problema.
        # self.date_format = config["ESA_vito"]["date_format"]

        if project:
            url_elastic = project['database']
            if url_elastic:
                try:
                    self.database = DB.Database(url_elastic)
                except Exception as e:
                    print("Error conecting to elastic", url_elastic, str(e))
                    exit(-1)
            else:
                self.database = None

            self.instrument = project['instrument']
            self.satellite = project['satellite']
            self.work_path = project['files']
            self.document = {'agency': self.agency, 'satellite': self.satellite, 'instrument': self.instrument,
                             'path': self.work_path}
            logging.info("ESA_vito.py: " + self.instrument + " Loaded ESA_vito Project:")
        else:
            print("no projet")
        logging.info(self.document.__str__())

    def get_data(self, date, lat, lon, download=False):
        # self.download=download
        file = self.get_file(lat, lon, date)
        if file is None:
            return None
        return self.extract_data(file, lat, lon)

    def extract_data(self, file, lat, lon):

        file = self.work_path + "/" + file

        df = cd.Dataset(file, format="NETCDF4")
        print(df.variables['time'][0])
        attr_time = df.variables["time"][0]
        fecha = attr_time * 86400  # time is in days from 1970 (epoch)
        fecha = TIME.strftime("%Y-%m-%d", TIME.localtime(fecha))
        time = "T12:00:00.000Z"
        vars = list(df.variables.keys())
        vars.remove('lat')
        vars.remove('lon')
        vars.remove('time')
        vars.remove('crs')
        lats = df.variables['lat'][:]
        lons = df.variables['lon'][:]
        # Matriz de distancias
        lonidx = (np.abs(lons - lon)).argmin()
        latidx = (np.abs(lats - lat)).argmin()
        latselect = lats[latidx]
        lonselect = lons[lonidx]
        d = np.power(latselect - lat, 2) + np.power(lonselect - lon, 2)
        min_dist = np.sqrt(d)

        data = {"date": fecha, "time": time, "lat": latselect, "lon": lonselect, "dist": min_dist}
        for var in vars:
            values = df.variables[var]
            value = values[0][latidx][lonidx]
            if np.ma.is_masked(value):
                data[var] = np.nan
            else:
                data[var] = value
        return data

    # descarga el archivo correspondiente a la fecha, lat y lon.
    def delete_file(self, lat, lon, date):
        dbQuery = {"agency": self.agency,
                   "date": date,
                   "lat": lat,
                   "lon": lon,
                   "satellite": self.satellite,
                   "instrument": self.instrument
                   }

        if self.database:
            file = self.database.exist_file(dbQuery)
            if file:
                return file

    # En SCGL es toda europa y las coordenadas fijas
    def verify_coords(self, file, lat, lon):
        # this product offers the same range
        return [[[72.0, -11.0], [72.0, 50.0], [35.0, 50.0], [35.0, -11.0], [72.0, -11.0]]]

    # borra la imagen de disco.
    # Deberia comprabar si esta imagen ha sido descargada para otro punto pero se hace antes de llamar a este proceso
    def remove_HD_file(self, file):
        os.system("rm " + file)

    # descarga el archivo correspondiente a la fecha, lat y lon.
    def get_file(self, lat, lon, date):
        dbQuery = {"agency": self.agency,
                   "date": date,
                   "lat": lat,
                   "lon": lon,
                   "satellite": self.satellite,
                   "instrument": self.instrument
                   }

        if self.database:
            file = self.database.exist_file(dbQuery)
            if file:
                return file
        if not self.download:
            return None
        date_split = date.split("-")
        url = self.search_url.replace("$year", date_split[0]).replace("$month", date_split[1]).replace("$day",
                                                                                                       date_split[2])
        file = url.split("/")[-1]  # nombre del fichero
        if os.path.exists(self.work_path + "/" + file):
            # el doc existe pero no en la base de datos...QUE HACER?
            return file
        r = RQ.get(url, auth=RQ.auth.HTTPBasicAuth(self.user, self.password), allow_redirects=True)
        if r.headers.get('content-disposition'):
            try:
                file = re.findall('filename=(.+)', r.headers.get('content-disposition'))[0]
            except Exception as e:
                print(e)
                return None
            open(self.work_path + "/" + file, 'wb').write(r.content)
        else:
            logging.info("ESA_vito.py: " + self.instrument + " WARNING! " + r.text)
            logging.info("ESA_vito.py: " + self.instrument + " ----> " + self.search_url)
            return None

        if file:
            coords = self.verify_coords(self.work_path + "/" + file, lat, lon)
            # Si el punto no esta dentro  del poliogono se descarta
            if coords is None:
                self.remove_HD_file(file)
                # las coordenadas pedidas no están.
                return None

            polygon = {}
            polygon['type'] = "polygon"
            polygon['coordinates'] = coords

            self.document['date'] = date
            self.document['time'] = "12:00:00.000Z"
            self.document['url'] = url
            self.document['file'] = file
            self.document['boundingBox'] = polygon
            if self.database:
                self.database.add_file(self.document)

            return self.document['path'] + "/" + self.document['file']
        else:
            return None

    def download_file(self, url, file):
        logging.info("ESA_vito.py: " + self.instrument + " DEBUG: Downloading file " + url)  # self.document['url'])
        wget = 'wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --content-disposition '
        # wget + self.document['url'] + " -P " + self.document['path'] + " -O " + self.document['file']
        os.system(wget + url + " -O " + self.document['path'] + "/" + file)

    # deprecated in this method o quiza formar la url, para que tengan los mismos metodos todas las clases???
    def get_url_file(self, point, fecha):

        headers = {'Authorization': 'Bearer ' + self.token}
        umms = []
        date = fecha.split("-")
        url = self.search_url.replace("$year", date[0]).replace("$month", date[1]).replace("$day", date[2])
        print(url)
        r = RQ.get(url, allow_redirects=True)
        if r.headers.get('content-disposition'):
            file = re.findall('filename=(.+)', cd)
            open(self.work_path + "/" + file, 'wb').write(r.content)
            return file
        else:
            logging.info("ESA_vito.py: " + self.instrument + " WARNING! " + r.text)
            logging.info("ESA_vito.py: " + self.instrument + " ----> " + self.search_url)
            return None

    def set_cookies(self):
        gen_cookies = 'echo "machine urs.earthdata.ESA_vito.gov login ' + self.user + ' password ' + self.password + '" > ~/.netrc; > ~/.urs_cookies'
        os.system(gen_cookies)

    def get_database(self):
        return self.database

    def get_document(self):
        return self.document
