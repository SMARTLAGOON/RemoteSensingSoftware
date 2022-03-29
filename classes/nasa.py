# Author: Jose Gines Gimenez Manuel. 2021
# Python 3.5
# Descripcion:

#
import requests as RQ
import netCDF4 as cd
import os
import numpy as np
from numpy import ravel
import datetime
import configparser
import database.elasticSearch_adapter as DB
import logging
import base64
from shapely.geometry import Polygon, Point

class NASA:

    def __init__(self,  project=None):
        config = configparser.ConfigParser()
        config.read("config.ini")
        self.agency = "NASA"
        self.user = config["NASA"]["user_name"]
        self.password = config["NASA"]["password"]
        self.User_Knows_not_auth = False
        self.search_url = config["NASA"]["url_search"]
        self.token = None #config["NASA"]["token"]
        self.token_requested = False #controla en caso de error que no entre en bucle regenerando tokens: crea uno y si falla hay otro problema.
        self.date_format = config["NASA"]["date_format"]
        self.document = {}

        if project:
            print(project)
            url_elastic = project['database']
            if url_elastic:
                try:
                    self.database = DB.Database(url_elastic)
                except Exception as e:
                    print("Error conecting to elastic", url_elastic, str(e))
                    exit(-1)
            else:
                self.database = None

            # Producttype & processingLevel are unecesary, they are used to save at database and keep the structure
            # NASA product based the search in API using just instrument parameter that does not have a general form to
            # make it automatic.
            self.instrument = project['instrument']
            extract_info = self.instrument.split("_")
            try:
                self.processing_level = project['processing_level']
            except:
                try:
                    self.processing_level = extract_info[1]
                except:
                    self.processing_level = ''
            try:
                self.product_type = project['product_type']
            except:
                try:
                    self.product_type = extract_info[1]
                except:
                    self.product_type = ''

            self.instrument = project['instrument']
            self.satellite = project['satellite']
            self.work_path = project['files']
            self.document = {'agency': self.agency,'satellite': self.satellite, 'instrument': self.instrument, 'path':  self.work_path, 'product_type':self.product_type, 'processing_level':self.processing_level }
            self.check_token()
            self.set_cookies()
            logging.info("NASA.py: " + self.instrument + " Loaded NASA Project:")
            logging.info(self.document.__str__())

        else:
            print("No project loaded")
            self.work_path = "."

        self.download = True

    def check_token(self):
        url = "https://urs.earthdata.nasa.gov/api/users/tokens"
        credentials = self.user + ":" + self.password
        headers = {"Authorization" : "Basic " + credentials}
        response = RQ.get(url, headers=headers)

        if response.status_code != 200:
            logging.info("NASA.py: " + self.instrument + " ERROR: Current user cannot retrieve token. Check credentials user password in config.ini")
            exit(-1)
        tokens = response.json()
        if len(tokens) == 0:
            self.request_token()
        else:
            for t in tokens:
                token_date = datetime.datetime.strptime(t['expiration_date'], "%m/%d/%Y")
                if (not self.token) & (datetime.datetime.now() < token_date):
                    self.token = t['access_token']
                elif datetime.datetime.now() >= token_date:
                    self.revoke_token()
            if (not self.token):
                  self.request_token()


    def request_token(self):
        url = "https://urs.earthdata.nasa.gov/api/users/token"
        credentials = self.user + ":" + self.password
        credentials = credentials.encode('ascii')
        credentials = base64.b64encode(credentials)
        credentials = credentials.decode('ascii')
        headers = {"Authorization": "Basic " + credentials}
        response = RQ.post(url, headers=headers)

        if response.status_code != 200:
            logging.info("NASA.py: " + self.instrument + " DEBUG: No token generated")
            logging.info("NASA.py: " + self.instrument + " ----> " + response.status_code)
            print(response.status_code)
            print(response.content)
            exit(-1)

        token = response.json()
        self.token = token['access_token']
        self.token_requested = True
        return

    def revoke_token(self, token):
        url = "https://urs.earthdata.nasa.gov/api/users/revoke_token"
        headers = {"Authorization": "Basic " + self.user + ":" + self.password}
        data = {'token': token}
        response = RQ.post(url, data=data, headers=headers)
        if response.status_code == 200:
            logging.info("NASA.py: " + self.instrument + " DEBUG: Token deleted " + token[10] + "...")
        return

    def get_data(self, date, lat, lon, download=False):
        self.download=download
        file = self.get_file(lat, lon, date)
        if file is None:
             return None
        return self.extract_data(file, lat, lon)

    def extract_data(self, file, lat, lon):
        file = self.work_path + "/" + file
        df = cd.Dataset(file, format="NETCDF4")
        df.groups['geophysical_data'].variables.keys()
        fecha = df.getncattr("time_coverage_start").split('T')[0]
        time = df.getncattr("time_coverage_start").split('T')[1]
        gf_data = df.groups['geophysical_data'].variables
        lats = df.groups['navigation_data'].variables['latitude'][:]
        lons = df.groups['navigation_data'].variables['longitude'][:]
        # Matriz de distancias
        lons_d = lons - lon
        lats_d = lats - lat
        lons_d = np.power(lons_d, 2)
        lats_d = np.power(lats_d, 2)
        dis_sq = lons_d + lats_d
        dist = np.sqrt(dis_sq)
        # MINIMO TOTAL
        min_dist = ravel(dist)[np.argmin(dist)]
        punto = np.where(dist == min_dist)
        idx = punto[0][0]
        idy = punto[1][0]
        data = {"date":fecha, "time":time, "lat": lats[idx][idy], "lon": lons[idx][idy], "dist":min_dist}
        for var in gf_data:
            values = df.groups['geophysical_data'].variables[var]
            value = values[idx][idy]
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

    # Comprueba en un nc si la lat y lon buscada están dentro de la imagen.
    def verify_coords(self, file, lat, lon):
        df = cd.Dataset(file, format="NETCDF4")
        lats = df.groups['navigation_data'].variables['latitude'][:]
        lons = df.groups['navigation_data'].variables['longitude'][:]

        rows = len(lats) - 1
        cols = len(lats[0]) - 1
        poly = []
        primero = [lons[rows][0], lats[rows][0]]
        # Empieza
        # poly.append(primero)
        # print("Primero", primero)
        # parte de arriba
        CADA = 500
        for x in range(0, cols, CADA):
            poly.append([lons[rows][x], lats[rows][x]])
        # lado der
        for x in range(rows, 0, 0 - CADA):
            poly.append([lons[x][cols], lats[x][cols]])
        # inferior
        for x in range(cols, 0, 0 - CADA):
            poly.append([lons[0][x], lats[0][x]])
        # izquiero
        for x in range(1, rows, CADA):
            poly.append([lons[x][0], lats[x][10]])
        df.close()
        poly.append(primero)
        Punto = Point(lon, lat)
        Poligono = Polygon(poly)
        if Poligono.contains(Punto):
            return [poly]
        else:
            self.remove_HD_file(file)
            return None
    #borra la imagen de disco.
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

        umms = self.get_url_file((lat, lon), date)
        if umms is None:
            logging.info("NASA.py: " + self.instrument + " DEBUG: Not file at database nor EarthdAta for " + date  + ", " + str(lat) + "," + str(lon))
            return None

        image_to_select = None

        if len(umms) > 0:
            for image in umms:
                url = image['RelatedUrls'][0]['URL']
                file = image['GranuleUR']
                #la imagen existe pero es usada por otro punto. Si ha llegado aqui efectivamente es porque en elastics el punto no cae dentro del poligono
                if os.path.exists(self.work_path + "/" + file):
                    print("La imagen existe en ", self.work_path)
                    continue #la imagen  de la API está pero no en realidad no contiene este punto.
                #si no existe se descarga y verifica
                self.download_file(url, file)
                coords = self.verify_coords(self.work_path + "/" + file, lat, lon)
                #Si el punto no esta dentro  del poliogono se descarta
                if coords is None:
                    self.remove_HD_file(file)
                    continue
                else:
                    image_to_select = image
                    polygon ={}
                    polygon['type'] = "polygon"
                    polygon['coordinates'] = coords
                #es la correcta
                    break

            if image_to_select is None:
                logging.info(
                    "NASA.py: " + self.instrument + " DEBUG: Not file at database nor EarthdAta for " + date + ", " + str(
                        lat) + "," + str(lon))
                return None

            datetimeStr = image_to_select["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]
            try:
                dateTime = datetime.datetime.strptime(datetimeStr, self.date_format)
            except:
                dateTime = datetime.datetime.strptime(datetimeStr, "%Y-%m-%dT%H:%M:%SZ")


            date = dateTime.date().strftime("%Y-%m-%d")
            time = dateTime.time().strftime("%H:%M:%S") + '.000Z'
            self.document['date'] = date
            self.document['time'] = time
            self.document['url']= image_to_select['RelatedUrls'][0]['URL']
            self.document['file'] = image_to_select['GranuleUR']

            self.document['boundingBox']= polygon
            if self.database:
                self.database.add_file(self.document)

            return self.document['path'] + "/" + self.document['file']
        else:
            return None

    def download_file(self, url, file):
        logging.info("NASA.py: " + self.instrument + " DEBUG: Downloading file " +  url)#self.document['url'])
        wget = 'wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --content-disposition '
        # wget + self.document['url'] + " -P " + self.document['path'] + " -O " + self.document['file']
        os.system(wget + url + " -O " + self.document['path'] + "/" + file)

    def get_url_file(self, point, fecha):
        fini = fecha + "T00:00:00"
        ffin = fecha + "T23:59:59"

        point_url = str(point[1]) + "," + str(point[0]) #lon, lat
        temporal = fini + "," + ffin

        params = {
                  "page_size": 7,
                  "sort_key": "start_date",
                  "short_name": self.instrument,
                  "provider": "OB_DAAC",
                  "point": point_url,
                  "temporal": temporal
        }

        headers = {'Authorization': 'Bearer ' + self.token}
        umms = []
        r = RQ.get(self.search_url, params=params, headers=headers) # auth=("Bearer", self.token))
        if r.status_code == 200:
            data = r.json()
            if len(data['items']) > 0:
                for item in data['items']:
                    umm = item['umm']
                    umms.append(umm)
            return umms
        elif r.status_code == 403:
            if self.User_Knows_not_auth:
                return None
            logging.info("NASA.py: " + self.instrument + " ERROR: Error 403: Unauthorized. Check config.ini Token value, or generate a new one in https://urs.earthdata.nasa.gov/")
            exit(-1)
            # este checkeo se deja para otro momento donde se ejecute manualmente
            var = input("Do you want to continue without download files? [Y=yes, otherwise=no] : ")
            if var == 'Y':
                self.User_Knows_not_auth = True
                return None
            else:
                exit(-1)
        elif r.status_code == 401:
            if self.token_requested:
                logging.info("NASA.py: " + self.instrument + " ERROR connection: check network and try later")
                logging.info("NASA.py: " + self.instrument + " ----->" + self.search_url  +  params)
                logging.info("NASA.py: " + self.instrument + " ----->" + r.text)
                exit(-1)
            # Recursividad 1 vez.. controlar!
            if ("Token" in r.text) & ("expired" in r.text):
                self.revoke_token(self.token)
                self.request_token()
                return self.get_url_file(point, fecha)
        else:

            logging.info("NASA.py: " + self.instrument + " WARNING! " + r.text)
            logging.info("NASA.py: " + self.instrument + " ----> " + self.search_url)
            return None

    def set_cookies(self):
        gen_cookies = 'echo "machine urs.earthdata.nasa.gov login ' + self.user + ' password ' + self.password + '" > ~/.netrc; > ~/.urs_cookies'
        os.system(gen_cookies)

    def get_database(self):
        return self.database

    def get_document(self):
        return self.document
