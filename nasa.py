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
import database as DB


class NASA:

    def __init__(self, satellite, instrument, work_path, database_file):
        config = configparser.ConfigParser()
        config.read("config.ini")
        self.agency = "NASA"
        self.user = config["NASA"]["user_name"]
        self.password = config["NASA"]["password"]
        self.User_Knows_not_auth = False
        self.search_url = config["NASA"]["url_search"]
        self.token = config["NASA"]["token"]
        self.date_format = config["NASA"]["date_format"]
        self.database = DB.Database(work_path + "/" + database_file)
        self.instrument = instrument
        self.satellite = satellite
        self.work_path = work_path
        self.document = {'agency': self.agency,'satellite': satellite, 'instrument': instrument, 'path': work_path}

        self.set_cookies()
        print("NASA Project:")
        print(self.document)

    def get_data(self, date, lat, lon):
        file = self.get_file(lat, lon, date)
        if file is None:
             return None
        return self.extract_data(file, lat, lon)

    def extract_data(self, file, lat, lon):
        # try:

        df = cd.Dataset(file, format="NETCDF4")
        # except:
        #     print("ERROR abriendo " + file)
        #     return None
        df.groups['geophysical_data'].variables.keys()
        fecha = df.getncattr("time_coverage_start").split('T')[0]
        time = df.getncattr("time_coverage_start").split('T')[1]
        # chlor_a = df.groups['geophysical_data'].variables['chlor_a']
        # chl_ocx = df.groups['geophysical_data'].variables['chl_ocx']
        gf_data = df.groups['geophysical_data'].variables

        # Rrs_443 =d .groups['geophysical_data'].variables['Rrs_443']
        lats = df.groups['navigation_data'].variables['latitude'][:]
        lons = df.groups['navigation_data'].variables['longitude'][:]

        # Matriz de distancias
        lons_d = lons - lon
        lats_d = lats - lat
        lons_d = np.power(lons_d, 2)
        lats_d = np.power(lats_d, 2)
        dis_sq = lons_d + lats_d
        dist = np.sqrt(dis_sq)
        # distancia = ravel(dist)[np.argmin(dist)]
        # coger minimas distancia e indice
        # sorted = ravel(dist).argsort()

        # MINIMO TOTAL
        min_dist = ravel(dist)[np.argmin(dist)]
        punto = np.where(dist == min_dist)

        idx = punto[0][0]
        idy = punto[1][0]
        data = {"date":fecha, "time":time}

        for var in gf_data:
            values = df.groups['geophysical_data'].variables[var]
            value = values[idx][idy]
            data[var] = value

        return data


    # descarga el archivo correspondiente a la fecha, lat y lon.
    def get_file(self, lat, lon, date):

        dbQuery = {"agency": self.agency,
                "date": date,
                "lat": lat,
                "lon": lon,
                "satellite": self.satellite,
                "instrument": self.instrument
                }

        file = self.database.exist_file(dbQuery)
        if file:
            return file

        umms = self.get_url_file((lat, lon), date)
        if umms is None:
            return None
        if len(umms) > 0:
            datetimeStr = umms[0]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"]

            try:
                dateTime = datetime.datetime.strptime(datetimeStr, self.date_format)
            except:
                dateTime = datetime.datetime.strptime(datetimeStr, "%Y-%m-%dT%H:%M:%SZ")

            date = dateTime.date().strftime("%Y-%m-%d")
            time = dateTime.time().strftime("%H:%M:%S")
            self.document['date'] = date
            self.document['time'] = time
            self.document['url']= umms[0]['RelatedUrls'][0]['URL']
            self.document['file'] = umms[0]['GranuleUR']
            coords = umms[0]["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["BoundingRectangles"][0]
            self.document['North'] = coords['NorthBoundingCoordinate']
            self.document['South'] = coords['SouthBoundingCoordinate']
            self.document['West'] = coords['WestBoundingCoordinate']
            self.document['East'] = coords['EastBoundingCoordinate']

            self.download_file()
            self.database.add_file(self.document)

            return self.document['path'] + "/" + self.document['file']

    def download_file(self):
        wget = 'wget --load-cookies ~/.urs_cookies --save-cookies ~/.urs_cookies --auth-no-challenge=on --content-disposition '
        wget + self.document['url'] + " -P " + self.document['path'] + " -O " + self.document['file']
        os.system(wget + self.document['url'] + " -O " + self.document['path'] + "/" + self.document['file'])

    def get_url_file(self, point, fecha):
        fini = fecha + "T00:00:00"
        ffin = fecha + "T23:59:59"

        point_url = str(point[1]) + "," + str(point[0]) #lon, lat
        temporal = fini + "," + ffin

        params = {
                  "page_size": 5,
                  "sort_key" : "start_date",
                  "short_name" : self.instrument,
                  "provider" : "OB_DAAC",
                  "point" : point_url,
                  "temporal" :temporal
        }

        headers = {'Authorization': 'Bearer ' + self.token}
            # , "appkey": self.token, "Echo-Token": self.token, "token": self.token}

        umms = []
        print(self.token)
        print(headers)
        # print(self.search_url, headers)
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
            print("Error 403: Unauthorized. Check config.ini Token value, or generate a new one in https://urs.earthdata.nasa.gov/")
            var = input("Do you want to continue without download files? [Y=yes, otherwise=no] : ")
            if var == 'Y':
                self.User_Knows_not_auth = True
                return None
            else:
                exit(-1)
        elif r.status_code == 401:
            print(r.content)
            print("Error connection: check network and try later")
            print(self.search_url)
            print(r)
            exit(-1)
        else:

            print("--> Error: ", r, self.search_url)
            print()
            return None

    def set_cookies(self):
        gen_cookies = 'echo "machine urs.earthdata.nasa.gov login ' + self.user + ' password ' + self.password + '" > ~/.netrc; > ~/.urs_cookies'
        os.system(gen_cookies)





