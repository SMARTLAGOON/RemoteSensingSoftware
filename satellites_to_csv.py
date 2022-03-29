# La carga y lectura de funciona por "proyectos" que se configuran en el json que se pasa por parametro.
# El csv generado es el mismo para cada proyecto ya que el nombre se genera con estos datos
# Se añaden filas al CSV, por lo que al hacer el analisis será conveniente ordenar y/o eliminar duplicados.

import sys
import json
import traceback

import pandas as PD
import datetime
import os
import csv
from classes import nasa
from classes import esa
import logging
import time

start_time = time.time()
def to_excel(file, data):
    # file = path + file name
    add_headers = True
    if os.path.exists(file):
        add_headers = False
    with open(file, 'a') as f:
        w = csv.DictWriter(f, data.keys())
        if add_headers:
            w.writeheader()
        w.writerow(data)
try:
    config_path = sys.argv[1]
except Exception as EX:
    print("ERROR ", str(EX))
    exit(-1)

with open(config_path) as config_file:
    config = json.loads(config_file.read())

try:  # fechas
    project = config['project']
    agency = config['agency']
    satellite = config['satellite']
    logging.basicConfig(filename="logs/" + satellite + '_to_csv.log', format='%(asctime)s %(message)s', level=logging.INFO)
    
    instrument = config['instrument']
    date_from = config['date_from']
    date_to = config['date_to']
       
    csv_path = config['csv_path']
    if not os.path.exists(csv_path):
        logging.info(" ERROR: CSV path " + csv_path + " does not exist")
        exit(-1)

    csv_file = config['project'] + "_" + config['satellite'] + "_" + config['instrument'] + ".csv"

    work_path = config['files']
    if not os.path.exists(work_path):
        logging.info(" ERROR: Files path " + work_path + " does not exist")
        exit(-1)

    database_file = config['database']

    # estaciones o puntos
    points = PD.read_csv(config['points'])
except Exception as EX:
    logging.error(EX.with_traceback(None))
    logging.info(" ERROR: Config file parameter missed.")
    exit(-1)

if str.upper(agency) == "ESA":
    SAT = esa.ESA(satellite, instrument, work_path, database_file)
elif str.upper(agency) == "NASA":
    SAT = nasa.NASA(satellite, instrument, work_path, database_file)
else:
    logging.info(" ERROR: ERROR: Agency value NOT RECOGNIZED")
    exit(-1)

fini = datetime.datetime.strptime(date_from, "%Y-%m-%d")
ffin = datetime.datetime.strptime(date_to, "%Y-%m-%d")

current_date = fini

logging.info(" Satellite To CSV: Cargando datos")

next_date = False
while current_date <= ffin:
    fecha = datetime.datetime.strftime(current_date, format="%Y-%m-%d")
    logging.info(" DEBUG: Looking for data at" + str(current_date))

    i = 0
    for idx, row in points.iterrows():
        file_name = csv_path + "/" + str(row['name']) + "_" + csv_file
        # print("LAT",  row['lat'], "LON", row['lon'])
        try:
            data = SAT.get_data(fecha, row['lat'], row['lon'], False)
        except Exception as ex:
            logging.info(" Error: Not file for " + current_date.strftime("%Y-%m-%d"))
            logging.info(" Error:" + current_date.strftime("%Y-%m-%d") + " " + str(ex))
            break

        if data is None:
            logging.info(" DEBUG: Not file for " + current_date.strftime("%Y-%m-%d"))
            break

        # data['lat'] = row['lat']
        # data['lon'] = row['lon']
        to_excel(file_name, data)

    current_date = current_date + datetime.timedelta(days=1)

logging.info(" Files CSV generated at" + csv_path)
seconds = time.time() - start_time
logging.info(satellite + ". TIME: " + str(seconds))

print("--- %s seconds ---" % (time.time() - start_time))
print("Process finish, check log")