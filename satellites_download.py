# La carga y lectura de funciona por "proyectos" que se configuran en el json que se pasa por parametro.
# El csv generado es el mismo para cada proyecto ya que el nombre se genera con estos datos
# Se añaden filas al CSV, por lo que al hacer el analisis será conveniente ordenar y/o eliminar duplicados.

import sys
import json
import pandas as PD
import datetime
import os
import nasa
import esa


try:
    config_path = sys.argv[1]
except Exception as EX:
    print(EX.with_traceback(None))
    print("Config file parameter missed.")
    exit(-1)


with open(config_path) as config_file:
     config = json.loads(config_file.read())

try:    #fechas
    project = config['project']
    agency = config['agency']
    satellite = config['satellite']
    instrument = config['instrument']

    try:
        date_from = config['date_from']
        date_to = config['date_to']
        fini = datetime.datetime.strptime(date_from, "%Y-%m-%d")
        ffin = datetime.datetime.strptime(date_to, "%Y-%m-%d")
    except:
        today = datetime.datetime.now().date()
        fini = today
        ffin = today

    csv_path = config['csv_path']
    if not os.path.exists(csv_path):
        print("CSV path ", csv_path, " does not exist")
        exit(-1)

    csv_file = config['project'] + "_" + config['satellite'] + "_" + config['instrument'] + ".csv"

    work_path  = config['files']
    if not os.path.exists(work_path):
        print("Files path ", work_path, " does not exist")
        exit(-1)
    database = config['database']
        #estaciones o puntos
    points = PD.read_csv(config['points'])
except Exception as EX:
    print(EX.with_traceback(None))
    print("Config file parameter missed.")
    exit(-1)

if str.upper(agency) == "ESA":
    SAT = esa.ESA(satellite, instrument, work_path, database)
elif str.upper(agency) == "NASA":
    SAT = nasa.NASA(satellite, instrument, work_path, database)
else:
    print("ERROR: Agency value NOT RECOGNIZED")
    exit(-1)



current_date = fini
print()
print("--> Satellite_Download.py: Cargando datos....")
print()
while current_date <= ffin:
    fecha = datetime.datetime.strftime(current_date, format="%Y-%m-%d")
    print("--> Looking for data at", fecha)
    print()
    i = 0
    for idx, row in points.iterrows():
        file = SAT.get_file( row['lat'], row['lon'], fecha)
        if file is None:
            print("------> No data for point ", row['name'], row['lat'], row['lon'], "at", fecha)
            continue
        print("------> Downloaded", row['name'], row['lat'], row['lon'], fecha, file)

    current_date = current_date + datetime.timedelta(days=1)

print()
print("File Download ends")