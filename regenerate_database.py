# Recorre todos los archivos .NC y genera una base de datos tyni.
# param 1: Directorio con ficheros .nc
import glob
import os.path

import database as DB
import sys

try:
    dir = sys.argv[1]
    if not os.path.exists(dir):
        print("No existe el directorio")
        exit()
except Exception as EX:
    print(EX.with_traceback(None))
    print("Param missed: Dir to read .nc files")
    exit(-1)

type = None
try:
    type = sys.argv[2]

except:
    print("Reading all files")


files = glob.glob(dir)
for f in files:
    #is dir
    if os.path.isdir(f):

    else:
        NC_to_DB()