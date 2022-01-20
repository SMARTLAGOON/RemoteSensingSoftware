from tinydb import TinyDB, Query

scheme = {
        "agency": ["NASA", "ESA"],
        "satellite": ["SENTINEL A", "SENTINEL B", "SNPP", "VIIRS", "MODIS"],
        "instrument": ["OLCI", "VIIRSN_L2_OC", "MODIS"],
        "date": "",
        "time":"",
        "North": 1,
        "South": 1,
        "West":1,
        "Est":1,
        "file": "",
        "path": "",
        "url": ""
    }

class Database():

    def __init__(self, name):
        try:
            self.db = TinyDB(name)
        except:
            f = open(name, "x")
            self.db = TinyDB(name)

    def exist_file(self, data):
        # if self.check_query_constrains(data):
        table = self.db.table(data['satellite'])
        q = Query()

        regs = table.search((q.date == data['date']) & (q.instrument == data['instrument']) &
                            (q.North >= data['lat']) & (q.South <= data['lat']) &
                            (q.East >= data['lon']) & (q.West <= data['lon']))
        if len(regs) > 0:
            return regs[0]['path'] + "/" + regs[0]['file']
        else:
            return False

    def add_file(self, data):
        # if self.check_insert_constrains(data):
        table = self.db.table(data['satellite'])
        table.insert(data)
        # self.db.insert(data)

    def check_query_constrains(self, data):
        # try:
        print(data)
        if not str.upper(data['agency']) in scheme['agency']:
            print("CAGO EN DIOS")
            return False

        if not str.upper(data['satellite']) in scheme['satellite']:
            print("CAGO EN DIOS 1")
            return False

        if not str.upper(data['instrument']) in scheme['instrument']:
            print("CAGO EN DIOS 2")
            return False

        if data['lat'] > 90 or data['lat'] < -90 \
                or data['lon'] > 180 or data['lon'] < -180:

            print("CAGO EN DIOS 3")
            return False

        return True

    def check_insert_constrains(self, data):
        # try:
            print(data)
            if not str.upper(data['agency']) in scheme['agency']:
                print("CAGO EN DIOS")
                return False

            if not str.upper(data['satellite']) in scheme['satellite']:
                print("CAGO EN DIOS 1")
                return False

            if not str.upper(data['instrument']) in scheme['instrument']:
                print("CAGO EN DIOS 2")
                return False

            if data['North']> 90 or data['North']< -90\
                    or data['South'] > 90 or data['South']< -90\
                    or data['East'] > 180 or data['East'] < -180\
                    or data['West'] > 180 or data['West'] < -180:
                print("CAGO EN DIOS 3")
                return False
        # except:
        #     print("CAGO EN DIOS 4")
        #     exit()
        #     return False

