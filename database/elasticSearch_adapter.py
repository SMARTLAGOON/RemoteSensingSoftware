# coding=utf-8
# Gestiona los datos de la aplicacion en elastic.
# los id de los elementos de elasctic se corresponde con en el nombre del archivo que es unico.
import traceback
from elasticsearch import Elasticsearch
from elasticsearch import helpers


class Database:

    def __init__(self, url="http://127.0.0.1:9200", name='nc_files'):
        try:
            self.db = Elasticsearch(url)
            # self.db.transport.connection_pool.connection.session.headers.update(headers)
            self.index = name
            if not self.db.ping():
                raise Exception("Connection failed to " + url)
            print("DatabaseES.py: connection established")
        except Exception as Ex:
            traceback.print_exc(Ex)


    # por fecha, agencia, instrumento y coordenadas
    def generate_standar_query(self, data):
        query = {
            "query": {
                "bool": {
                    "must": [],
                    "filter": {
                        "geo_shape": {
                            "boundingBox": {
                                "shape": {
                                    "type": "point",
                                    "coordinates": []  # lon, lat
                                },
                                "relation": "contains"
                            }
                        }
                    }
                }
            }
        }
        date = {"term": {"date": data['date']}}
        query['query']['bool']['must'].append(date)
        agency = {"term": {"agency": data['agency']}}
        query['query']['bool']['must'].append(agency)
        satellite = {"term": {"satellite": data['satellite']}}
        query['query']['bool']['must'].append(satellite)
        instrument = {"term": {"instrument": data['instrument']}}
        query['query']['bool']['must'].append(instrument)

        query['query']['bool']['filter']['geo_shape']['boundingBox']['shape']['coordinates'] = [data['lon'], data['lat']]
        return query

    def generate_lastRecord_query(self, data):
        query = {
            "query": {
                "bool": {
                    "must": [],
                    "filter": {
                        "geo_shape": {
                            "boundingBox": {
                                "shape": {
                                    "type": "point",
                                    "coordinates": []  # lon, lat
                                },
                                "relation": "contains"
                            }
                        }
                    }
                }
            }
        }
        sort = {"date": {"order": "desc"}}

        query['sort'] = sort
        query['size'] = 1
        agency = {"term": {"agency": data['agency']}}
        query['query']['bool']['must'].append(agency)
        satellite = {"term": {"satellite": data['satellite']}}
        query['query']['bool']['must'].append(satellite)
        instrument = {"term": {"instrument": data['instrument']}}
        query['query']['bool']['must'].append(instrument)

        query['query']['bool']['filter']['geo_shape']['boundingBox']['shape']['coordinates'] = [data['lon'], data['lat']]
        return query

    def exist_file(self, data):
        # if self.check_query_constrains(data):
        query = self.generate_standar_query(data)
        results = self.db.search(index=self.index, body=query)
        total = results['hits']['total']['value']

        if total == 0:
            return False

        return results['hits']['hits'][0]['_source']['file']
    #OK
    def get_record(self, data):
        query = self.generate_standar_query(data)
        results = self.db.search(index=self.index, body=query)
        return results['hits']['hits']

    #OK
    def add_file(self, data):
        bulk_index = {}
        index = {}
        # {"index": {"_index": "test", "_type": "_doc", "_id": "1"}}
        # bulk_index['_type'] = "_doc"
        bulk_index['_index'] = self.index
        bulk_index['_id'] = data['file']
        # index['index'] = bulk_index
        bulk_index['_source'] = data

        result = helpers.bulk(self.db, [bulk_index])
        return result
    #ok
    def get_last_by_date(self, data):
        query = self.generate_lastRecord_query(data)
        results = self.db.search(index=self.index, body=query)
        return results['hits']['hits'][0]

    #OK
    def remove_file(self, data):
        # query = {"query": {"terms": {"file": data['file']}}}
        res = self.db.delete(index=self.index, id=data['file'])
        return res

