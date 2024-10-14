from hdfs import InsecureClient
from hdfs.util import HdfsError
import os
import csv
import json
import fastavro
import io
import requests
from tqdm import tqdm
import logging

# We create a dictionary of the sources and their schema
# The code is written in a manner that to add a new source, we just need
# to include it in this dictionary for it to uploaded to hdfs. Supported
# formats at the moment are csv and json. If a new source was introduced
# the necessary code would need to be implemented to read and process the
# files.
sources_schema = {'idealista': {'format': 'json',
                         'schema': {
                             'namespace': 'bdm1.avro',
                             'type': 'record',
                             'name': 'idealista',
                             'fields': [
                                 {'name': 'propertyCode',
                                  'type': 'string', 'default': 'null'},
                                 {'name': 'thumbnail', 'type': 'string',
                                  'default': 'null'},
                                 {'name': 'externalReference',
                                  'type': 'string', 'default': 'null'},
                                 {'name': 'numPhotos', 'type': 'int', 'default': -1},
                                 {'name': 'price', 'type': 'double', 'default': -1},
                                 {'name': 'propertyType',
                                  'type': 'string', 'default': 'null'},
                                 {'name': 'operation', 'type': 'string',
                                  'default': 'null'},
                                 {'name': 'size', 'type': 'double', 'default': -1},
                                 {'name': 'exterior', 'type': 'boolean',
                                  'default': False},
                                 {'name': 'rooms', 'type': 'int', 'default': -1},
                                 {'name': 'bathrooms', 'type': 'int', 'default': -1},
                                 {'name': 'address', 'type': 'string',
                                  'default': 'null'},
                                 {'name': 'province', 'type': 'string',
                                  'default': 'null'},
                                 {'name': 'municipality',
                                  'type': 'string', 'default': 'null'},
                                 {'name': 'district', 'type': 'string',
                                  'default': 'null'},
                                 {'name': 'country', 'type': 'string',
                                  'default': 'null'},
                                 {'name': 'neighborhood',
                                  'type': 'string', 'default': 'null'},
                                 {'name': 'latitude', 'type': 'double', 'default': -1},
                                 {'name': 'longitude',
                                     'type': 'double', 'default': -1},
                                 {'name': 'showAddress',
                                  'type': 'boolean', 'default': False},
                                 {'name': 'url', 'type': 'string',
                                     'default': 'null'},
                                 {'name': 'distance', 'type': 'string',
                                  'default': 'null'},
                                 {'name': 'hasVideo', 'type': 'boolean',
                                  'default': False},
                                 {'name': 'status', 'type': 'string',
                                  'default': 'null'},
                                 {'name': 'newDevelopment',
                                  'type': 'boolean', 'default': False},
                                 {'name': 'hasLift', 'type': 'boolean',
                                  'default': False},
                                 {'name': 'parkingSpace',
                                     'type': {'type': 'record', 'name': 'ParkingSpace', 'fields': [
                                         {'name': 'hasParkingSpace',
                                          'type': 'boolean', 'default': False},
                                         {'name': 'isParkingSpaceIncludedInPrice', 'type': 'boolean', 'default': False}]
                                     }, 'default': {'hasParkingSpace': False, 'isParkingSpaceIncludedInPrice': False}
                                  },
                                 {'name': 'priceByArea',
                                  'type': 'double', 'default': -1},
                                 {
                                     'name': 'detailedType',
                                     'type': {
                                         'type': 'record',
                                         'name': 'detailedType',
                                         'fields': [
                                             {'name': 'typology',
                                              'type': 'string', 'default': 'null'}
                                         ]
                                     }, 'default': {'typology': 'null'}
                                 },
                                 {
                                     'name': 'suggestedTexts',
                                     'type': {
                                         'type': 'record',
                                         'name': 'suggestedTexts',
                                         'fields': [
                                             {'name': 'subtitle', 'type': 'string',
                                              'default': 'null'},
                                             {'name': 'title', 'type': 'string',
                                              'default': 'null'}
                                         ]
                                     }
                                 },
                                 {'name': 'hasPlan', 'type': 'boolean',
                                  'default': False},
                                 {'name': 'has3DTour', 'type': 'boolean',
                                  'default': False},
                                 {'name': 'has360', 'type': 'boolean',
                                  'default': False},
                                 {'name': 'hasStaging',
                                  'type': 'boolean', 'default': False},
                                 {'name': 'topNewDevelopment',
                                  'type': 'boolean', 'default': False}
                             ]
                         }},
           'opendatabcn-income': {'format': 'csv',
                                  'schema': {
                                      'namespace': 'bdm1.avro',
                                      'type': 'record',
                                      'name': 'income',
                                      'fields': [
                                          {'name': 'Any', 'type': ['int', 'string'],
                                              'default': -1},
                                          {'name': 'Codi_Districte',
                                           'type':  ['int', 'string'], 'default': -1},
                                          {'name': 'Nom_Districte',
                                           'type': 'string', 'default': 'null'},
                                          {'name': 'Codi_Barri',
                                           'type':  ['int', 'string'], 'default': -1},
                                          {'name': 'Nom_Barri', 'type': 'string',
                                           'default': 'null'},
                                          {'name': 'Població',
                                           'type':  ['int', 'string'], 'default': -1},
                                          {'name': 'Índex RFD Barcelona = 100',
                                           'type': 'string', 'default': 'null'},
                                      ]
                                  }},
           'lookup_tables': {'format': 'csv',
                             'schema': {
                                 'namespace': 'bdm1.avro',
                                 'type': 'record',
                                 'name': 'lookup',
                                 'fields': [
                                     {'name': 'district', 'type': 'string',
                                      'default': 'null'},
                                     {'name': 'neighborhood',
                                      'type': 'string', 'default': 'null'},
                                     {'name': 'district_n_reconciled',
                                      'type': 'string', 'default': 'null'},
                                     {'name': 'district_n', 'type': 'string',
                                      'default': 'null'},
                                     {'name': 'district_id',
                                      'type': 'string', 'default': 'null'},
                                     {'name': 'neighborhood_n_reconciled',
                                      'type': 'string', 'default': 'null'},
                                     {'name': 'neighborhood_n',
                                      'type': 'string', 'default': 'null'},
                                     {'name': 'neighborhood_id',
                                      'type': 'string', 'default': 'null'},
                                 ]
                             }},
            'opendatabcn-elections': {'format': 'json',
                                      'schema': {
                                            'namespace': 'bdm1.avro',
                                            'type': 'record',
                                            'name': 'lookup',
                                            'fields': [
                                                {'name': '_id', 'type': 'int', 
                                                 'default': -1},
                                                {'name': 'Any', 'type': ['int', 'string'],
                                                 'default': -1},
                                                {'name': 'Codi_Districte',
                                                 'type':  ['int', 'string'], 'default': -1},
                                                {'name': 'Nom_Districte',
                                                 'type': 'string', 'default': 'null'},
                                                {'name': 'Codi_Barri',
                                                 'type':  ['int', 'string'], 'default': -1},
                                                {'name': 'Nom_Barri', 'type': 'string',
                                                 'default': 'null'},
                                                {'name': 'Seccio_censal', 'type':['int', 'string'],
                                                 'default': -1},
                                                {'name': 'Camp', 'type':'string',
                                                 'default': 'null'},
                                                {'name': 'Nombre', 'type':['int', 'string'],
                                                 'default': -1}
                                            ]
                                      }
                
            }

           }

api_sources = {'opendatabcn-elections': {'2019_elections': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=f067ff8a-1201-474e-a02c-7f9974623b3c',
                                         '2016_elections': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=45b264df-baf2-402a-85cb-7e0c642e0e98',
                                         '2015_elections': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=10c76eb3-2a6b-4197-9713-8d8ac8a9f014',
                                         '2011_elections': 'https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=cbfa8cf3-db43-410a-90a9-a37d98e52959'
                                         }
              }

# Setting up logger
logging.basicConfig(filename='../logs/dataSourceRegistry.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

def upload(records, src, file, con):
    # Create IO stream
    avro_data = io.BytesIO()

    # Write to IO stream data in avro format
    fastavro.writer(avro_data, sources_schema[src]['schema'], records)
    
    # Reset buffer to the beginning
    avro_data.seek(0)
    try:
        con.write('/user/bdm/' + source + '/' + file.split('.')[0] + '.avro', data = avro_data, overwrite = True)
    except HdfsError:
        print(file + ' already exists.')
               
# Set up HDFS client
hdfs_con = InsecureClient('http://localhost:9870', user='bdm')

for source in list(sources_schema.keys()):
    print('Processing ' + source)
    
    # If the directory does not exist in hdfs, we create it.
    if not hdfs_con.status('/user/bdm/'+ source, strict=False):
        hdfs_con.makedirs('/user/bdm/'+ source)
    
    # If source comes from an API
    if source in api_sources.keys():

        # Iterate through files
        for file in tqdm(api_sources[source]):
            
            # Retrieve file using API
            req = requests.get(api_sources[source][file])

            # Request successful
            if req.status_code == 200:
                data = req.json()
                data = data['result']['records']
                upload(data, source, file, hdfs_con)
            
            # Request not successful
            else:
                print('Failed to fetch ' + file)
                print('Status code: ' + str(req.status_code))

    else:
    
        input_dir  = '../data/' + source

        # Iterate through each file in the input directory
        for filename in tqdm(os.listdir(input_dir)):
            
            # Supported formats
            if filename.endswith('.json') or filename.endswith('.csv'):
                
                # Open the file
                with open(os.path.join(input_dir, filename), 'r') as file:
                    
                    # Read the data
                    if filename.endswith('.json'):
                        data = json.load(file)
                    elif filename.endswith('.csv'):
                        reader = csv.DictReader(file)
                        data = []
                        for i, row in enumerate(reader):
                            data.append(row)
                    
                    upload(data, source, filename, hdfs_con)
                
                file.close()
            
            # Unsupported formats
            else:
                print('WARNING: file format not supported (' + filename + ')')
    print()