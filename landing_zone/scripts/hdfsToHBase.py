import happybase
from fastavro import reader
from hdfs import InsecureClient
from datetime import datetime as dt
from tqdm import tqdm
import logging


# Setting up logger
logging.basicConfig(filename='../logs/processedFiles.log', level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

# Connect to HBase
hbase_con = happybase.Connection('localhost')
hbase_con.open()

# Connect to HDFS
hdfs_con = InsecureClient('http://localhost:9870')
hdfs_dir = '/user/bdm/'

# Initialize batch
batch_size = 1000

# List of sources in HDFS to move to HBase
sources = hdfs_con.list('/user/bdm/')

for source in sources:
    print('Moving ' + source + ' from HDFS to HBase')
    file_list = hdfs_con.list(hdfs_dir+source)

    tables = hbase_con.tables()
    if bytes(source, encoding='utf-8') not in tables:
        hbase_con.create_table(
            source,
            {'data': dict(),
             'metadata': dict()}
        )

    table = hbase_con.table(source)
    batch = table.batch(batch_size=batch_size)
    
    # For each file in source directory
    for file in tqdm(file_list):

        # Get timestamp
        ts = str(dt.now())
        ts = ts.replace(' ', '_')
        ts = ts.replace('.', '_')
        ts = ts.replace(':', '_')

        # Process only avro files
        if file.endswith('.avro'):
            with hdfs_con.read(hdfs_dir+source+'/'+file) as f:
                
                # Fastavro reader
                readr = reader(f)

                # Iterate over rows in avro file
                for row_num, record_bytes in enumerate(readr):
                    # Deserialize the Avro record
                    record = dict(record_bytes)
                    
                    # Iterate keys of a record
                    for key in record.keys():
                        value = record[key]

                        # In case of nested dictionary
                        if isinstance(value, dict):
                            for key2 in value.keys():
                                batch.put(file.split('.')[0]+'_'+ts, {'data:'+ key2 + '_' + str(row_num): str(value[key2])})
                        
                        # When not a nested dictionary
                        else:
                            batch.put(file.split('.')[0]+'_'+ts, {'data:'+ key + '_' + str(row_num): str(record[key])})
                    
                    # When batch limit reached, send batch
                    if batch._mutation_count == batch_size:
                        batch.send()
                
                # Add schema to HBase
                fields = readr.writer_schema['fields']
                
                for field in fields:

                    # Nested dictionary
                    if isinstance(field['type'], dict):
                        for field2 in field['type']['fields']:
                            batch.put(file.split('.')[0]+'_'+ts, {'metadata:'+ field2['name']: str(field2['type'])})
                    
                    # Not nested
                    else:
                        batch.put(file.split('.')[0]+'_'+ts, {'metadata:'+ field['name']: str(field['type'])})

            logging.info(file.split('.')[0]+'_'+ts + ' processed into HBase')

        if not hdfs_con.delete(hdfs_dir+source+'/'+file):
            print('Failed to delete: ' + str(hdfs_dir+source+'/'+file))

    print()

    # Flush remaning rows
    batch.send()
    
hbase_con.close()
