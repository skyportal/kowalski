# WIP
import argparse
import numpy as np
import sys
import json
import pymongo
import traceback
import datetime
import time
import pandas as pd
import pytz
from utils import (
    Mongo,
    radec_str2geojson,
    time_stamp,
)


''' load config and secrets '''
with open('/app/config.json') as cjson:
    config = json.load(cjson)

with open('/app/secrets.json') as sjson:
    secrets = json.load(sjson)

for k in secrets:
    config[k].update(secrets.get(k, {}))


def mongify(_dict):

    _tmp = dict(_dict)

    doc = {_key.lower().replace('.', '_').replace(' ', '_'): _tmp[_key] for _key in _tmp}

    # candid+objectId should be a unique combination:
    doc['_id'] = f"{_dict['ID']}"

    # discovery date as datetime
    try:
        doc['discovery_date'] = datetime.datetime.strptime(_dict['Discovery Date (UT)'],
                                                           '%Y-%m-%d %H:%M:%S').astimezone(pytz.utc)
    except Exception as _e:
        doc['discovery_date'] = None

    # GeoJSON for 2D indexing
    doc['coordinates'] = {}
    # doc['coordinates']['epoch'] = 2000.0
    _ra_str = doc['ra']
    _dec_str = doc['dec']

    _radec_str = [_ra_str, _dec_str]
    _ra_geojson, _dec_geojson = radec_str2geojson(_ra_str, _dec_str)

    doc['coordinates']['radec_str'] = _radec_str

    doc['coordinates']['radec_geojson'] = {'type': 'Point',
                                           'coordinates': [_ra_geojson, _dec_geojson]}
    # degrees:
    doc['coordinates']['radec_deg'] = [_ra_geojson + 180.0, _dec_geojson]
    # radians:
    doc['coordinates']['radec_rad'] = [(_ra_geojson + 180.0) * np.pi / 180.0, _dec_geojson * np.pi / 180.0]

    return doc


def get_tns_date2date(date1, date2, grab_all=False):
    """
    Queries the TNS and obtains the targets reported between two dates.
    It parses the coordinates and transporms them into decimals.
    It writes a csv table with RA, DEC in degrees, which is directly ingestable into a postresql file.

    date1: in the format of: YYYY-MM-DD
    date2: in the format of: YYYY-MM-DD

    """

    # connect to MongoDB:
    print(f'{time_stamp()}: Connecting to DB.')
    mongo = Mongo(
        host=config['database']['host'],
        port=config['database']['port'],
        username=config['database']['username'],
        password=config['database']['password'],
        db=config['database']['db'],
        verbose=0
    )
    print(f'{time_stamp()}: Successfully connected.')

    collection = 'TNS'

    mongo.db[collection].create_index(
        [
            ('coordinates.radec_geojson', '2dsphere'),
            ('_id', pymongo.ASCENDING)
        ],
        background=True
    )

    # convert dates to YYYY-MM-DD:
    date1_f = date1.strftime('%Y-%m-%d')
    date2_f = date2.strftime('%Y-%m-%d')

    # fetch last 1000 entries
    url = 'https://wis-tns.weizmann.ac.il/search?' + \
          f'&date_start%5Bdate%5D={date1_f}&date_end%5Bdate%5D={date2_f}&format=csv&num_page=1000'

    kk = 50 if grab_all else 1
    for page in range(kk):
        # print(page)
        url = f'https://wis-tns.weizmann.ac.il/search?format=csv&num_page=1000&page={page}'

        data = pd.read_csv(url)
        # print(data)

        documents = []

        for index, row in data.iterrows():
            doc = mongify(row)
            documents.append(doc)

        mongo.insert_many(collection=collection, documents=documents)

    # close connection to db
    mongo.client.close()
    print(f'{time_stamp()}: Disconnected from db.')


def main(date1, date2, grab_all=False):

    while True:
        try:
            get_tns_date2date(date1, date2, grab_all)

        except KeyboardInterrupt:
            sys.stderr.write('Aborted by user\n')
            sys.exit()

        except Exception as e:
            print(str(e))
            traceback.print_exc()

        #
        time.sleep(600)


if __name__ == '__main__':
    ''' Create command line argument parser '''
    parser = argparse.ArgumentParser()

    parser.add_argument('--graball', action='store_true', help='grab all data TNS')

    args = parser.parse_args()

    start = datetime.datetime(1000, 1, 1)
    end = datetime.datetime.utcnow()

    # get_tns_date2date(start, end)
    main(start, end, grab_all=args.graball)
