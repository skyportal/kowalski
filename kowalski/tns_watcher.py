import argparse
import datetime
import numpy as np
import pandas as pd
import pytz
import sys
import time
import traceback

from utils import (
    load_config,
    log,
    Mongo,
    radec_str2geojson,
)


''' load config and secrets '''
config = load_config(config_file='config.yaml')['kowalski']


def mongify(_dict):
    """Massage a TNS catalog entry into something digestable by mongo

    :param _dict:
    :return:
    """
    _tmp = dict(_dict)

    doc = {
        _key.lower().replace('.', '_').replace(' ', '_'): _tmp[_key]
        for _key in _tmp
        if not pd.isnull(_tmp[_key])
    }

    doc['_id'] = _dict['ID']
    doc.pop('id')

    # discovery date as datetime
    try:
        doc['discovery_date'] = datetime.datetime.strptime(
            _dict['Discovery Date (UT)'],
            '%Y-%m-%d %H:%M:%S.%f'
        ).astimezone(pytz.utc)
    except Exception as _e:
        log(_e)
        try:
            doc['discovery_date'] = datetime.datetime.strptime(
                _dict['Discovery Date (UT)'],
                '%Y-%m-%d %H:%M:%S'
            ).astimezone(pytz.utc)
        except Exception as _e:
            log(_e)
            doc['discovery_date'] = None

    # GeoJSON for 2D indexing
    doc['coordinates'] = {}
    _ra_str = doc['ra']
    _dec_str = doc['dec']

    _radec_str = [_ra_str, _dec_str]
    _ra_geojson, _dec_geojson = radec_str2geojson(_ra_str, _dec_str)

    doc['coordinates']['radec_str'] = _radec_str

    doc['coordinates']['radec_geojson'] = {
        'type': 'Point',
        'coordinates': [_ra_geojson, _dec_geojson]
    }

    return doc


def get_tns(grab_all=False):
    """
    Queries the TNS and obtains the targets reported to it.

    """

    # connect to MongoDB:
    log("Connecting to DB")
    mongo = Mongo(
        host=config['database']['host'],
        port=config['database']['port'],
        username=config['database']['username'],
        password=config['database']['password'],
        db=config['database']['db'],
        verbose=0
    )
    log("Successfully connected")

    collection = 'TNS'

    log("Checking indexes")
    mongo.db[collection].create_index(
        [
            ('coordinates.radec_geojson', '2dsphere'),
            ('_id', 1)
        ],
        background=True
    )
    mongo.db[collection].create_index(
        [
            ('discovery_date', -1)
        ],
        background=True
    )

    log("Fetching data...")

    # grab the last 20 pages (with 50 entries each) by default
    num_pages = 20

    if grab_all:
        # grab the latest data:
        url = f'https://wis-tns.weizmann.ac.il/search?format=csv&num_page=50&page=0'
        data = pd.read_csv(url)
        # TNS outputs a maximum of 50 entries per call/page
        num_pages = data["ID"].max() // 50

    for num_page in range(num_pages):
        log(f"Digesting page #{num_page+1} of {num_pages}...")
        url = f'https://wis-tns.weizmann.ac.il/search?format=csv&num_page=50&page={num_page}'

        data = pd.read_csv(url)

        for index, row in data.iterrows():
            try:
                doc = mongify(row)
                doc_id = doc.pop("_id", None)
                if doc_id:
                    mongo.update_one(
                        collection=collection,
                        filt={"_id": doc_id},
                        update={"$set": doc},
                        upsert=True
                    )
            except Exception as e:
                log(str(e))
                log(traceback.print_exc())

    # close connection to db
    mongo.client.close()
    log("Disconnected from db")


def main(grab_all=False):

    while True:
        try:
            get_tns(grab_all)

        except KeyboardInterrupt:
            log("Aborted by user")
            sys.stderr.write('Aborted by user\n')
            sys.exit()

        except Exception as e:
            log(str(e))
            log(traceback.print_exc())

        if not grab_all:
            time.sleep(120)
        else:
            time.sleep(86400*7)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--graball', action='store_true', help='grab all data from TNS')
    args = parser.parse_args()

    main(grab_all=args.graball)
