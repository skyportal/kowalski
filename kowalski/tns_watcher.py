import argparse
import datetime
import pandas as pd
import pytz
import requests
import os
import sys
import time
import traceback

from utils import (
    load_config,
    log,
    Mongo,
    radec_str2geojson,
)


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


def mongify(_dict):
    """Massage a TNS catalog entry into something digestable by mongo

    :param _dict:
    :return:
    """
    _tmp = dict(_dict)

    doc = {
        _key.lower().replace(".", "_").replace(" ", "_"): _tmp[_key].strip()
        if isinstance(_tmp[_key], str)
        else _tmp[_key]
        for _key in _tmp
        if not pd.isnull(_tmp[_key])
    }

    doc["_id"] = _dict["ID"]
    doc.pop("id")

    # discovery date as datetime
    try:
        doc["discovery_date"] = datetime.datetime.strptime(
            _dict["Discovery Date (UT)"], "%Y-%m-%d %H:%M:%S.%f"
        ).astimezone(pytz.utc)
    except Exception as _e:
        log(_e)
        try:
            doc["discovery_date"] = datetime.datetime.strptime(
                _dict["Discovery Date (UT)"], "%Y-%m-%d %H:%M:%S"
            ).astimezone(pytz.utc)
        except Exception as _e:
            log(_e)
            doc["discovery_date"] = None

    # GeoJSON for 2D indexing
    doc["coordinates"] = {}
    _ra_str = doc["ra"]
    _dec_str = doc["dec"]

    _radec_str = [_ra_str, _dec_str]
    _ra_geojson, _dec_geojson = radec_str2geojson(_ra_str, _dec_str)

    doc["coordinates"]["radec_str"] = _radec_str

    doc["coordinates"]["radec_geojson"] = {
        "type": "Point",
        "coordinates": [_ra_geojson, _dec_geojson],
    }

    return doc


def get_tns(grab_all: bool = False, num_pages: int = 10, entries_per_page: int = 100):
    """
    Queries the TNS and obtains the sources reported to it.

    :param grab_all: grab the complete database from TNS? takes a while!
    :param num_pages: grab the last <num_pages> pages
    :param entries_per_page: number of entries per page to grab
    :return:
    """

    # connect to MongoDB:
    log("Connecting to DB")
    mongo = Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        db=config["database"]["db"],
        verbose=0,
    )
    log("Successfully connected")

    collection = config["database"]["collections"]["tns"]

    if config["database"]["build_indexes"]:
        log("Checking indexes")
        for index in config["database"]["indexes"][collection]:
            try:
                ind = [tuple(ii) for ii in index["fields"]]
                mongo.db[collection].create_index(
                    keys=ind,
                    name=index["name"],
                    background=True,
                    unique=index["unique"],
                )
            except Exception as e:
                log(e)

    log("Fetching data...")

    if grab_all:
        # grab the latest data (5 is the minimum):
        url = os.path.join(config["tns"]["url"], "search?format=csv&num_page=5&page=0")
        data = pd.read_csv(url)
        num_pages = data["ID"].max() // entries_per_page

    for num_page in range(num_pages):
        log(f"Digesting page #{num_page+1} of {num_pages}...")
        url = os.path.join(
            config["tns"]["url"],
            f"search?format=csv&num_page={entries_per_page}&page={num_page}",
        )

        # 20210114: wis-tns.org has issues with their certificate
        csv_data = requests.get(url, verify=False).content
        data = pd.read_csv(csv_data.decode("utf-8"))

        for index, row in data.iterrows():
            try:
                doc = mongify(row)
                doc_id = doc.pop("_id", None)
                if doc_id:
                    mongo.update_one(
                        collection=collection,
                        filt={"_id": doc_id},
                        update={"$set": doc},
                        upsert=True,
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
            sys.stderr.write("Aborted by user\n")
            sys.exit()

        except Exception as e:
            log(str(e))
            log(traceback.print_exc())

        if not grab_all:
            time.sleep(120)
        else:
            time.sleep(86400 * 7)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--graball", action="store_true", help="grab all data from TNS")
    args = parser.parse_args()

    main(grab_all=args.graball)
