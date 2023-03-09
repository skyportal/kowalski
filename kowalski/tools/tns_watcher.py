import argparse
import datetime
import json
import os
import sys
import time
import traceback

import pytz
import requests
import tqdm
from kowalski.utils import Mongo, load_config, log, radec_str2geojson

""" load config and secrets """
KOWALSKI_APP_PATH = os.environ.get("KOWALSKI_APP_PATH", "/kowalski")
config = load_config(path=KOWALSKI_APP_PATH, config_file="config.yaml")["kowalski"]


def mongify(doc):
    """Massage a TNS catalog entry into something digestible by K's mongo

    :param doc: dict returned by TNS:/api/get/object
                see p.5, https://www.wis-tns.org/sites/default/files/api/TNS_APIs_manual.pdf
    :return:
    """
    doc["_id"] = doc["objid"]
    doc["name"] = f"{doc['name_prefix']} {doc['objname']}"

    # discovery date as datetime
    try:
        doc["discovery_date"] = datetime.datetime.strptime(
            doc["discoverydate"], "%Y-%m-%d %H:%M:%S.%f"
        ).astimezone(pytz.utc)
    except Exception as _e:
        log(_e)
        try:
            doc["discovery_date"] = datetime.datetime.strptime(
                doc["discoverydate"], "%Y-%m-%d %H:%M:%S"
            ).astimezone(pytz.utc)
        except Exception as _e:
            log(_e)
            doc["discovery_date"] = None
    if doc["discovery_date"] is not None:
        doc["discovery_date_(ut)"] = doc["discovery_date"].strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )

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


def get_tns(grab_all: bool = False, test: bool = False):
    """
    Queries the TNS and obtains the sources reported to it.

    :param grab_all: grab the complete database from TNS? takes a while!
    :param test: grab one object and return
    :return:
    """

    # connect to MongoDB:
    log("Connecting to DB")
    mongo = Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        replica_set=config["database"]["replica_set"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        db=config["database"]["db"],
        srv=config["database"]["srv"],
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

    tns_credentials = {
        param: config["tns"][param] for param in ("bot_id", "bot_name", "api_key")
    }
    for param in tns_credentials:
        if tns_credentials[param] is None:
            # on the CI, get it from env
            tns_credentials[param] = os.environ.get(f"TNS_{param.upper()}")

    bot_id = tns_credentials.get("bot_id")
    bot_name = tns_credentials.get("bot_name")
    headers = {
        "User-Agent": f'tns_marker{{"tns_id": {bot_id}, "type": "bot", "name": "{bot_name}"}}',
    }

    if grab_all or test:
        public_timestamp = datetime.datetime(1, 1, 1).strftime("%Y-%m-%d %H:%M:%S")
    else:
        public_timestamp = (
            datetime.datetime.utcnow() - datetime.timedelta(hours=3)
        ).strftime("%Y-%m-%d %H:%M:%S")

    url = os.path.join(
        config["tns"]["url"],
        "api/get/search",
    )

    recent_sources = (
        requests.post(
            url,
            headers=headers,
            data={
                "api_key": tns_credentials["api_key"],
                "data": json.dumps({"public_timestamp": public_timestamp}),
            },
            allow_redirects=True,
            stream=True,
            timeout=60,
        )
        .json()
        .get("data", dict())
        .get("reply", [])
    )
    recent_sources.reverse()
    log(f"Found {len(recent_sources)} sources reported after {public_timestamp}")

    url = os.path.join(
        config["tns"]["url"],
        "api/get/object",
    )
    for source in tqdm.tqdm(recent_sources):
        try:
            data = requests.post(
                url,
                headers=headers,
                data={
                    "api_key": tns_credentials["api_key"],
                    "data": json.dumps({"objname": source.get("objname")}),
                },
                allow_redirects=True,
                stream=True,
                timeout=10,
            ).json()
            source_data = data.get("data", dict()).get("reply", dict())
            if len(source_data) != 0:
                doc = mongify(source_data)
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

        if test:
            # attempt ingest only the most recent source when running test
            break

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
            time.sleep(60 * 4)
        else:
            time.sleep(86400 * 7)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--graball", action="store_true", help="grab all data from TNS")
    args = parser.parse_args()

    main(grab_all=args.graball)
