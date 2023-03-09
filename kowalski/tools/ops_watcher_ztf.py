import datetime
import os
import sys
import time
import traceback

import pandas as pd
import pymongo
import requests
from kowalski.utils import (
    Mongo,
    datetime_to_jd,
    load_config,
    radec_str2geojson,
    time_stamp,
)

""" load config and secrets """
KOWALSKI_APP_PATH = os.environ.get("KOWALSKI_APP_PATH", "/kowalski")
config = load_config(path=KOWALSKI_APP_PATH, config_file="config.yaml")["kowalski"]


def mongify(doc):

    if doc["ra"] == -99.0 and doc["dec"] == -99.0:
        return doc

    # GeoJSON for 2D indexing
    doc["coordinates"] = dict()
    # doc['coordinates']['epoch'] = 2000.0
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


def get_ops():
    """
    Fetch and ingest ZTF ops data
    """
    # connect to MongoDB:
    print(f"{time_stamp()}: Connecting to DB.")
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
    print(f"{time_stamp()}: Successfully connected.")

    collection = "ZTF_ops"

    print(f"{time_stamp()}: Checking indexes.")
    mongo.db[collection].create_index(
        [("coordinates.radec_geojson", "2dsphere")], background=True
    )
    mongo.db[collection].create_index(
        [
            ("utc_start", pymongo.ASCENDING),
            ("utc_end", pymongo.ASCENDING),
            ("fileroot", pymongo.ASCENDING),
        ],
        background=True,
    )
    mongo.db[collection].create_index(
        [
            ("jd_start", pymongo.ASCENDING),
            ("jd_end", pymongo.ASCENDING),
            ("fileroot", pymongo.ASCENDING),
        ],
        background=True,
    )
    mongo.db[collection].create_index(
        [
            ("jd_start", pymongo.DESCENDING),
            ("pid", pymongo.ASCENDING),
            ("field", pymongo.ASCENDING),
        ],
        background=True,
    )

    # fetch full table
    print(f"{time_stamp()}: Fetching data.")
    url = config["ztf_ops"]["url"]
    r = requests.get(
        url,
        auth=(config["ztf_ops"]["username"], config["ztf_ops"]["password"]),
        verify=False,
    )
    if r.status_code == requests.codes.ok:
        with open(os.path.join("/_tmp", "allexp.tbl"), "wb") as f:
            f.write(r.content)
    else:
        raise Exception(f"{time_stamp()}: Failed to fetch allexp.tbl")

    latest = list(mongo.db[collection].find({}, sort=[["$natural", -1]], limit=1))

    print(f"{time_stamp()}: Loading data.")
    df = pd.read_fwf(
        os.path.join("/_tmp", "allexp.tbl"),
        comment="|",
        widths=[22, 4, 6, 4, 5, 8, 4, 9, 9, 7, 8, 29, 11, 25],
        header=None,
        names=[
            "utc_start",
            "sun_elevation",
            "exp",
            "filter",
            "type",
            "field",
            "pid",
            "ra",
            "dec",
            "slew",
            "wait",
            "fileroot",
            "programpi",
            "qcomment",
        ],
    )

    # drop comments:
    comments = df["utc_start"] == "UT_START"
    df = df.loc[~comments]

    for col in ["sun_elevation", "exp", "filter", "field", "pid"]:
        df[col] = df[col].apply(lambda x: int(x))
    for col in ["ra", "dec", "slew", "wait"]:
        df[col] = df[col].apply(lambda x: float(x))

    df["utc_start"] = df["utc_start"].apply(
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%f")
    )
    df["utc_end"] = df["utc_start"].add(
        df["exp"].apply(lambda x: datetime.timedelta(seconds=x))
    )

    df["jd_start"] = df["utc_start"].apply(lambda x: datetime_to_jd(x))
    df["jd_end"] = df["utc_end"].apply(lambda x: datetime_to_jd(x))

    # drop rows with utc_start <= c['utc_start]
    if len(latest) > 0:
        new = df["jd_start"] > latest[0].get("jd_start", 0)

        if sum(new):
            print(f"{time_stamp()}: Found {sum(new)} new records.")
            df = df.loc[new]
        else:
            # no new data? take a nap...
            print(f"{time_stamp()}: No new data found.")
            # close connection to db
            mongo.client.close()
            print(f"{time_stamp()}: Disconnected from db.")
            return

    documents = df.to_dict("records")
    documents = [mongify(doc) for doc in documents]

    print(f"{time_stamp()}: Inserting {len(documents)} documents.")

    mongo.insert_many(collection=collection, documents=documents)

    # close connection to db
    mongo.client.close()
    print(f"{time_stamp()}: Disconnected from db.")


def main():

    while True:
        try:
            get_ops()

        except KeyboardInterrupt:
            sys.stderr.write(f"{time_stamp()}: Aborted by user\n")
            sys.exit()

        except Exception as e:
            print(str(e))
            traceback.print_exc()

        #
        time.sleep(300)


if __name__ == "__main__":

    main()
