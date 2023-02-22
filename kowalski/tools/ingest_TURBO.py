import datetime
import fire
import multiprocessing
import os
import pprint
import pathlib
import pyarrow.parquet as pq
import pymongo
import pytz
import random
import time
from tqdm import tqdm
import traceback
from typing import Sequence

from utils import (
    deg2dms,
    deg2hms,
    init_db_sync,
    load_config,
    log,
    Mongo,
)


""" load config and secrets """
#env_var=os.environ
#print("User's Environment variable:")
#pprint.pprint(dict(env_var), width = 1)
#exit()
KOWALSKI_APP_PATH = os.environ.get("KOWALSKI_APP_PATH", "/app")
#KOWALSKI_APP_PATH = '/home/rstrausb/kowalski/'
config = load_config(path=KOWALSKI_APP_PATH, config_file="config.yaml")["kowalski"]
init_db_sync(config=config)


def utc_now():
    return datetime.datetime.now(pytz.utc)


filters = {"zg": 1, "zr": 2, "zi": 3}

sources_int_fields = ("_id", "filter", "field", "rc", "nepochs")
sourcedata_int_fields = "catflags"


def process_file(argument_list: Sequence):
    file_name, collections, batch_size, rm_file, dry_run = argument_list
    try:
        # connect to MongoDB:
        mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            verbose=0,
        )

        df = pq.read_table(file_name).to_pandas()

        df.rename(
            columns={
                "objectid": "_id",
                "filterid": "filter",
                "fieldid": "field",
                "rcid": "rc",
                "objra": "ra",
                "objdec": "dec",
                "hmjd": "hjd",
                "nepochs": "nobs",
            },
            inplace=True,
        )

        def clean_up_document(document):
            """Format passed in dicts for Mongo insertion"""
            # convert types for pymongo:
            for k, v in document.items():
                if k in sources_int_fields:
                    document[k] = int(document[k])

            # GeoJSON for 2D indexing
            document["coordinates"] = dict()
            _ra = document["ra"]
            _dec = document["dec"]
            _radec_str = [deg2hms(_ra), deg2dms(_dec)]
            document["coordinates"]["radec_str"] = _radec_str
            # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
            _radec_geojson = [_ra - 180.0, _dec]
            document["coordinates"]["radec_geojson"] = {
                "type": "Point",
                "coordinates": _radec_geojson,
            }
            document["data"] = []
            for t, m, e, c, f in zip(
                document["hjd"],
                document["mag"],
                document["magerr"],
                document["clrcoeff"],
                document["catflags"],
            ):
                data_point = {
                    "hjd": t,
                    "mag": m,
                    "magerr": e,
                    "clrcoeffs": c,
                    "catflags": f,
                }
                # convert types for pymongo:
                for k, v in data_point.items():
                    if k in sourcedata_int_fields:
                        data_point[k] = int(data_point[k])
                    else:
                        data_point[k] = float(data_point[k])
                        if k == "mjd":
                            data_point[k] = round(data_point[k], 5)
                        else:
                            data_point[k] = round(data_point[k], 3)
                document["data"].append(data_point)
            del (
                document["hjd"],
                document["mag"],
                document["magerr"],
                document["clrcoeff"],
                document["catflags"],
            )

            return document

        # prepare docs to ingest into db:
        docs_sources = []
        for index, row in df.iterrows():
            try:
                doc = row.to_dict()
                doc = clean_up_document(doc)
                docs_sources.append(doc)
            except Exception as exception:
                log(str(exception))

            # ingest in batches
            try:
                if len(docs_sources) % batch_size == 0 and len(docs_sources) != 0:
                    if not dry_run:
                        mongo.insert_many(
                            collection=collections["sources"],
                            documents=docs_sources,
                        )
                    # flush:
                    docs_sources = []
            except Exception as exception:
                log(str(exception))

        # ingest remaining
        while len(docs_sources) > 0:
            try:
                # In case mongo crashed and disconnected, docs will accumulate in documents
                # keep on trying to insert them until successful
                if not dry_run:
                    mongo.insert_many(
                        collection=collections["sources"], documents=docs_sources
                    )
                    # flush:
                    docs_sources = []

            except Exception as e:
                traceback.print_exc()
                log(e)
                log("Failed, waiting 5 seconds to retry")
                time.sleep(5)

        mongo.client.close()

    except Exception as e:
        traceback.print_exc()
        log(e)
        # if there was an error, return without potentially deleting the file
        return

    try:
        if rm_file:
            os.remove(file_name)
    finally:
        pass


def run(
    path: str,
    tag: str = "20210401",
    num_proc: int = multiprocessing.cpu_count(),
    batch_size: int = 2048,
    rm: bool = False,
    dry_run: bool = False,
):
    """Preprocess and Ingest ZTF matchfiles into Kowalski

    :param path: local path to matchfiles
    :param tag: matchfile release time tag
    :param num_proc: number of processes for parallel ingestion
    :param batch_size: batch size for light curve data ingestion
    :param rm: remove matchfiles after ingestion?
    :param dry_run: dry run?
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
        verbose=0,
    )
    log("Successfully connected to DB")

    collections = {
        "sources": f"ZTF_public_sources_{tag}",
    }

    # create indices:
    log("Creating indices")
    if not dry_run:
        mongo.db[collections["sources"]].create_index(
            [("coordinates.radec_geojson", "2dsphere"), ("_id", pymongo.ASCENDING)],
            background=True,
        )
        mongo.db[collections["sources"]].create_index(
            [
                ("field", pymongo.ASCENDING),
                ("ccd", pymongo.ASCENDING),
                ("quad", pymongo.ASCENDING),
            ],
            background=True,
        )
        mongo.db[collections["sources"]].create_index(
            [("nobs", pymongo.ASCENDING), ("_id", pymongo.ASCENDING)], background=True
        )

    files = [str(f) for f in pathlib.Path(path).glob("ztf_*.parquet")]

    log(f"# files to process: {len(files)}")

    input_list = [(f, collections, batch_size, rm, dry_run) for f in sorted(files)]
    # for a more even job distribution:
    random.shuffle(input_list)

    with multiprocessing.Pool(processes=num_proc) as pool:
        for _ in tqdm(pool.imap(process_file, input_list), total=len(files)):
            pass


if __name__ == "__main__":
    fire.Fire(run)
