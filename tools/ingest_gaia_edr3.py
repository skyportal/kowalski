import argparse
from multiprocessing.pool import Pool
import pandas as pd
import pathlib
import os
from tqdm.auto import tqdm

from utils import (
    deg2dms,
    deg2hms,
    load_config,
    log,
    Mongo,
)


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


def process_file(args):
    file, collection, batch_size, rm, verbose = args
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

    collection = "Gaia_EDR3"

    if verbose:
        log(f"processing {file}")

    for ii, dff in enumerate(pd.read_csv(file, chunksize=batch_size)):

        if verbose:
            log(f"{file}: processing batch # {ii + 1}")

        dff["_id"] = dff["source_id"].apply(lambda x: str(x))

        batch = dff.fillna("DROPMEPLEASE").to_dict(orient="records")

        # pop nulls - save space
        batch = [
            {
                kk: vv
                for kk, vv in bb.items()
                if vv not in ("DROPMEPLEASE", "NOT_AVAILABLE")
            }
            for bb in batch
        ]

        bad_doc_ind = []

        for ie, doc in enumerate(batch):
            try:
                # GeoJSON for 2D indexing
                doc["coordinates"] = dict()
                # string format: H:M:S, D:M:S
                doc["coordinates"]["radec_str"] = [
                    deg2hms(doc["ra"]),
                    deg2dms(doc["dec"]),
                ]
                # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
                _radec_geojson = [doc["ra"] - 180.0, doc["dec"]]
                doc["coordinates"]["radec_geojson"] = {
                    "type": "Point",
                    "coordinates": _radec_geojson,
                }
            except Exception as e:
                if verbose:
                    log(str(e))
                bad_doc_ind.append(ie)

        if len(bad_doc_ind) > 0:
            if verbose:
                log("Removing bad docs")
            for index in sorted(bad_doc_ind, reverse=True):
                del batch[index]

        # ingest
        mongo.insert_many(collection=collection, documents=batch)

    # disconnect from db:
    try:
        mongo.client.close()
    finally:
        if verbose:
            log("Successfully disconnected from db")

    # clean up:
    if rm:
        os.remove(file)
        if verbose:
            log(f"Successfully removed {file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        type=str,
        default=str(pathlib.Path(__file__).parent),
        help="local path to unzipped Gaia EDR3 csv files",
    )
    parser.add_argument("-v", action="store_true", help="verbose?")
    parser.add_argument(
        "--rm", action="store_true", help="remove csv files after ingestion?"
    )
    parser.add_argument(
        "--np", type=int, default=12, help="number of processes for parallel ingestion"
    )
    parser.add_argument("--bs", type=int, default=2048, help="batch size for ingestion")

    args = parser.parse_args()

    path = pathlib.Path(args.path)

    files = list(path.glob("Gaia*.csv"))

    catalog_name = "Gaia_EDR3"

    log("Connecting to DB")
    m = Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        db=config["database"]["db"],
        verbose=args.v,
    )
    log("Successfully connected")

    # create indexes:
    log("Creating indexes")
    m.db[catalog_name].create_index(
        [("coordinates.radec_geojson", "2dsphere"), ("_id", 1)], background=True
    )
    m.db[catalog_name].create_index(
        [("ra", 1), ("dec", 1), ("parallax", 1)], background=True
    )
    m.db[catalog_name].create_index(
        [
            ("parallax", 1),
            ("phot_g_mean_mag", 1),
            ("radial_velocity", 1),
            ("radial_velocity_error", 1),
        ],
        name="coughlin01",
        background=True,
    )

    params = [(f, catalog_name, args.bs, args.rm, args.v) for f in files]

    with Pool(processes=args.np) as pool:
        list(tqdm(pool.imap(process_file, params), total=len(files)))
