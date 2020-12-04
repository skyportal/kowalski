"""
A tool to digest unzipped Gaia EDR3 CSV files from http://cdn.gea.esac.esa.int/Gaia/gedr3/gaia_source
and ingest the data into Kowalski.
"""

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
        log(f"Processing {file}")

    for chunk_index, dataframe_chunk in enumerate(
        pd.read_csv(file, chunksize=batch_size)
    ):

        if verbose:
            log(f"{file}: processing batch # {chunk_index + 1}")

        dataframe_chunk["_id"] = dataframe_chunk["source_id"].apply(lambda x: str(x))

        batch = dataframe_chunk.fillna("DROPMEPLEASE").to_dict(orient="records")

        # pop nulls - save space
        batch = [
            {
                key: value
                for key, value in document.items()
                if value not in ("DROPMEPLEASE", "NOT_AVAILABLE")
            }
            for document in batch
        ]

        bad_document_indexes = []

        for document_index, document in enumerate(batch):
            try:
                # GeoJSON for 2D indexing
                document["coordinates"] = dict()
                # string format: H:M:S, D:M:S
                document["coordinates"]["radec_str"] = [
                    deg2hms(document["ra"]),
                    deg2dms(document["dec"]),
                ]
                # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
                _radec_geojson = [document["ra"] - 180.0, document["dec"]]
                document["coordinates"]["radec_geojson"] = {
                    "type": "Point",
                    "coordinates": _radec_geojson,
                }
            except Exception as e:
                if verbose:
                    log(str(e))
                bad_document_indexes.append(document_index)

        if len(bad_document_indexes) > 0:
            if verbose:
                log("Removing bad docs")
            for index in sorted(bad_document_indexes, reverse=True):
                del batch[index]

        # ingest batch
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
    # Note: fetch CSV files from http://cdn.gea.esac.esa.int/Gaia/gedr3/gaia_source
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        type=str,
        default=str(pathlib.Path(__file__).parent),
        help="Local path to unzipped Gaia EDR3 csv files",
    )
    parser.add_argument("-v", action="store_true", help="verbose?")
    parser.add_argument(
        "--rm", action="store_true", help="Remove CSV files after ingestion?"
    )
    parser.add_argument(
        "--n_processes",
        type=int,
        default=12,
        help="Number of processes for parallel ingestion",
    )
    parser.add_argument(
        "--batch_size", type=int, default=2048, help="batch size for ingestion"
    )

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

    # Create indexes in the database:
    log("Creating indexes")
    # 2D position on the sphere, ID:
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
        name="parallax__g_mag__rv__rv_error",
        background=True,
    )

    params = [(f, catalog_name, args.batch_size, args.rm, args.v) for f in files]

    with Pool(processes=args.n_processes) as pool:
        # list+pool.imap is a trick to make tqdm work here
        list(tqdm(pool.imap(process_file, params), total=len(files)))
