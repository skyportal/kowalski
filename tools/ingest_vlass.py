"""
This tool will digest untarred VLASS source data from https://cirada.ca/vlasscatalogueql0 and ingest the data into Kowalski.
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
    if verbose:
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
    if verbose:
        log("Successfully connected")

    collection = "VLASS_DR1"

    if verbose:
        log(f"Processing {file}")

    names = [
        "Component_name",
        "RA",
        "DEC",
        "E_RA",
        "E_DEC",
        "Total_flux",
        "E_Total_flux",
        "Peak_flux",
        "E_Peak_flux",
        "Maj",
        "E_Maj",
        "Min",
        "E_Min",
        "Duplicate_flag",
        "Quality_flag",
    ]

    for chunk_index, dataframe_chunk in enumerate(
        pd.read_csv(file, chunksize=batch_size)
    ):

        if verbose:
            log(f"{file}: processing batch # {chunk_index + 1}")

        dataframe_chunk = dataframe_chunk[names]
        dataframe_chunk = dataframe_chunk[dataframe_chunk["Duplicate_flag"] < 2]
        dataframe_chunk = dataframe_chunk[dataframe_chunk["Quality_flag"] == 0]

        batch = dataframe_chunk.to_dict(orient="records")

        bad_document_indexes = []

        for document_index, document in enumerate(batch):
            try:
                # GeoJSON for 2D indexing
                document["coordinates"] = dict()
                # string format: H:M:S, D:M:S
                document["coordinates"]["radec_str"] = [
                    deg2hms(document["RA"]),
                    deg2dms(document["DEC"]),
                ]
                # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
                _radec_geojson = [document["RA"] - 180.0, document["DEC"]]
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
    # Note CSV data file comes from https://cirada.ca/vlasscatalogueql0
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        type=str,
        default=str(pathlib.Path(__file__).parent),
        help="Local path to unzipped VLASS csv files",
    )
    parser.add_argument("-v", action="store_true", help="verbose?")
    parser.add_argument(
        "--rm", action="store_true", help="Remove CSV files after ingestion"
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

    files = ["CIRADA_VLASS1QL_table1_components_v1.csv"]

    catalog_name = "VLASS_DR1"

    log("Connecting to DB")
    m = Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        replica_set=config["database"]["replica_set"],
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

    params = [(f, catalog_name, args.batch_size, args.rm, args.v) for f in files]

    with Pool(processes=args.n_processes) as pool:
        # list+pool.imap is a trick to make tqdm work here
        list(tqdm(pool.imap(process_file, params), total=len(files)))
