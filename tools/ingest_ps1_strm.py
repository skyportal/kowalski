"""
This tool will digest unzipped PS1-Source Types and Redshifts with Machine Learning (STRM)
from https://archive.stsci.edu/hlsps/ps1-strm/ and ingest the data into Kowalski.
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
KOWALSKI_APP_PATH = os.environ.get("KOWALSKI_APP_PATH", "/app")
config = load_config(path=KOWALSKI_APP_PATH, config_file="config.yaml")["kowalski"]


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
        srv=config["database"]["srv"],
        verbose=0,
    )
    if verbose:
        log("Successfully connected")

    collection = "PS1_STRM"

    if verbose:
        log(f"Processing {file}")

    names = [
        "objID",
        "uniquePspsOBid",
        "raMean",
        "decMean",
        "l",
        "b",
        "class",
        "prob_Galaxy",
        "prob_Star",
        "prob_QSO",
        "extrapolation_Class",
        "cellDistance_Class",
        "cellID_Class",
        "z_phot",
        "z_photErr",
        "z_phot0",
        "extrapolation_Photoz",
        "cellDistance_Photoz",
        "cellID_Photoz",
    ]

    for chunk_index, dataframe_chunk in enumerate(
        pd.read_csv(
            file,
            chunksize=batch_size,
            names=names,
        )
    ):

        if verbose:
            log(f"{file}: processing batch # {chunk_index + 1}")

        dataframe_chunk = dataframe_chunk.replace(-999, "DROPMEPLEASE")

        dataframe_chunk["_id"] = dataframe_chunk["uniquePspsOBid"].apply(
            lambda x: str(x)
        )  # unique id uniquePspsOBid from PS1 table

        batch = dataframe_chunk.fillna("DROPMEPLEASE").to_dict(orient="records")

        # pop nulls - save space
        batch = [
            {
                key: value
                for key, value in document.items()
                if (value not in ("DROPMEPLEASE", "NOT_AVAILABLE"))
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
                    deg2hms(document["raMean"]),
                    deg2dms(document["decMean"]),
                ]
                # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
                _radec_geojson = [document["raMean"] - 180.0, document["decMean"]]
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
    # Note CSV data files will be taken from https://archive.stsci.edu/hlsps/ps1-strm/
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        type=str,
        default=str(pathlib.Path(__file__).parent),
        help="Local path to unzipped PS1-STRM csv files",
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

    files = list(path.glob("hlsp*.csv"))  # all PS1_STRM files start with hlsp_...

    catalog_name = "PS1_STRM"

    log("Connecting to DB")
    m = Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        replica_set=config["database"]["replica_set"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        db=config["database"]["db"],
        srv=config["database"]["srv"],
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
        [
            (
                "class",
                1,
            ),  # the class assigned to the source, using a fiducial decision boundary (i.e GALAXY, STAR, QSO, UNSURE)
            (
                "prob_Galaxy",
                1,
            ),  # the probability-like neural network output for the galaxy class
            (
                "prob_Star",
                1,
            ),  # the probability-like neural network output for the star class
            ("z_phot", 1),  # the Monte-Carlo photometric redshift estimate
            ("z_photErr", 1),  # the calibrated redshift error estimate
        ],
        name="class__prob_Galaxy__prob_QSO__z_phot__z_photErr",
        background=True,
    )

    params = [(f, catalog_name, args.batch_size, args.rm, args.v) for f in files]

    with Pool(processes=args.n_processes) as pool:
        # list+pool.imap is a trick to make tqdm work here
        list(tqdm(pool.imap(process_file, params), total=len(files)))
