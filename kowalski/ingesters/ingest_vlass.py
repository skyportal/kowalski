"""
This tool will digest untarred VLASS source data from https://cirada.ca/vlasscatalogueql0 and ingest the data into Kowalski.
"""

import fire
import glob
import multiprocessing as mp
import pandas as pd
import os
from tqdm.auto import tqdm

import kowalski.tools.istarmap as istarmap  # noqa: F401
from kowalski.utils import (
    deg2dms,
    deg2hms,
    init_db_sync,
    load_config,
    log,
    Mongo,
)

""" load config and secrets """

config = load_config(config_file="config.yaml")["kowalski"]

# init db if necessary
init_db_sync(config=config)


def process_file(file, collection, batch_size):

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

    collection = "VLASS_DR1"

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
                log(str(e))
                bad_document_indexes.append(document_index)

        if len(bad_document_indexes) > 0:
            log("Removing bad docs")
            for index in sorted(bad_document_indexes, reverse=True):
                del batch[index]

        # ingest batch
        mongo.insert_many(collection=collection, documents=batch)

    # disconnect from db:
    try:
        mongo.client.close()
    finally:
        log("Successfully disconnected from db")


def run(
    path: str = "./",
    num_processes: int = 1,
    batch_size: int = 2048,
):
    """Pre-process and ingest VLASS catalog
    :param path: path to CSV data file (~3.3 GB untarred)
                 see https://cirada.ca/vlasscatalogueql0
    :param num_processes:
    :return:
    """

    files = glob.glob(os.path.join(path, "CIRADA*.csv"))

    catalog_name = "VLASS_DR1"

    log("Connecting to DB")
    m = Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        replica_set=config["database"]["replica_set"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        db=config["database"]["db"],
        srv=config["database"]["srv"],
    )
    log("Successfully connected")

    # Create indexes in the database:
    log("Creating indexes")
    # 2D position on the sphere, ID:
    m.db[catalog_name].create_index(
        [("coordinates.radec_geojson", "2dsphere"), ("_id", 1)], background=True
    )

    input_list = [(f, catalog_name, batch_size) for f in files]

    with mp.Pool(processes=num_processes) as p:
        for _ in tqdm(p.istarmap(process_file, input_list), total=len(input_list)):
            pass


if __name__ == "__main__":
    fire.Fire(run)
