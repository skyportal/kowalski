"""
This tool will digest zipped IGAPS source data from http://www.star.ucl.ac.uk/IGAPS/catalogue/ and ingest the data into Kowalski.
"""

import fire
import numpy as np
import pandas as pd
from astropy.io import fits
import pathlib

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


def process_file(
    file,
    collection,
    ra_col=None,
    dec_col=None,
    batch_size=2048,
    max_docs=None,
    format="fits",
):

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

    # if the file is not an url
    if not file.startswith("http"):
        try:
            file = pathlib.Path(file).resolve(strict=True)
        except FileNotFoundError:
            log(f"File {file} not found")
            return

    log(f"Processing {file}")

    total_good_documents = 0
    total_bad_documents = 0

    if format == "fits":
        names = []
        with fits.open(file, cache=False) as hdulist:
            nhdu = 1
            names = hdulist[nhdu].columns.names
            dataframe = pd.DataFrame(np.asarray(hdulist[nhdu].data), columns=names)

        if max_docs is not None and isinstance(max_docs, int):
            dataframe = dataframe.iloc[:max_docs]

        if ra_col is None:
            try:
                ra_col = [col for col in names if col.lower() == "ra"][0]
            except IndexError:
                log("RA column not found")
                return
        else:
            # verify that the columns exist
            if ra_col not in names:
                log(f"Provided RA column {ra_col} not found")
                return

        if dec_col is None:
            try:
                dec_col = [col for col in names if col.lower() == "dec"][0]
            except IndexError:
                log("Dec column not found")
                return
        else:
            # verify that the columns exist
            if dec_col not in names:
                log(f"Provided Dec column {dec_col} not found")
                return

        for chunk_index, dataframe_chunk in dataframe.groupby(
            np.arange(len(dataframe)) // batch_size
        ):

            log(f"{file}: processing batch # {chunk_index + 1}")

            for col, dtype in dataframe_chunk.dtypes.items():
                if dtype == object:
                    dataframe_chunk[col] = dataframe_chunk[col].apply(
                        lambda x: x.decode("utf-8")
                    )

            batch = dataframe_chunk.to_dict(orient="records")
            batch = dataframe_chunk.fillna("DROPMEPLEASE").to_dict(orient="records")

            # pop nulls - save space
            batch = [
                {
                    key: value
                    for key, value in document.items()
                    if value != "DROPMEPLEASE"
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
                        deg2hms(document[ra_col]),
                        deg2dms(document[dec_col]),
                    ]
                    # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
                    _radec_geojson = [document[ra_col] - 180.0, document[dec_col]]
                    document["coordinates"]["radec_geojson"] = {
                        "type": "Point",
                        "coordinates": _radec_geojson,
                    }
                except Exception as e:
                    log(str(e))
                    bad_document_indexes.append(document_index)

            total_good_documents += len(batch) - len(bad_document_indexes)
            if len(bad_document_indexes) > 0:
                total_bad_documents += len(bad_document_indexes)
                log("Removing bad docs")
                for index in sorted(bad_document_indexes, reverse=True):
                    del batch[index]

            # ingest batch
            mongo.insert_many(collection=collection, documents=batch)

    elif format == "csv":
        last_chunk = False
        for chunk_index, dataframe_chunk in enumerate(
            pd.read_csv(file, chunksize=batch_size)
        ):
            if last_chunk:
                break
            if max_docs is not None and isinstance(max_docs, int):
                if (chunk_index + 1) * batch_size > max_docs:
                    dataframe_chunk = dataframe_chunk.iloc[: max_docs % batch_size]
                    last_chunk = True

            names = list(dataframe_chunk.columns)
            log(f"{file}: processing batch # {chunk_index + 1}")
            if ra_col is None:
                try:
                    ra_col = [col for col in names if col.lower() == "ra"][0]
                except IndexError:
                    log("No RA/Dec columns found")
                    return
            else:
                # verify that the columns exist
                if ra_col not in names:
                    log(f"Provided RA column {ra_col} not found")
                    return

            if dec_col is None:
                try:
                    dec_col = [col for col in names if col.lower() == "dec"][0]
                except IndexError:
                    log("No RA/Dec columns found")
                    return
            else:
                # verify that the columns exist
                if dec_col not in names:
                    log(f"Provided Dec column {dec_col} not found")
                    return
            # dataframe_chunk = dataframe_chunk[names]
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
                        deg2hms(document[ra_col]),
                        deg2dms(document[dec_col]),
                    ]
                    # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
                    _radec_geojson = [document[ra_col] - 180.0, document[ra_col]]
                    document["coordinates"]["radec_geojson"] = {
                        "type": "Point",
                        "coordinates": _radec_geojson,
                    }
                except Exception as e:
                    log(str(e))
                    bad_document_indexes.append(document_index)

            total_good_documents += len(batch) - len(bad_document_indexes)
            if len(bad_document_indexes) > 0:
                total_bad_documents += len(bad_document_indexes)
                log("Removing bad docs")
                for index in sorted(bad_document_indexes, reverse=True):
                    del batch[index]

            # ingest batch
            mongo.insert_many(collection=collection, documents=batch)

    else:
        log("Unknown format. Supported formats: fits, csv")
        return 0, 0
    # disconnect from db:
    try:
        mongo.client.close()
    finally:
        log("Successfully disconnected from db")

    log(f"Total good documents: {total_good_documents}")
    log(f"Total bad documents: {total_bad_documents}")
    return total_good_documents, total_bad_documents


def run(
    catalog_name: str,
    path: str = "./data/catalogs",
    ra_col: str = None,
    dec_col: str = None,
    batch_size: int = 2048,
    max_docs: int = None,
    format: str = "fits",
):
    """Pre-process and ingest catalog from fits files into Kowalski
    :param path: path to fits file
    :param num_processes:
    :return:
    """

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

    total_good_documents, total_bad_documents = process_file(
        path, catalog_name, ra_col, dec_col, batch_size, max_docs, format
    )
    return total_good_documents, total_bad_documents


if __name__ == "__main__":
    fire.Fire(run)
