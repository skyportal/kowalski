"""
This tool will catalogs with different formats (fits, csv, and parquet) to Kowalski
"""

import multiprocessing
import os
import pathlib
import random
import time
import traceback
from copy import deepcopy
from typing import Sequence

import fire
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from astropy.io import fits
from tqdm.auto import tqdm

import kowalski.tools.istarmap as istarmap  # noqa: F401
from kowalski.config import load_config
from kowalski.log import log
from kowalski.utils import Mongo, deg2dms, deg2hms, init_db_sync

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]

# init db if necessary
init_db_sync(config=config)


def get_mongo_client() -> Mongo:
    n_retries = 0
    while n_retries < 10:
        try:
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
        except Exception as e:
            traceback.print_exc()
            log(e)
            log("Failed to connect to the database, waiting 15 seconds before retry")
            time.sleep(15)
            continue
        return mongo
    raise Exception("Failed to connect to the database after 10 retries")


def process_fits_file(
    file,
    collection,
    ra_col,
    dec_col,
    id_col,
    position_from_catalog,
    position_from_catalog_name,
    position_from_catalog_ra_col,
    position_from_catalog_dec_col,
    position_from_catalog_id_col,
    batch_size,
    max_docs,
    mongo,
):
    total_good_documents = 0
    total_bad_documents = 0

    names = []
    with fits.open(file, cache=False) as hdulist:
        nhdu = 1
        names = hdulist[nhdu].columns.names
        dataframe = pd.DataFrame(np.asarray(hdulist[nhdu].data), columns=names)
        for col in dataframe.columns:
            if dataframe[col].dtype in [">f4", ">f8"]:
                dataframe[col] = dataframe[col].astype(float)
            elif dataframe[col].dtype in [">i2", ">i4", ">i8"]:
                dataframe[col] = dataframe[col].astype(int)

    if max_docs is not None and isinstance(max_docs, int):
        dataframe = dataframe.iloc[:max_docs]

    if id_col is not None:
        if id_col not in names:
            log(f"Provided ID column {id_col} not found")
            return 0, 0
        if dataframe[id_col].isnull().values.any():
            log(f"ID column {id_col} has null values")
            return 0, 0
        if not isinstance(dataframe[id_col].iloc[0], str):
            if not isinstance(dataframe[id_col].iloc[0], (int, np.integer)):
                log(f"ID column {id_col} is not a string or an integer")
                return 0, 0
            if dataframe[id_col].iloc[0] < 0:
                log(f"ID column {id_col} has negative values")
                return 0, 0

    if not position_from_catalog and ra_col is None:
        try:
            ra_col = [col for col in names if col.lower() in ["ra", "objra"]][0]
        except IndexError:
            log("RA column not found")
            return 0, 0
    # verify that the columns exist
    elif not position_from_catalog and ra_col not in names:
        log(f"Provided RA column {ra_col} not found")
        return 0, 0

    if not position_from_catalog and dec_col is None:
        try:
            dec_col = [col for col in names if col.lower() in ["dec", "objdec"]][0]
        except IndexError:
            log("Dec column not found")
            return 0, 0
    # verify that the columns exist
    elif not position_from_catalog and dec_col not in names:
        log(f"Provided Dec column {dec_col} not found")
        return 0, 0

    if position_from_catalog:
        if id_col is None:
            log(
                "No ID column (in the catalog to ingest) provided to get positions from"
            )
            return 0, 0
        if position_from_catalog_name is None:
            log("No catalog name provided to get positions from")
            return 0, 0
        if position_from_catalog_ra_col is None:
            log("No RA column provided to get positions from")
            return 0, 0
        if position_from_catalog_dec_col is None:
            log("No Dec column provided to get positions from")
            return 0, 0
        if position_from_catalog_id_col is None:
            log("No ID column provided to get positions from")
            return 0, 0
        if position_from_catalog_name == collection:
            log("Cannot get positions from the same collection")
            return 0, 0

        if ra_col is None:
            ra_col = "ra"
        if dec_col is None:
            dec_col = "dec"

    for chunk_index, dataframe_chunk in dataframe.groupby(
        np.arange(len(dataframe)) // batch_size
    ):

        for col, dtype in dataframe_chunk.dtypes.items():
            if dtype == object:
                dataframe_chunk[col] = dataframe_chunk[col].apply(
                    lambda x: x.decode("utf-8")
                )

        batch = dataframe_chunk.to_dict(orient="records")
        batch = dataframe_chunk.fillna("DROPMEPLEASE").to_dict(orient="records")

        # pop nulls - save space
        batch = [
            {key: value for key, value in document.items() if value != "DROPMEPLEASE"}
            for document in batch
        ]

        if id_col is not None:
            for document in batch:
                if id_col not in document.keys():
                    log(f"Provided ID column {id_col} not found")
                    return 0, 0
                document["_id"] = document.pop(id_col)

        bad_document_indexes = []

        for document_index, document in enumerate(batch):
            try:
                if position_from_catalog:
                    # get position from other catalog (by id) if the catalog to ingest does not have it
                    try:
                        position = mongo.db[position_from_catalog_name].find_one(
                            filter={position_from_catalog_id_col: document["_id"]},
                            projection={
                                position_from_catalog_ra_col: 1,
                                position_from_catalog_dec_col: 1,
                                "_id": 0,
                            },
                        )
                        if position is not None and {
                            position_from_catalog_ra_col,
                            position_from_catalog_dec_col,
                        }.issubset(position.keys()):
                            document[ra_col] = position[position_from_catalog_ra_col]
                            document[dec_col] = position[position_from_catalog_dec_col]
                        else:
                            log(
                                f"Failed to get position for {document['_id']} from {position_from_catalog_name} (with {position_from_catalog_id_col} {id_col} {ra_col} {dec_col})"
                            )
                            bad_document_indexes.append(document_index)
                            continue
                    except Exception as e:
                        log(
                            f"Failed to get position for {document['_id']} from {position_from_catalog_name} (with {position_from_catalog_id_col} {id_col} {ra_col} {dec_col}): {str(e)}"
                        )
                        bad_document_indexes.append(document_index)
                        continue
                # GeoJSON for 2D indexing
                document["coordinates"] = dict()
                # string format: H:M:S, D:M:S
                if document[ra_col] == 360.0:
                    document[ra_col] = 0.0
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

                # if the entry is a string, strip it
                # if it ends up being an empty string, it will be dropped
                keys_to_pop = []
                for key, value in document.items():
                    if isinstance(value, str):
                        document[key] = value.strip()
                        if not document[key] or len(document[key]) == 0:
                            keys_to_pop.append(key)
                for key in keys_to_pop:
                    document.pop(key)
            except Exception as e:
                traceback.print_exc()
                log(f"Failed to process document {document_index}: {str(e)}")
                bad_document_indexes.append(document_index)

        total_good_documents += len(batch) - len(bad_document_indexes)
        if len(bad_document_indexes) > 0:
            total_bad_documents += len(bad_document_indexes)
            for index in sorted(bad_document_indexes, reverse=True):
                del batch[index]

        # ingest batch
        mongo.insert_many(collection=collection, documents=batch)

    return total_good_documents, total_bad_documents


def process_csv_file(
    file,
    collection,
    ra_col,
    dec_col,
    id_col,
    position_from_catalog,
    position_from_catalog_name,
    position_from_catalog_ra_col,
    position_from_catalog_dec_col,
    position_from_catalog_id_col,
    batch_size,
    max_docs,
    mongo,
):
    total_good_documents = 0
    total_bad_documents = 0

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

        if id_col is not None:
            if id_col not in names:
                log(f"Provided ID column {id_col} not found")
                return 0, 0
            # if there is any row where this is None, return
            if dataframe_chunk[id_col].isnull().values.any():
                log(f"ID column {id_col} has null values")
                return 0, 0
            if not isinstance(dataframe_chunk[id_col].iloc[0], str):
                if not isinstance(dataframe_chunk[id_col].iloc[0], (int, np.integer)):
                    log(f"ID column {id_col} is not a string or an integer")
                    return 0, 0
                if dataframe_chunk[id_col].iloc[0] < 0:
                    log(f"ID column {id_col} has negative values")
                    return 0, 0

        if not position_from_catalog and ra_col is None:
            try:
                ra_col = [col for col in names if col.lower() in ["ra", "objra"]][0]
            except IndexError:
                log("No RA column found")
                return 0, 0
        elif not position_from_catalog and ra_col not in names:
            # verify that the columns exist
            if ra_col not in names:
                log(f"Provided RA column {ra_col} not found")
                return 0, 0

        if not position_from_catalog and dec_col is None:
            try:
                dec_col = [col for col in names if col.lower() in ["dec", "objdec"]][0]
            except IndexError:
                log("No Dec column found")
                return 0, 0
        elif not position_from_catalog and dec_col not in names:
            # verify that the columns exist
            if dec_col not in names:
                log(f"Provided Dec column {dec_col} not found")
                return 0, 0

        if position_from_catalog:
            if id_col is None:
                log(
                    "No ID column (in the catalog to ingest) provided to get positions from"
                )
                return 0, 0
            if position_from_catalog_name is None:
                log("No catalog name provided to get positions from")
                return 0, 0
            if position_from_catalog_ra_col is None:
                log("No RA column provided to get positions from")
                return 0, 0
            if position_from_catalog_dec_col is None:
                log("No Dec column provided to get positions from")
                return 0, 0
            if position_from_catalog_id_col is None:
                log("No ID column provided to get positions from")
                return 0, 0
            if position_from_catalog_name == collection:
                log("Cannot get positions from the same collection")
                return 0, 0

            if ra_col is None:
                ra_col = "ra"
            if dec_col is None:
                dec_col = "dec"

        batch = dataframe_chunk.to_dict(orient="records")

        if id_col is not None:
            for document in batch:
                if id_col not in document.keys():
                    log(f"Provided ID column {id_col} not found")
                    return 0, 0
                document["_id"] = document.pop(id_col)

        bad_document_indexes = []

        for document_index, document in enumerate(batch):
            try:
                if position_from_catalog:
                    # get position from other catalog (by id) if the catalog to ingest does not have it
                    try:
                        position = mongo.db[position_from_catalog_name].find_one(
                            filter={position_from_catalog_id_col: document["_id"]},
                            projection={
                                position_from_catalog_ra_col: 1,
                                position_from_catalog_dec_col: 1,
                                "_id": 0,
                            },
                        )
                        if position is not None and {
                            position_from_catalog_ra_col,
                            position_from_catalog_dec_col,
                        }.issubset(position.keys()):
                            document[ra_col] = position[position_from_catalog_ra_col]
                            document[dec_col] = position[position_from_catalog_dec_col]
                        else:
                            log(
                                f"Failed to get position for {document['_id']} from {position_from_catalog_name} (with {position_from_catalog_id_col} {id_col} {ra_col} {dec_col})"
                            )
                            bad_document_indexes.append(document_index)
                            continue
                    except Exception as e:
                        log(
                            f"Failed to get position for {document['_id']} from {position_from_catalog_name} (with {position_from_catalog_id_col} {id_col} {ra_col} {dec_col}): {str(e)}"
                        )
                        bad_document_indexes.append(document_index)
                        continue
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
            for index in sorted(bad_document_indexes, reverse=True):
                del batch[index]

        # ingest batch
        mongo.insert_many(collection=collection, documents=batch)

        return total_good_documents, total_bad_documents


def process_parquet_file(
    file,
    collection,
    ra_col,
    dec_col,
    id_col,
    position_from_catalog,
    position_from_catalog_name,
    position_from_catalog_ra_col,
    position_from_catalog_dec_col,
    position_from_catalog_id_col,
    batch_size,
    max_docs,
    mongo,
):
    total_good_documents = 0
    total_bad_documents = 0

    df: pd.DataFrame = pq.read_table(file).to_pandas()
    for name in list(df.columns):
        if name.startswith("_"):
            df.rename(columns={name: name[1:]}, inplace=True)
    names = list(df.columns)

    if id_col is not None:
        if id_col.startswith("_"):
            id_col = id_col[1:]
        if id_col not in names:
            log(f"Provided ID column {id_col} not found")
            return 0, 0
        # if there is any row where this is None, return
        if df[id_col].isnull().values.any():
            log(f"ID column {id_col} has null values")
            return 0, 0
        if not isinstance(df[id_col].iloc[0], str):
            if not isinstance(df[id_col].iloc[0], (int, np.integer)):
                log(f"ID column {id_col} is not a string or an integer")
                return 0, 0
            if df[id_col].iloc[0] < 0:
                log(f"ID column {id_col} has negative values")
                return 0, 0

    if not position_from_catalog and ra_col is None:
        try:
            ra_col = [col for col in names if col.lower() in ["ra", "objra"]][0]
        except IndexError:
            log("No RA column found")
            return 0, 0
    elif not position_from_catalog and ra_col not in names:
        # verify that the columns exist
        if ra_col not in names:
            log(f"Provided RA column {ra_col} not found")
            return 0, 0

    if not position_from_catalog and dec_col is None:
        try:
            dec_col = [col for col in names if col.lower() in ["dec", "objdec"]][0]
        except IndexError:
            log("No Dec column found")
            return 0, 0
    elif not position_from_catalog and dec_col not in names:
        # verify that the columns exist
        if dec_col not in names:
            log(f"Provided DEC column {dec_col} not found")
            return 0, 0

    if position_from_catalog:
        if id_col is None:
            log(
                "No ID column (in the catalog to ingest) provided to get positions from"
            )
            return 0, 0
        if position_from_catalog_name is None:
            log("No catalog name provided to get positions from")
            return 0, 0
        if position_from_catalog_ra_col is None:
            log("No RA column provided to get positions from")
            return 0, 0
        if position_from_catalog_dec_col is None:
            log("No Dec column provided to get positions from")
            return 0, 0
        if position_from_catalog_id_col is None:
            log("No ID column provided to get positions from")
            return 0, 0
        if position_from_catalog_name == collection:
            log("Cannot get positions from the same collection")
            return 0, 0

        if ra_col is None:
            ra_col = "ra"
        if dec_col is None:
            dec_col = "dec"

    batch = []

    def convert_nparray_to_list(value):
        if isinstance(value, np.ndarray):
            return [convert_nparray_to_list(v) for v in value]
        elif isinstance(value, np.integer):
            return int(value)
        elif isinstance(value, np.floating):
            return float(value)
        elif isinstance(value, np.bool_):
            return bool(value)
        elif pd.isna(value):
            return None
        else:
            return value

    for row in df.itertuples():
        if max_docs and total_good_documents + total_bad_documents >= max_docs:
            break
        try:
            document = {}
            # drop any value with NAType
            for k, v in row._asdict().items():
                if k == "Index":
                    continue
                if isinstance(v, (pd.core.series.Series, np.ndarray)):
                    # recursively convert np arrays and series to lists
                    try:
                        document[k] = convert_nparray_to_list(v)
                    except Exception as e:
                        log(f"Failed to convert {k} to list: {str(e)}")
                # if its a numerical type other than the python ones (like np.int64), convert to python type
                elif isinstance(v, np.integer):
                    document[k] = int(v)
                elif isinstance(v, np.floating):
                    document[k] = float(v)
                elif isinstance(v, np.bool_):
                    document[k] = bool(v)
                elif pd.isna(v):
                    document[k] = None
                else:
                    document[k] = v

            if id_col is not None:
                if id_col not in document.keys():
                    log(f"Provided ID column {id_col} not found")
                    return total_good_documents, total_bad_documents
                document["_id"] = document.pop(id_col)

            # if there is already a key called radec_geojson, then delete it
            if "radec_geojson" in document.keys():
                del document["radec_geojson"]

            if position_from_catalog:
                # get position from other catalog (by id) if the catalog to ingest does not have it
                try:
                    position = mongo.db[position_from_catalog_name].find_one(
                        filter={position_from_catalog_id_col: document["_id"]},
                        projection={
                            position_from_catalog_ra_col: 1,
                            position_from_catalog_dec_col: 1,
                            "_id": 0,
                        },
                    )
                    if position is not None and {
                        position_from_catalog_ra_col,
                        position_from_catalog_dec_col,
                    }.issubset(position.keys()):
                        document[ra_col] = position[position_from_catalog_ra_col]
                        document[dec_col] = position[position_from_catalog_dec_col]
                    else:
                        log(
                            f"Failed to get position for {document['_id']} from {position_from_catalog_name} (with {position_from_catalog_id_col} {id_col} {ra_col} {dec_col})"
                        )
                        continue
                except Exception as e:
                    log(
                        f"Failed to get position for {document['_id']} from {position_from_catalog_name} (with {position_from_catalog_id_col} {id_col} {ra_col} {dec_col}): {str(e)}"
                    )
                    continue

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
            batch.append(document)
        except Exception as exception:
            total_bad_documents += 1
            log(str(exception))
            continue

        # ingest in batches
        try:
            if (len(batch) % batch_size == 0 and len(batch) != 0) or len(
                batch
            ) == max_docs:
                n_retries = 0
                while n_retries < 10:
                    mongo.insert_many(
                        collection=collection,
                        documents=batch,
                    )
                    if id_col is not None:
                        ids = [doc["_id"] for doc in batch]
                        count = mongo.db[collection].count_documents(
                            {"_id": {"$in": ids}}
                        )
                        if count == len(batch):
                            total_good_documents += len(batch)
                            break
                        n_retries += 1
                        time.sleep(6)
                        mongo.close()
                        mongo = get_mongo_client()
                    else:
                        total_good_documents += len(batch)
                        break

                if n_retries == 10:
                    log(
                        f"Failed to ingest batch for {file} after {n_retries} retries, skipping"
                    )
                    total_bad_documents += len(batch)

                # flush:
                batch = []
        except Exception as e:
            log(str(e))

    while len(batch) > 0:
        try:
            n_retries = 0
            while n_retries < 10:
                mongo.insert_many(collection=collection, documents=batch)
                if id_col is not None:
                    ids = [doc["_id"] for doc in batch]
                    count = mongo.db[collection].count_documents({"_id": {"$in": ids}})
                    if count == len(batch):
                        total_good_documents += len(batch)
                        break
                    n_retries += 1
                    time.sleep(6)
                    mongo.close()
                    mongo = get_mongo_client()
                else:
                    total_good_documents += len(batch)
                    break

            if n_retries == 10:
                log(
                    f"Failed to ingest batch for {file} after {n_retries} retries, skipping and not deleting file"
                )
                total_bad_documents += len(batch)
            # flush:
            batch = []
        except Exception as e:
            log(str(e))
            log("Failed, waiting 6 seconds to retry")
            time.sleep(6)
            mongo.close()
            mongo = get_mongo_client()

    return total_good_documents, total_bad_documents


def process_file(argument_list: Sequence):
    (
        file,
        collection,
        ra_col,
        dec_col,
        id_col,
        position_from_catalog,
        position_from_catalog_name,
        position_from_catalog_ra_col,
        position_from_catalog_dec_col,
        position_from_catalog_id_col,
        batch_size,
        max_docs,
        rm,
        format,
    ) = argument_list

    log(f"Processing {file}")
    if format not in ("fits", "csv", "parquet"):
        log("Format not supported")
        return 0, 0

    mongo = get_mongo_client()

    # if the file is not an url
    if not file.startswith("http"):
        try:
            file = pathlib.Path(file).resolve(strict=True)
        except FileNotFoundError:
            log(f"File {file} not found")
            return 0, 0

    total_good_documents = 0
    total_bad_documents = 0

    if format == "fits":
        try:
            total_good_documents, total_bad_documents = process_fits_file(
                file,
                collection,
                ra_col,
                dec_col,
                id_col,
                position_from_catalog,
                position_from_catalog_name,
                position_from_catalog_ra_col,
                position_from_catalog_dec_col,
                position_from_catalog_id_col,
                batch_size,
                max_docs,
                mongo,
            )
        except Exception as e:
            traceback.print_exc()
            log(f"Failed to process fits: {e}")
    elif format == "csv":
        try:
            total_good_documents, total_bad_documents = process_csv_file(
                file,
                collection,
                ra_col,
                dec_col,
                id_col,
                position_from_catalog,
                position_from_catalog_name,
                position_from_catalog_ra_col,
                position_from_catalog_dec_col,
                position_from_catalog_id_col,
                batch_size,
                max_docs,
                mongo,
            )
        except Exception as e:
            traceback.print_exc()
            log(f"Failed to process csv: {e}")
    elif format == "parquet":
        try:
            total_good_documents, total_bad_documents = process_parquet_file(
                file,
                collection,
                ra_col,
                dec_col,
                id_col,
                position_from_catalog,
                position_from_catalog_name,
                position_from_catalog_ra_col,
                position_from_catalog_dec_col,
                position_from_catalog_id_col,
                batch_size,
                max_docs,
                mongo,
            )
        except Exception as e:
            traceback.print_exc()
            log(f"Failed to process parquet: {e}")
    else:
        log("Unknown format. Supported formats: fits, csv, parquet")
        return 0, 0
    # disconnect from db:
    try:
        mongo.client.close()
    except Exception as e:
        log(f"Failed to disconnect from db: {e}")

    if total_good_documents + total_bad_documents == 0:
        log("No documents ingested")
    if total_bad_documents > 0:
        log(f"Failed to ingest {total_bad_documents} documents")

    if total_bad_documents == 0 and total_good_documents > 0 and rm is True:
        try:
            os.remove(pathlib.Path(file))
        except Exception as e:
            log(f"Failed to remove original file: {e}")
    return total_good_documents, total_bad_documents


def get_file_ids(argument_list: Sequence):
    file, id_col, format = argument_list

    ids = []
    if format == "fits":
        with fits.open(file, cache=False) as hdulist:
            nhdu = 1
            names = hdulist[nhdu].columns.names
            # first check if the id_col is in the names
            if id_col not in names:
                raise Exception(f"Provided ID column {id_col} not found in file {file}")
            dataframe = pd.DataFrame(np.asarray(hdulist[nhdu].data), columns=names)
            ids = list(dataframe[id_col])
    elif format == "csv":
        dataframe = pd.read_csv(file)
        if id_col not in dataframe.columns:
            raise Exception(f"Provided ID column {id_col} not found in file {file}")
        ids = list(dataframe[id_col])
    elif format == "parquet":
        df = pq.read_table(file).to_pandas()
        for name in list(df.columns):
            if name.startswith("_"):
                df.rename(columns={name: name[1:]}, inplace=True)
        if id_col.startswith("_"):
            id_col = id_col[1:]
        if id_col not in df.columns:
            raise Exception(f"Provided ID column {id_col} not found in file {file}")
        ids = list(df[id_col])
    else:
        raise Exception(f"Unknown format {format}")

    return ids


def verify_ids(files: list, id_col: str, format: str, num_proc: int = 4):
    ids_per_file = {}
    files_copy = deepcopy(files)

    with multiprocessing.Pool(processes=num_proc) as pool:
        with tqdm(total=len(files)) as pbar:
            for result in pool.imap_unordered(
                get_file_ids, [(file, id_col, format) for file in files]
            ):
                file = files_copy.pop(0)
                ids_per_file[file] = result
                pbar.update(1)

    # now we have a list of all the ids in all the files
    # we want to make sure that all the ids are unique
    # if they are not, then we want to print out the file names concerned, and the ids concerned
    # and then exit the program

    ids = []
    for file in files:
        ids += ids_per_file[file]

    log(f"in total, we found {len(set(ids))} unique IDs out of {len(ids)} IDs")

    if len(ids) != len(set(ids)):
        # we have duplicate ids
        # we want to print out the file names concerned, and the ids concerned
        # and then exit the program
        duplicate_ids = []
        for id in ids:
            if ids.count(id) > 1:
                duplicate_ids.append(id)

        for file in files:
            file_ids = ids_per_file[file]
            for id in file_ids:
                if id in duplicate_ids:
                    log(f"Duplicate ID {id} found in file {file}")
        log(
            f"{len(duplicate_ids)} duplicate IDs found. Please make sure that all the IDs are unique across all files before ingesting"
        )
        raise Exception(
            "Duplicate IDs found. Please make sure that all the IDs are unique across all files before ingesting"
        )

    return


def run(
    catalog_name: str,
    path: str = "./data/catalogs",
    ra_col: str = None,
    dec_col: str = None,
    id_col: str = None,
    num_proc: int = multiprocessing.cpu_count(),
    batch_size: int = 2048,
    max_docs: int = None,
    rm: bool = False,
    format: str = "fits",
    verify_only: bool = False,
    skip_verify: bool = False,
    position_from_catalog: bool = False,
    position_from_catalog_name: str = None,
    position_from_catalog_ra_col: str = None,
    position_from_catalog_dec_col: str = None,
    position_from_catalog_id_col: str = None,
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

    # grab all the files in the directory and subdirectories:
    files = []
    if os.path.isfile(path):
        files.append(path)
    else:
        for root, dirnames, filenames in os.walk(path):
            files += [os.path.join(root, f) for f in filenames if f.endswith(format)]

    if id_col is not None and not skip_verify:
        verify_ids(files, id_col, format, num_proc=num_proc)

    if verify_only:
        return

    input_list = [
        (
            file,
            catalog_name,
            ra_col,
            dec_col,
            id_col,
            position_from_catalog,
            position_from_catalog_name,
            position_from_catalog_ra_col,
            position_from_catalog_dec_col,
            position_from_catalog_id_col,
            batch_size,
            max_docs,
            rm,
            format,
        )
        for file in sorted(files)
    ]
    random.shuffle(input_list)

    total_good_documents, total_bad_documents = 0, 0
    log(f"Processing {len(files)} files with {num_proc} processes")
    with multiprocessing.Pool(processes=num_proc) as pool:
        with tqdm(total=len(files)) as pbar:
            for result in pool.imap_unordered(process_file, input_list):
                total_good_documents += result[0]
                total_bad_documents += result[1]
                pbar.update(1)

    log(f"Successfully ingested {total_good_documents} documents")
    log(f"Failed to ingest {total_bad_documents} documents")

    return total_good_documents, total_bad_documents


if __name__ == "__main__":
    fire.Fire(run)
