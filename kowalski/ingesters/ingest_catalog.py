"""
This tool will catalogs with different formats (fits, csv, and parquet) to Kowalski
"""

import pathlib
import time

import fire
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from astropy.io import fits

import kowalski.tools.istarmap as istarmap  # noqa: F401
from kowalski.config import load_config
from kowalski.log import log
from kowalski.utils import Mongo, deg2dms, deg2hms, init_db_sync

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]

# init db if necessary
init_db_sync(config=config)


def process_file(
    file,
    collection,
    ra_col=None,
    dec_col=None,
    id_col=None,
    batch_size=2048,
    max_docs=None,
    format="fits",
):
    if format not in ("fits", "csv", "parquet"):
        log("Format not supported")
        return

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

        if id_col is not None:
            if id_col not in names:
                log(f"Provided ID column {id_col} not found")
                return
            if dataframe[id_col].isnull().values.any():
                log(f"ID column {id_col} has null values")
                return
            if not isinstance(dataframe[id_col].iloc[0], str):
                if not isinstance(dataframe[id_col].iloc[0], (int, np.integer)):
                    log(f"ID column {id_col} is not a string or an integer")
                    return
                if dataframe[id_col].iloc[0] < 0:
                    log(f"ID column {id_col} has negative values")
                    return

        if ra_col is None:
            try:
                ra_col = [col for col in names if col.lower() in ["ra", "objra"]][0]
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
                dec_col = [col for col in names if col.lower() in ["dec", "objdec"]][0]
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

            if id_col is not None:
                for document in batch:
                    if id_col not in document.keys():
                        log(f"Provided ID column {id_col} not found")
                        return
                    document["_id"] = document.pop(id_col)

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

            if id_col is not None:
                if id_col not in names:
                    log(f"Provided ID column {id_col} not found")
                    return
                # if there is any row where this is None, return
                if dataframe_chunk[id_col].isnull().values.any():
                    log(f"ID column {id_col} has null values")
                    return
                if not isinstance(dataframe_chunk[id_col].iloc[0], str):
                    if not isinstance(
                        dataframe_chunk[id_col].iloc[0], (int, np.integer)
                    ):
                        log(f"ID column {id_col} is not a string or an integer")
                        return
                    if dataframe_chunk[id_col].iloc[0] < 0:
                        log(f"ID column {id_col} has negative values")
                        return

            if ra_col is None:
                try:
                    ra_col = [col for col in names if col.lower() in ["ra", "objra"]][0]
                except IndexError:
                    log("No RA column found")
                    return
            else:
                # verify that the columns exist
                if ra_col not in names:
                    log(f"Provided RA column {ra_col} not found")
                    return

            if dec_col is None:
                try:
                    dec_col = [
                        col for col in names if col.lower() in ["dec", "objdec"]
                    ][0]
                except IndexError:
                    log("No Dec column found")
                    return
            else:
                # verify that the columns exist
                if dec_col not in names:
                    log(f"Provided Dec column {dec_col} not found")
                    return

            batch = dataframe_chunk.to_dict(orient="records")

            if id_col is not None:
                for document in batch:
                    if id_col not in document.keys():
                        log(f"Provided ID column {id_col} not found")
                        return
                    document["_id"] = document.pop(id_col)

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

    elif format == "parquet":
        df = pq.read_table(file).to_pandas()
        for name in list(df.columns):
            if name.startswith("_"):
                df.rename(columns={name: name[1:]}, inplace=True)
        names = list(df.columns)

        if id_col is not None:
            if id_col.startswith("_"):
                id_col = id_col[1:]
            if id_col not in names:
                log(f"Provided ID column {id_col} not found")
                return
            # if there is any row where this is None, return
            if df[id_col].isnull().values.any():
                log(f"ID column {id_col} has null values")
                return
            if not isinstance(df[id_col].iloc[0], str):
                if not isinstance(df[id_col].iloc[0], (int, np.integer)):
                    log(f"ID column {id_col} is not a string or an integer")
                    return
                if df[id_col].iloc[0] < 0:
                    log(f"ID column {id_col} has negative values")
                    return
        if ra_col is None:
            try:
                ra_col = [col for col in names if col.lower() in ["ra", "objra"]][0]
            except IndexError:
                log("No RA column found")
                return
        else:
            # verify that the columns exist
            if ra_col not in names:
                log(f"Provided RA column {ra_col} not found")
                return

        if dec_col is None:
            try:
                dec_col = [col for col in names if col.lower() in ["dec", "objdec"]][0]
            except IndexError:
                log("No Dec column found")
                return
        else:
            # verify that the columns exist
            if dec_col not in names:
                log(f"Provided DEC column {dec_col} not found")
                return
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
                        return
                    document["_id"] = document.pop(id_col)

                # if there is already a key called radec_geojson, then delete it
                if "radec_geojson" in document.keys():
                    del document["radec_geojson"]

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
                total_good_documents += 1
            except Exception as exception:
                total_bad_documents += 1
                log(str(exception))

            # ingest in batches
            try:
                if (
                    len(batch) % batch_size == 0
                    and len(batch) != 0
                    or len(batch) == max_docs
                ):
                    mongo.insert_many(
                        collection=collection,
                        documents=batch,
                    )
                    # flush:
                    batch = []
            except Exception as exception:
                log(str(exception))

        while len(batch) > 0:
            try:
                mongo.insert_many(collection=collection, documents=batch)
                # flush:
                batch = []
            except Exception as e:
                log(e)
                log("Failed, waiting 5 seconds to retry")
                time.sleep(5)

    else:
        log("Unknown format. Supported formats: fits, csv, parquet")
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
    id_col: str = None,
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
        path,
        catalog_name,
        ra_col,
        dec_col,
        id_col,
        batch_size,
        max_docs,
        format,
    )
    return total_good_documents, total_bad_documents


if __name__ == "__main__":
    fire.Fire(run)
