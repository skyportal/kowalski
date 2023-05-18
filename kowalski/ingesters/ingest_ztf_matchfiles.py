import datetime
import fire
import multiprocessing
import numpy as np
import os
import pandas as pd
import pathlib
import pymongo
import pytz
import random
import tables
import time
from tqdm import tqdm
import traceback
from typing import Sequence

from kowalski.utils import (
    ccd_quad_to_rc,
    deg2dms,
    deg2hms,
    init_db_sync,
    Mongo,
)
from kowalski.config import load_config
from kowalski.log import log


""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]
init_db_sync(config=config)


def utc_now():
    return datetime.datetime.now(pytz.utc)


filters = {"zg": 1, "zr": 2, "zi": 3}

sources_int_fields = (
    "matchid",
    "ngoodobs",
    "ngoodobsrel",
    "nmedianbufferrange",
    "nobs",
    "nobsrel",
    "npairposslope",
)

sourcedata_int_fields = ("catflags", "expid", "programid")
sources_fields_to_exclude = [
    "medmagerr",
    "nmedianbufferrange",
    "smallkurtosis",
    "magrms",
    "medianabsdev",
    "maxmag",
    "nconsecfrommeanbystd",
    "medianmag",
    "npairposslope",
    "ngoodobs",
    "stetsonk",
    "nobsrel",
    "lineartrend",
    "z",
    "minmag",
    "nconsecabovemeanbystd",
    "ngoodobsrel",
    "weightedmagrms",
    "x",
    "nbelowmeanbystd",
    "nabovemeanbystd",
    "refsnr",
    "nconsecbelowmeanbystd",
    "con",
    "astrometricrms",
    "skewness",
    "maxslope",
    "weightedmeanmag",
    "stetsonj",
    "y",
    "chisq",
    "uncalibmeanmag",
    "percentiles",
]


def track_failures(tag, file_name, exception):
    # we want the process file method to be able to save to a csv file
    # the file name and exception that occured if any
    # this way we can track which files failed to process, why, and reprocess them later
    # the csv file will be used by the ingest_ztf_matchfiles.py script
    # to reprocess the failed files

    # path is extracted from the file name (e.g. /data/20230309/ztf_123456_zg_c01_q1_match.pytable)
    # will give us /data/20230309
    path = "/".join(file_name.split("/")[:-1])
    output = f"{path}/ztf_matchfiles_failures_{str(tag)}.csv"
    if not os.path.isfile(output):
        with open(output, "w") as f:
            f.write("file_name,exception\n")

    # both need to be one line strings to be read properly
    # as the exception can contain commas, we need to replace them with semicolons
    with open(output, "a") as f:
        exception = str(exception).replace(",", ";").replace("\n", " ")
        f.write(f"{file_name},{exception}\n")


def process_file(argument_list: Sequence):
    file_name, collections, tag, batch_size, rm_file, dry_run = argument_list
    try:
        # connect to MongoDB:
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

        with tables.open_file(file_name, "r+") as f:
            group = f.root.matches
            ff_basename = pathlib.Path(file_name).name
            # base id:
            _, field, filt, ccd, quadrant, _ = ff_basename.split("_")
            field = int(field)
            filt = filters[filt]
            ccd = int(ccd[1:])
            quadrant = int(quadrant[1:])
            readout_channel = ccd_quad_to_rc(ccd=ccd, quad=quadrant)
            baseid = int(1e13 + field * 1e9 + readout_channel * 1e7 + filt * 1e6)
            exposure_baseid = int(
                1e16 + field * 1e12 + readout_channel * 1e10 + filt * 1e9
            )

            def clean_up_document(document):
                """Format passed in dicts for Mongo insertion"""
                # convert types for pymongo:
                for k, v in document.items():
                    if k != "data":
                        if k in sources_int_fields:
                            document[k] = int(document[k])
                        else:
                            document[k] = float(document[k])
                            if k not in ("ra", "dec"):
                                # this will save a lot of space:
                                document[k] = round(document[k], 3)

                # generate unique _id:
                document["_id"] = baseid + document["matchid"]
                document["filter"] = filt
                document["field"] = field
                document["ccd"] = ccd
                document["quad"] = quadrant
                document["rc"] = readout_channel

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
                document["data"].sort(key=lambda x: x["hjd"])
                for data_point in document["data"]:
                    # convert types for pymongo:
                    for k, v in data_point.items():
                        if k in sourcedata_int_fields:
                            data_point[k] = int(data_point[k])
                        else:
                            data_point[k] = float(data_point[k])
                            if k not in ("ra", "dec", "hjd", "mag", "magerr"):
                                data_point[k] = round(data_point[k], 3)
                            elif k in ("hjd", "mag", "magerr"):
                                data_point[k] = round(data_point[k], 5)
                    # generate unique exposure id's that match _id's in exposures collection
                    data_point["uexpid"] = exposure_baseid + data_point["expid"]

                return document

            exposures = pd.DataFrame.from_records(group.exposures[:])
            # prepare docs to ingest into db:
            docs_exposures = []
            for index, row in exposures.iterrows():
                try:
                    doc = row.to_dict()
                    # unique exposure id:
                    doc["_id"] = exposure_baseid + doc["expid"]
                    doc["matchfile"] = ff_basename
                    doc["filter"] = filt
                    doc["field"] = field
                    doc["ccd"] = ccd
                    doc["quad"] = quadrant
                    doc["rc"] = readout_channel
                    docs_exposures.append(doc)
                except Exception as exception:
                    log(str(exception))

            # ingest exposures in one go:
            if not dry_run:
                mongo.insert_many(
                    collection=collections["exposures"], documents=docs_exposures
                )

            # light curves
            docs_sources = []
            batch_num = 1
            # fixme? skip transients
            # for source_type in ('source', 'transient'):
            for source_type in ("source",):
                sources = pd.DataFrame.from_records(
                    group[f"{source_type}s"].read(),
                    index="matchid",
                    exclude=sources_fields_to_exclude,
                )
                # Load in percentiles separately to compute the IQR column
                # because Pandas DF from_records() only wants 2-D tables
                percentiles = group[f"{source_type}s"].col("percentiles")
                # Ignore float errors due to infinity values
                old_settings = np.seterr(all="ignore")
                iqr = np.round(percentiles[:, 8] - percentiles[:, 3], 3)
                np.seterr(**old_settings)
                sources["iqr"] = iqr

                sourcedatas = pd.DataFrame.from_records(
                    group[f"{source_type}data"][:],
                    index="matchid",
                    exclude=[
                        "ypos",
                        "xpos",
                        "mjd",
                        "psfflux",
                        "psffluxerr",
                        "magerrmodel",
                    ],
                )
                sourcedatas.rename(
                    columns={"ra": "ra_data", "dec": "dec_data"}, inplace=True
                )
                sourcedata_colnames = sourcedatas.columns.values
                # Join sources and their data
                merged = sources.merge(sourcedatas, left_index=True, right_index=True)
                prev_matchid = None
                current_doc = None
                for row in merged.itertuples():
                    matchid = row[0]
                    try:
                        # At a new source
                        if matchid != prev_matchid:
                            # Done with last source; save
                            if current_doc is not None:
                                current_doc = clean_up_document(current_doc)
                                docs_sources.append(current_doc)

                            # Set up new doc
                            doc = dict(row._asdict())
                            doc["matchid"] = doc["Index"]
                            doc.pop("Index")
                            # Coerce the source data info into its own nested array
                            first_data_row = {}
                            for col in sourcedata_colnames:
                                if col not in ["dec_data", "ra_data"]:
                                    first_data_row[col] = doc[col]
                                else:
                                    real_col = col.split("_data")[0]
                                    first_data_row[real_col] = doc[col]
                                doc.pop(col)
                            doc["data"] = [first_data_row]
                            current_doc = doc
                        # For continued source, just append new data row
                        else:
                            data_row = {}
                            data = dict(row._asdict())
                            for col in sourcedata_colnames:
                                if col not in ["dec_data", "ra_data"]:
                                    data_row[col] = data[col]
                                else:
                                    real_col = col.split("_data")[0]
                                    data_row[real_col] = data[col]

                            current_doc["data"].append(data_row)

                        prev_matchid = matchid

                    except Exception as exception:
                        log(str(exception))

                    # ingest in batches
                    try:
                        if (
                            len(docs_sources) % batch_size == 0
                            and len(docs_sources) != 0
                        ):
                            if not dry_run:
                                mongo.insert_many(
                                    collection=collections["sources"],
                                    documents=docs_sources,
                                )
                            # flush:
                            docs_sources = []
                            batch_num += 1
                    except Exception as exception:
                        log(str(exception))

        # Clean up and append the last doc
        if current_doc is not None:
            current_doc = clean_up_document(current_doc)
            docs_sources.append(current_doc)

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
        track_failures(tag=tag, file_name=file_name, exception=e)
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
    failures_only: bool = False,
):
    """Preprocess and Ingest ZTF matchfiles into Kowalski

    :param path: local path to matchfiles
    :param tag: matchfile release time tag
    :param num_proc: number of processes for parallel ingestion
    :param batch_size: batch size for light curve data ingestion
    :param rm: remove matchfiles after ingestion?
    :param dry_run: dry run?
    :param failures_only: ingest only failed matchfiles?
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
        srv=config["database"]["srv"],
        verbose=0,
    )
    log("Successfully connected to DB")

    collections = {
        "exposures": f"ZTF_exposures_{tag}",
        "sources": f"ZTF_sources_{tag}",
    }

    # create indices:
    log("Creating indices")
    if not dry_run:
        mongo.db[collections["exposures"]].create_index(
            [("expid", pymongo.ASCENDING)], background=True
        )
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

    files = [str(f) for f in pathlib.Path(path).glob("ztf_*.pytable")]

    if failures_only:
        # find the csv files that has the list of failed matchfiles and the exception
        # this file is located where the matchfiles are.
        # there could be multiple files, so we need to find the latest one
        # the name format is ztf_matchfiles_failures_YYYYMMDD_HHMMSS.csv
        # the file contains two columns: matchfile name and exception

        # find all csv files, sort by timestamp and get the latest one
        csv_files = [
            str(f) for f in pathlib.Path(path).glob("ztf_matchfiles_failures_*.csv")
        ]
        csv_files_timestamps = [(f, os.path.getmtime(f)) for f in csv_files]
        csv_files_timestamps.sort(key=lambda x: x[1])
        csv_file = csv_files_timestamps[-1][0]

        # read the csv file and get the list of failed matchfiles to filter the list of files
        df = pd.read_csv(csv_file)
        failed_matchfiles = df["file_name"].tolist()
        files = [f for f in files if f in failed_matchfiles]
        print(f"Only processing {len(files)} failed matchfiles")
    else:
        log(f"# files to process: {len(files)}")

    # the tag_ingestion will be used when keeping track of failures for a given ingestion
    tag_ingestion = f"{tag}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

    input_list = [
        (f, collections, tag_ingestion, batch_size, rm, dry_run) for f in sorted(files)
    ]
    # for a more even job distribution:
    random.shuffle(input_list)

    with multiprocessing.Pool(processes=num_proc) as pool:
        for _ in tqdm(pool.imap(process_file, input_list), total=len(files)):
            pass


if __name__ == "__main__":
    fire.Fire(run)
