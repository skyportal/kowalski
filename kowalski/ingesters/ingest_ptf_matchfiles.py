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
    deg2dms,
    deg2hms,
    init_db_sync,
    load_config,
    log,
    Mongo,
)


""" load config and secrets """

config = load_config(config_files=["config.yaml"])["kowalski"]
init_db_sync(config=config)


def utc_now():
    return datetime.datetime.now(pytz.utc)


sources_int_fields = ("_id", "filter", "field", "rc", "nepochs")
sourcedata_int_fields = ("ipacFlags", "matchedSourceID")

sources_fields_to_exclude = [
    "astrometricRMS",
    "bestAstrometricRMS",
    "bestChiSQ",
    "bestCon",
    "bestLinearTrend",
    "bestMagRMS",
    "bestMaxMag",
    "bestMaxSlope",
    "bestMeanMag",
    "bestMedianAbsDev",
    "bestMedianMag",
    "bestMinMag",
    "bestNAboveMeanByStd",
    "bestNBelowMeanByStd",
    "bestNConsecAboveMeanByStd",
    "bestNConsecBelowMeanByStd",
    "bestNConsecFromMeanByStd",
    "bestNMedianBufferRange",
    "bestNPairPosSlope",
    "bestPercentiles",
    "bestPeriodSearch",
    "bestProbNonQso",
    "bestProbQso",
    "bestSkewness",
    "bestSmallKurtosis",
    "bestStetsonJ",
    "bestStetsonK",
    "bestVonNeumannRatio",
    "bestWeightedMagRMS",
    "bestWeightedMeanMag",
    "chiSQ",
    "con",
    "linearTrend",
    "magRMS",
    "maxMag",
    "maxSlope",
    "meanMag",
    "medianAbsDev",
    "medianMag",
    "minMag",
    "nAboveMeanByStd",
    "nBelowMeanByStd",
    "nConsecAboveMeanByStd",
    "nConsecBelowMeanByStd",
    "nConsecFromMeanByStd",
    "nMedianBufferRange",
    "nPairPosSlope",
    "nbestobs",
    "ngoodobs",
    "nobs",
    "percentiles",
    "periodSearch",
    "probNonQso",
    "probQso",
    "referenceMag",
    "referenceMagErr",
    "referenceMuMax",
    "skewness",
    "smallKurtosis",
    "stetsonJ",
    "stetsonK",
    "uncalibMeanMag",
    "vonNeumannRatio",
    "weightedMagRMS",
    "weightedMeanMag",
    "x",
    "y",
    "z",
]

sourcedata_to_exclude = [
    "a_world",
    "absphotzp",
    "b_world",
    "background",
    "dec",
    "errx2_image",
    "erry2_image",
    "fwhm_image",
    "mag_auto",
    "magerr_auto",
    "mu_max",
    "pid",
    "sid",
    "ra",
    "relPhotFlags",
    "sextractorFlags",
    "x",
    "x_image",
    "xpeak_image",
    "y",
    "y_image",
    "ypeak_image",
    "z",
]


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
            srv=config["database"]["srv"],
            verbose=0,
        )

        with tables.open_file(file_name, "r+") as f:
            for group in f.walk_groups():
                pass

            ff_basename = pathlib.Path(file_name).name
            # base id:
            _, field, filt, ccd, _, _ = ff_basename.split("_")
            field = int(field[1:])
            filt = int(filt[1:])
            readout_channel = int(ccd[1:])
            baseid = int(1e13 + field * 1e9 + readout_channel * 1e7 + filt * 1e6)
            exposure_baseid = int(
                1e16 + field * 1e12 + readout_channel * 1e10 + filt * 1e9
            )

            def clean_up_document(group):
                """Format passed in dicts for Mongo insertion"""
                document = {}
                for k, v in group.items():
                    if k == "matchedSourceID":
                        document[k] = group[k]
                        continue
                    if k in sources_int_fields:
                        document[k] = [int(group[k][key2]) for key2 in group[k].keys()]
                    else:
                        document[k] = [
                            float(group[k][key2]) for key2 in group[k].keys()
                        ]

                # document["ra"] = document["ra"][0]
                # document["dec"] = document["dec"][0]

                # generate unique _id:
                document["_id"] = baseid + document["matchedSourceID"]
                document["filter"] = filt
                document["field"] = field
                document["ccd"] = ccd

                # GeoJSON for 2D indexing
                document["coordinates"] = dict()
                _ra = np.median(document["ra"])
                _dec = np.median(document["dec"])
                _radec_str = [deg2hms(_ra), deg2dms(_dec)]
                document["coordinates"]["radec_str"] = _radec_str
                # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
                _radec_geojson = [_ra - 180.0, _dec]
                document["coordinates"]["radec_geojson"] = {
                    "type": "Point",
                    "coordinates": _radec_geojson,
                }

                document["data"] = []
                for t, m, e, f, _ra, _dec in zip(
                    document["mjd"],
                    document["mag"],
                    document["magErr"],
                    document["ipacFlags"],
                    document["ra"],
                    document["dec"],
                ):
                    data_point = {
                        "mjd": t,
                        "mag": m,
                        "magerr": e,
                        "ipacflags": f,
                        "ra": _ra,
                        "dec": _dec,
                    }
                    # convert types for pymongo:
                    for k, v in data_point.items():
                        if k in sourcedata_int_fields:
                            data_point[k] = int(data_point[k])
                        else:
                            data_point[k] = float(data_point[k])
                            if k == "mjd":
                                data_point[k] = round(data_point[k], 5)
                            elif k not in ("ra", "dec"):
                                data_point[k] = round(data_point[k], 3)
                    document["data"].append(data_point)
                del (
                    document["mjd"],
                    document["mag"],
                    document["magErr"],
                    document["ipacFlags"],
                    document["ra"],
                    document["dec"],
                )
                document["data"].sort(key=lambda x: x["mjd"])

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
                    docs_exposures.append(doc)
                except Exception as exception:
                    log(str(exception))

            # ingest exposures in one go:
            if not dry_run:
                mongo.insert_many(
                    collection=collections["exposures"], documents=docs_exposures
                )

            sources = pd.DataFrame.from_records(
                group["sources"].read(),
                index="matchedSourceID",
                exclude=sources_fields_to_exclude,
            )
            sourcedatas = pd.DataFrame.from_records(
                group["sourcedata"].read(),
                index="matchedSourceID",
                exclude=sourcedata_to_exclude,
            )

            merged = sources.merge(sourcedatas, left_index=True, right_index=True)
            groups = merged.groupby("matchedSourceID")

            # light curves
            docs_sources = []
            batch_num = 1
            # fixme? skip transients
            for row, group in groups:
                try:
                    groupdict = group.to_dict()
                    groupdict["matchedSourceID"] = row
                    current_doc = clean_up_document(groupdict)
                    docs_sources.append(current_doc)
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
                        batch_num += 1
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
        srv=config["database"]["srv"],
        verbose=0,
    )
    log("Successfully connected to DB")

    collections = {
        "exposures": "PTF_exposures",
        "sources": "PTF_sources",
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

    files = [str(f) for f in pathlib.Path(path).glob("PTF_*.pytable")]

    log(f"# files to process: {len(files)}")

    input_list = [(f, collections, batch_size, rm, dry_run) for f in sorted(files)]
    # for a more even job distribution:
    random.shuffle(input_list)

    with multiprocessing.Pool(processes=num_proc) as pool:
        for _ in tqdm(pool.imap(process_file, input_list), total=len(files)):
            pass


if __name__ == "__main__":
    fire.Fire(run)
