"""
This tool will digest zipped IGAPS source data from http://www.star.ucl.ac.uk/IGAPS/catalogue/ and ingest the data into Kowalski.
"""

import fire
import glob
import multiprocessing as mp
import numpy as np
import pandas as pd
import os
from astropy.io import fits
from tqdm.auto import tqdm

import kowalski.tools.istarmap as istarmap  # noqa: F401
from kowalski.utils import (
    deg2dms,
    deg2hms,
    init_db_sync,
    Mongo,
)
from kowalski.config import load_config
from kowalski.log import log

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]

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

    collection = "IGAPS_DR2"

    log(f"Processing {file}")

    names = [
        "name",
        "RA",
        "DEC",
        "gal_long",
        "gal_lat",
        "sourceID",
        "posErr",
        "mergedClass",
        "pStar",
        "pGalaxy",
        "pNoise",
        "i",
        "iErr",
        "iAB",
        "iEll",
        "iClass",
        "iDeblend",
        "iSaturated",
        "iVignetted",
        "iTrail",
        "iTruncated",
        "iBadPix",
        "iMJD",
        "iSeeing",
        "iDetectionID",
        "iDeltaRA",
        "iDeltaDEC",
        "ha",
        "haErr",
        "haAB",
        "haEll",
        "haClass",
        "haDeblend",
        "haSaturated",
        "haVignetted",
        "haTrail",
        "haTruncated",
        "haBadPix",
        "haMJD",
        "haSeeing",
        "haDetectionID",
        "haDeltaRA",
        "haDeltaDEC",
        "r_I",
        "rErr_I",
        "rAB_I",
        "rEll_I",
        "rClass_I",
        "rDeblend_I",
        "rSaturated_I",
        "rVignetted_I",
        "rTrail_I",
        "rTruncated_I",
        "rBadPix_I",
        "rMJD_I",
        "rSeeing_I",
        "rDetectionID_I",
        "r_U",
        "rErr_U",
        "rAB_U",
        "rEll_U",
        "rClass_U",
        "rDeblend_U",
        "rSaturated_U",
        "rVignetted_U",
        "rTrail_U",
        "rTruncated_U",
        "rBadPix_U",
        "rMJD_U",
        "rSeeing_U",
        "rDetectionID_U",
        "rDeltaRA_U",
        "rDeltaDEC_U",
        "g",
        "gErr",
        "gAB",
        "gEll",
        "gClass",
        "gDeblend",
        "gSaturated",
        "gVignetted",
        "gTrail",
        "gTruncated",
        "gBadPix",
        "gmask",
        "gMJD",
        "gSeeing",
        "gDetectionID",
        "gDeltaRA",
        "gDeltaDEC",
        "U_RGO",
        "UErr",
        "UEll",
        "UClass",
        "UDeblend",
        "USaturated",
        "UVignetted",
        "UTrail",
        "UTruncated",
        "UBadPix",
        "UMJD",
        "USeeing",
        "UDetectionID",
        "UDeltaRA",
        "UDeltaDEC",
        "brightNeighb",
        "deblend",
        "saturated",
        "nBands",
        "errBits",
        "nObs_I",
        "nObs_U",
        "fieldID_I",
        "fieldID_U",
        "fieldGrade_I",
        "fieldGrade_U",
        "emitter",
        "variable",
        "SourceID2",
        "i2",
        "i2Err",
        "i2Class",
        "i2Seeing",
        "i2MJD",
        "i2DeltaRA",
        "i2DeltaDEC",
        "i2DetectionID",
        "i2ErrBits",
        "ha2",
        "ha2Err",
        "ha2Class",
        "ha2Seeing",
        "ha2MJD",
        "ha2DeltaRA",
        "ha2DeltaDEC",
        "ha2DetectionID",
        "ha2ErrBits",
        "r2_I",
        "r2Err_I",
        "r2Class_I",
        "r2Seeing_I",
        "r2MJD_I",
        "r2DeltaRA_I",
        "r2DeltaDEC_I",
        "r2DetectionID_I",
        "r2ErrBits_I",
        "r2_U",
        "r2Err_U",
        "r2Class_U",
        "r2Seeing_U",
        "r2MJD_U",
        "r2DeltaRA_U",
        "r2DeltaDEC_U",
        "r2DetectionID_U",
        "r2ErrBits_U",
        "g2",
        "g2Err",
        "g2Class",
        "g2Seeing",
        "g2MJD",
        "g2DeltaRA",
        "g2DeltaDEC",
        "g2DetectionID",
        "g2ErrBits",
        "U_RGO2",
        "U2Err",
        "U2Class",
        "U2Seeing",
        "U2MJD",
        "U2DeltaRA",
        "U2DeltaDEC",
        "U2DetectionID",
        "U2ErrBits",
        "errBits2",
    ]
    with fits.open(file) as hdulist:
        nhdu = 1
        dataframe = pd.DataFrame(np.asarray(hdulist[nhdu].data), columns=names)

    for chunk_index, dataframe_chunk in dataframe.groupby(
        np.arange(len(dataframe)) // batch_size
    ):

        log(f"{file}: processing batch # {chunk_index + 1}")

        for col, dtype in dataframe_chunk.dtypes.items():
            if dtype == np.object:
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
    """Pre-process and ingest IGAPS catalog
    :param path: path to CSV data files (~98 GB tarred)
                 see http://www.star.ucl.ac.uk/IGAPS/catalogue/
    :param num_processes:
    :return:
    """

    files = glob.glob(os.path.join(path, "igaps-*.fits.gz"))

    catalog_name = "IGAPS_DR2"

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
