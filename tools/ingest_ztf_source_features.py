import fire
import h5py
import multiprocessing as mp
import numpy as np
import os
import pandas as pd
import pathlib
import pymongo
import traceback
from tqdm import tqdm


import istarmap  # noqa: F401
from utils import (
    compute_dmdt,
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


# cone search radius in arcsec:
CONE_SEARCH_RADIUS = 2
# convert arcsec to rad:
CONE_SEARCH_RADIUS *= np.pi / 180.0 / 3600.0


def cross_match(_mongo, ra, dec):
    """
    Cross-match by position
    """

    cross_matches = dict()
    features = dict()

    catalogs = {
        "AllWISE": {
            "filter": {},
            "projection": {
                "_id": 1,
                "coordinates.radec_str": 1,
                "w1mpro": 1,
                "w1sigmpro": 1,
                "w2mpro": 1,
                "w2sigmpro": 1,
                "w3mpro": 1,
                "w3sigmpro": 1,
                "w4mpro": 1,
                "w4sigmpro": 1,
                "ph_qual": 1,
            },
        },
        "Gaia_EDR3": {
            "filter": {},
            "projection": {
                "_id": 1,
                "coordinates.radec_str": 1,
                "phot_g_mean_mag": 1,
                "phot_bp_mean_mag": 1,
                "phot_rp_mean_mag": 1,
                "parallax": 1,
                "parallax_error": 1,
                "pmra": 1,
                "pmra_error": 1,
                "pmdec": 1,
                "pmdec_error": 1,
                "astrometric_excess_noise": 1,
                "phot_bp_rp_excess_factor": 1,
            },
        },
        "PS1_DR1": {
            "filter": {},
            "projection": {
                "_id": 1,
                "coordinates.radec_str": 1,
                "gMeanPSFMag": 1,
                "gMeanPSFMagErr": 1,
                "rMeanPSFMag": 1,
                "rMeanPSFMagErr": 1,
                "iMeanPSFMag": 1,
                "iMeanPSFMagErr": 1,
                "zMeanPSFMag": 1,
                "zMeanPSFMagErr": 1,
                "yMeanPSFMag": 1,
                "yMeanPSFMagErr": 1,
                "qualityFlag": 1,
            },
        },
    }

    try:
        ra_geojson = float(ra)
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(dec)

        for catalog in catalogs:
            catalog_filter = catalogs[catalog]["filter"]
            catalog_projection = catalogs[catalog]["projection"]

            object_position_query = dict()
            object_position_query["coordinates.radec_geojson"] = {
                "$nearSphere": [ra_geojson, dec_geojson],
                "$minDistance": 0,
                "$maxDistance": CONE_SEARCH_RADIUS,
            }
            s = _mongo.db[catalog].find(
                {**object_position_query, **catalog_filter},
                {**catalog_projection},
                limit=1,
            )
            cross_matches[catalog] = list(s)

        # convert into a dict of features
        for catalog in catalogs:
            if len(cross_matches[catalog]) > 0:
                match = cross_matches[catalog][0]
            else:
                match = dict()

            for feature in catalogs[catalog]["projection"].keys():
                if feature == "coordinates.radec_str":
                    continue
                f = match.get(feature, None)
                f_name = f"{catalog}__{feature}"
                features[f_name] = f

    except Exception as e:
        log(e)

    return features


def get_n_ztf_alerts(_mongo, ra, dec):
    """
    Cross-match by position
    """

    try:
        ra_geojson = float(ra)
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(dec)

        """ catalogs """
        catalog = "ZTF_alerts"
        object_position_query = dict()
        object_position_query["coordinates.radec_geojson"] = {
            "$geoWithin": {
                "$centerSphere": [[ra_geojson, dec_geojson], CONE_SEARCH_RADIUS]
            }
        }
        n = int(_mongo.db[catalog].count_documents(object_position_query))

    except Exception as e:
        print(str(e))
        n = None

    return n


def get_mean_ztf_alert_braai(_mongo, ra, dec):
    """
    Cross-match by position and get mean alert braai score
    """

    try:
        ra_geojson = float(ra)
        # geojson-friendly ra:
        ra_geojson -= 180.0
        dec_geojson = float(dec)

        """ catalogs """
        catalog = "ZTF_alerts"
        object_position_query = dict()
        object_position_query["coordinates.radec_geojson"] = {
            "$geoWithin": {
                "$centerSphere": [[ra_geojson, dec_geojson], CONE_SEARCH_RADIUS]
            }
        }
        # n = int(_db[catalog].count_documents(object_position_query))
        objects = list(
            _mongo.db[catalog].aggregate(
                [
                    {"$match": object_position_query},
                    {"$project": {"objectId": 1, "classifications.braai": 1}},
                    {
                        "$group": {
                            "_id": "$objectId",
                            "braai_avg": {"$avg": "$classifications.braai"},
                        }
                    },
                ]
            )
        )
        if len(objects) > 0:
            # there may be multiple objectId's in the match due to astrometric errors:
            braais = [
                float(o.get("braai_avg", 0))
                for o in objects
                if o.get("braai_avg", None)
            ]
            if len(braais) > 0:
                braai_avg = np.mean(braais)
            else:
                braai_avg = None
        else:
            braai_avg = None

    except Exception as e:
        log(e)
        braai_avg = None

    return braai_avg


def light_curve_dmdt(
    _mongo, _id, catalog: str = "ZTF_sources_20201201", dmdt_ints_v: str = "v20200318"
):
    """Compute dmdt for a filtered ZTF light curve

    :param _mongo:
    :param _id:
    :param catalog:
    :param dmdt_ints_v:
    :return:
    """
    try:
        c = _mongo.db[catalog].find(
            {"_id": _id}, {"_id": 0, "data.catflags": 1, "data.hjd": 1, "data.mag": 1}
        )
        lc = list(c)[0]

        df_lc = pd.DataFrame.from_records(lc["data"])
        w_good = df_lc["catflags"] == 0
        df_lc = df_lc.loc[w_good]

        dmdt = compute_dmdt(df_lc["hjd"].values, df_lc["mag"].values, dmdt_ints_v)
    except Exception as e:
        log(e)
        dmdt = None

    return dmdt


def process_file(_file, _collections, _xmatch):

    # connect to MongoDB:
    mongo = Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        replica_set=config["database"]["replica_set"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        db=config["database"]["db"],
        verbose=0,
    )

    try:
        with h5py.File(_file, "r") as f:
            features = f["stats"][...]
            periodic_features = f["stats_EAOV"][...]

        feature_column_names = [
            "_id",
            "ra",
            "dec",
            "n",
            "median",
            "wmean",
            "chi2red",
            "roms",
            "wstd",
            "norm_peak_to_peak_amp",
            "norm_excess_var",
            "median_abs_dev",
            "iqr",
            "i60r",
            "i70r",
            "i80r",
            "i90r",
            "skew",
            "smallkurt",
            "inv_vonneumannratio",
            "welch_i",
            "stetson_j",
            "stetson_k",
            "ad",
            "sw",
        ]

        periodic_feature_column_names = [
            "_id",
            "period",
            "significance",
            "pdot",
            "f1_power",
            "f1_BIC",
            "f1_a",
            "f1_b",
            "f1_amp",
            "f1_phi0",
            "f1_relamp1",
            "f1_relphi1",
            "f1_relamp2",
            "f1_relphi2",
            "f1_relamp3",
            "f1_relphi3",
            "f1_relamp4",
            "f1_relphi4",
        ]

        df_features = pd.DataFrame(features, columns=feature_column_names)
        df_periodic_features = pd.DataFrame(
            periodic_features, columns=periodic_feature_column_names
        )

        df = pd.merge(df_features, df_periodic_features, on="_id")
        df["_id"] = df["_id"].apply(lambda x: int(x))

        docs = df.to_dict(orient="records")

        requests = []

        for doc in docs:

            # get number of ZTF alerts within 2"
            n_ztf_alerts = get_n_ztf_alerts(mongo, doc["ra"], doc["dec"])
            doc["n_ztf_alerts"] = n_ztf_alerts

            if n_ztf_alerts > 0:
                doc["mean_ztf_alert_braai"] = get_mean_ztf_alert_braai(
                    mongo, doc["ra"], doc["dec"]
                )
            else:
                doc["mean_ztf_alert_braai"] = None

            # compute dmdt's
            dmdt = light_curve_dmdt(
                mongo,
                doc["_id"],
                catalog=_collections["sources"],
                dmdt_ints_v="v20200318",
            )
            doc["dmdt"] = dmdt.tolist() if dmdt is not None else None

            # Cross-match:
            if _xmatch:
                features = cross_match(mongo, doc["ra"], doc["dec"])
                for feature in features:
                    doc[feature] = features[feature]

            # GeoJSON for 2D indexing
            doc["coordinates"] = {}
            _ra = doc["ra"]
            _dec = doc["dec"]
            _radec_str = [deg2hms(_ra), deg2dms(_dec)]
            doc["coordinates"]["radec_str"] = _radec_str
            # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
            _radec_geojson = [_ra - 180.0, _dec]
            doc["coordinates"]["radec_geojson"] = {
                "type": "Point",
                "coordinates": _radec_geojson,
            }

            _id = doc["_id"]
            doc.pop("_id", None)
            requests += [
                pymongo.UpdateOne(
                    {"_id": _id},
                    {"$set": doc},
                    upsert=True,
                )
            ]

        mongo.db[_collections["features"]].bulk_write(requests)

    except Exception as e:
        traceback.print_exc()
        print(e)

    # disconnect from db:
    try:
        mongo.client.close()
    finally:
        pass


def run(
    path: str = "./",
    tag: str = "20201201",
    xmatch: bool = True,
    num_processes: int = mp.cpu_count(),
):
    """Pre-process and ingest ZTF source features

    :param path: path to hdf5 files containing ZTF source features
                 see https://github.com/mcoughlin/ztfperiodic
    :param tag: from collection name: ZTF_sources_<YYYYMMDD>
    :param xmatch: cross-match against external catalogs?
    :param num_processes:
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
        verbose=0,
    )
    log("Successfully connected")

    collections = {
        "sources": f"ZTF_sources_{tag}",
        "features": f"ZTF_source_features_{tag}",
    }

    if config["database"]["build_indexes"]:
        log("Checking indexes")
        try:
            mongo.db[collections["features"]].create_index(
                [("coordinates.radec_geojson", "2dsphere"), ("_id", pymongo.ASCENDING)],
                background=True,
            )
            mongo.db[collections["features"]].create_index(
                [("period", pymongo.DESCENDING), ("significance", pymongo.DESCENDING)],
                background=True,
            )
        except Exception as e:
            log(e)

    files = pathlib.Path(path).glob("*.h5")

    input_list = [
        (f, collections, xmatch) for f in sorted(files) if os.stat(f).st_size != 0
    ]

    log(f"# files to process: {len(input_list)}")

    with mp.Pool(processes=num_processes) as p:
        for _ in tqdm(p.istarmap(process_file, input_list), total=len(input_list)):
            pass

    log("All done")


if __name__ == "__main__":
    fire.Fire(run)
