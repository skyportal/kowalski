import datetime
import fire
import multiprocessing
import numpy as np
import os
import pathlib
import pytz
import time
from tqdm import tqdm
import traceback
from typing import Sequence
from typing import Mapping
from copy import deepcopy
import io
import fastavro


from kowalski.utils import (
    deg2dms,
    deg2hms,
    great_circle_distance,
    in_ellipse,
    radec2lb,
    retry,
    timer,
    init_db_sync,
    Mongo,
)

from kowalski.config import load_config
from kowalski.log import log


""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]
init_db_sync(config=config)

collection_alerts: str = config["database"]["collections"]["alerts_ztf"]
collection_alerts_aux: str = config["database"]["collections"]["alerts_ztf_aux"]
cross_match_config: dict = config["database"]["xmatch"]["ZTF"]


def utc_now():
    return datetime.datetime.now(pytz.utc)


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


def alert_mongify(alert: Mapping):
    """
    Prepare a raw alert for ingestion into MongoDB:
        - add a placeholder for ML-based classifications
        - add coordinates for 2D spherical indexing and compute Galactic coordinates
        - extract the prv_candidates section
        - extract the fp_hists section (if it exists)

    :param alert:
    :return:
    """

    doc = dict(alert)

    # let mongo create a unique _id

    # placeholders for classifications
    doc["classifications"] = dict()

    # GeoJSON for 2D indexing
    doc["coordinates"] = {}
    _ra = doc["candidate"]["ra"]
    _dec = doc["candidate"]["dec"]
    # string format: H:M:S, D:M:S
    _radec_str = [deg2hms(_ra), deg2dms(_dec)]
    doc["coordinates"]["radec_str"] = _radec_str
    # for GeoJSON, must be lon:[-180, 180], lat:[-90, 90] (i.e. in deg)
    _radec_geojson = [_ra - 180.0, _dec]
    doc["coordinates"]["radec_geojson"] = {
        "type": "Point",
        "coordinates": _radec_geojson,
    }

    # Galactic coordinates l and b
    l, b = radec2lb(doc["candidate"]["ra"], doc["candidate"]["dec"])
    doc["coordinates"]["l"] = l
    doc["coordinates"]["b"] = b

    prv_candidates = deepcopy(doc["prv_candidates"])
    doc.pop("prv_candidates", None)
    if prv_candidates is None:
        prv_candidates = []

    # extract the fp_hists section, if it exists
    fp_hists = deepcopy(doc.get("fp_hists", None))
    doc.pop("fp_hists", None)
    if fp_hists is None:
        fp_hists = []
    else:
        # sort by jd
        fp_hists = sorted(fp_hists, key=lambda k: k["jd"])

    return doc, prv_candidates, fp_hists


def process_file(argument_list: Sequence):
    def read_schema_data(bytes_io):
        """Read data that already has an Avro schema.

        :param bytes_io: `_io.BytesIO` Data to be decoded.
        :return: `dict` Decoded data.
        """
        bytes_io.seek(0)
        message = fastavro.reader(bytes_io)
        return message

    def decode_message(file_name):
        """
        Decode Avro message according to a schema.

        :param msg: The Kafka message result from consumer.poll()
        :return:
        """

        # open the file
        with open(file_name, "rb") as f:
            message = f.read()
        try:
            bytes_io = io.BytesIO(message)
            decoded_msg = read_schema_data(bytes_io)
        except AssertionError:
            decoded_msg = None
        finally:
            return decoded_msg

    def alert_filter__xmatch(alert: Mapping, cross_match_config: dict) -> dict:
        """Cross-match alerts against external catalogs"""

        xmatches = dict()

        try:
            ra = float(alert["candidate"]["ra"])
            dec = float(alert["candidate"]["dec"])
            ra_geojson = float(alert["candidate"]["ra"])
            # geojson-friendly ra:
            ra_geojson -= 180.0
            dec_geojson = float(alert["candidate"]["dec"])

            """ catalogs """
            matches = []
            for catalog in cross_match_config:
                try:
                    # if the catalog has "distance", "ra", "dec" in its config, then it is a catalog with distance
                    if cross_match_config[catalog].get("use_distance", False):
                        matches = alert_filter__xmatch_distance(
                            ra,
                            dec,
                            ra_geojson,
                            dec_geojson,
                            catalog,
                            cross_match_config,
                        )
                    else:
                        matches = alert_filter__xmatch_no_distance(
                            ra_geojson, dec_geojson, catalog, cross_match_config
                        )
                except Exception as e:
                    log(f"Failed to cross-match {catalog}: {str(e)}")
                    matches = []
                xmatches[catalog] = matches

            # clean up after thyself
            del ra, dec, ra_geojson, dec_geojson, matches, cross_match_config

        except Exception as e:
            log(f"Failed catalogs cross-match: {str(e)}")

        return xmatches

    def alert_filter__xmatch_no_distance(
        ra_geojson: float,
        dec_geojson: float,
        catalog: str,
        cross_match_config: dict,
    ) -> dict:
        """Cross-match alerts against external catalogs"""

        matches = []

        try:
            # cone search radius:
            catalog_cone_search_radius = float(
                cross_match_config[catalog]["cone_search_radius"]
            )
            # convert to rad:
            if cross_match_config[catalog]["cone_search_unit"] == "arcsec":
                catalog_cone_search_radius *= np.pi / 180.0 / 3600.0
            elif cross_match_config[catalog]["cone_search_unit"] == "arcmin":
                catalog_cone_search_radius *= np.pi / 180.0 / 60.0
            elif cross_match_config[catalog]["cone_search_unit"] == "deg":
                catalog_cone_search_radius *= np.pi / 180.0
            elif cross_match_config[catalog]["cone_search_unit"] == "rad":
                pass
            else:
                raise Exception(
                    f"Unknown cone search radius units for {catalog}."
                    " Must be one of [deg, rad, arcsec, arcmin]"
                )

            catalog_filter = cross_match_config[catalog]["filter"]
            catalog_projection = cross_match_config[catalog]["projection"]

            object_position_query = dict()
            object_position_query["coordinates.radec_geojson"] = {
                "$geoWithin": {
                    "$centerSphere": [
                        [ra_geojson, dec_geojson],
                        catalog_cone_search_radius,
                    ]
                }
            }
            s = retry(mongo.db[catalog].find)(
                {**object_position_query, **catalog_filter}, {**catalog_projection}
            )
            matches = list(s)

        except Exception as e:
            log(str(e))

        return matches

    def alert_filter__xmatch_distance(
        ra: float,
        dec: float,
        ra_geojson: float,
        dec_geojson: float,
        catalog: str,
        cross_match_config: dict,
    ) -> dict:
        """
        Run cross-match with catalogs that have a distance value

        :param alert:
        :param catalog: name of the catalog (collection) to cross-match with
        :return:
        """

        matches = []

        try:
            catalog_cm_at_distance = cross_match_config[catalog]["cm_at_distance"]
            catalog_cm_low_distance = cross_match_config[catalog]["cm_low_distance"]
            # cone search radius:
            catalog_cone_search_radius = float(
                cross_match_config[catalog]["cone_search_radius"]
            )
            # convert to rad:
            if cross_match_config[catalog]["cone_search_unit"] == "arcsec":
                catalog_cone_search_radius *= np.pi / 180.0 / 3600.0
            elif cross_match_config[catalog]["cone_search_unit"] == "arcmin":
                catalog_cone_search_radius *= np.pi / 180.0 / 60.0
            elif cross_match_config[catalog]["cone_search_unit"] == "deg":
                catalog_cone_search_radius *= np.pi / 180.0
            elif cross_match_config[catalog]["cone_search_unit"] == "rad":
                pass

            catalog_filter = cross_match_config[catalog]["filter"]
            catalog_projection = cross_match_config[catalog]["projection"]

            # first do a coarse search of everything that is around
            object_position_query = dict()
            object_position_query["coordinates.radec_geojson"] = {
                "$geoWithin": {
                    "$centerSphere": [
                        [ra_geojson, dec_geojson],
                        catalog_cone_search_radius,
                    ]
                }
            }
            galaxies = list(
                retry(mongo.db[catalog].find)(
                    {**object_position_query, **catalog_filter}, {**catalog_projection}
                )
            )

            distance_value = cross_match_config[catalog]["distance_value"]
            distance_method = cross_match_config[catalog]["distance_method"]

            # these guys are very big, so check them separately
            M31 = {
                "_id": 596900,
                "name": "PGC2557",
                "ra": 10.6847,
                "dec": 41.26901,
                "a": 6.35156,
                "b2a": 0.32,
                "pa": 35.0,
                "z": -0.00100100006,
                "DistMpc": 0.778,
                "sfr_fuv": None,
                "mstar": 253816876.412914,
                "sfr_ha": 0,
                "coordinates": {"radec_str": ["00:42:44.3503", "41:16:08.634"]},
            }
            M33 = {
                "_id": 597543,
                "name": "PGC5818",
                "ra": 23.46204,
                "dec": 30.66022,
                "a": 2.35983,
                "b2a": 0.59,
                "pa": 23.0,
                "z": -0.000597000006,
                "DistMpc": 0.869,
                "sfr_fuv": None,
                "mstar": 4502777.420493,
                "sfr_ha": 0,
                "coordinates": {"radec_str": ["01:33:50.8900", "30:39:36.800"]},
            }

            if distance_value == "z" or distance_method in ["redshift", "z"]:
                M31[distance_value] = M31["z"]
                M33[distance_value] = M33["z"]
            else:
                M31[distance_value] = M31["DistMpc"]
                M33[distance_value] = M33["DistMpc"]

            for galaxy in galaxies + [M31, M33]:
                try:
                    alpha1, delta01 = galaxy["ra"], galaxy["dec"]

                    redshift, distmpc = None, None
                    if distance_value == "z" or distance_method in [
                        "redshift",
                        "z",
                    ]:
                        redshift = galaxy[distance_value]

                        if redshift < 0.01:
                            # for nearby galaxies and galaxies with negative redshifts, do a `catalog_cm_low_distance` arc-minute cross-match
                            # (cross-match radius would otherwise get un-physically large for nearby galaxies)
                            cm_radius = catalog_cm_low_distance / 3600
                        else:
                            # For distant galaxies, set the cross-match radius to 30 kpc at the redshift of the host galaxy
                            cm_radius = (
                                catalog_cm_at_distance * (0.05 / redshift) / 3600
                            )
                    else:
                        distmpc = galaxy[distance_value]

                        if distmpc < 40:
                            # for nearby galaxies, do a `catalog_cm_low_distance` arc-minute cross-match
                            cm_radius = catalog_cm_low_distance / 3600
                        else:
                            # For distant galaxies, set the cross-match radius to 30 kpc at the distance (in Mpc) of the host galaxy
                            cm_radius = np.rad2deg(
                                np.arctan(catalog_cm_at_distance / (distmpc * 10**3))
                            )

                    in_galaxy = in_ellipse(ra, dec, alpha1, delta01, cm_radius, 1, 0)

                    if in_galaxy:
                        match = galaxy
                        distance_arcsec = round(
                            great_circle_distance(ra, dec, alpha1, delta01) * 3600,
                            2,
                        )
                        # also add a physical distance parameter for redshifts in the Hubble flow
                        if redshift is not None and redshift > 0.005:
                            distance_kpc = round(
                                great_circle_distance(ra, dec, alpha1, delta01)
                                * 3600
                                * (redshift / 0.05),
                                2,
                            )
                        elif distmpc is not None and distmpc > 0.005:
                            distance_kpc = round(
                                np.deg2rad(
                                    great_circle_distance(ra, dec, alpha1, delta01)
                                )
                                * distmpc
                                * 10**3,
                                2,
                            )
                        else:
                            distance_kpc = -1

                        match["coordinates"]["distance_arcsec"] = distance_arcsec
                        match["coordinates"]["distance_kpc"] = distance_kpc
                        matches.append(match)
                except Exception as e:
                    log(f"Could not crossmatch with galaxy {str(galaxy)} : {str(e)}")

            return matches

        except Exception as e:
            log(f"Could not crossmatch with ANY galaxies: {str(e)}")

        return matches

    def process_alert(alert: Mapping, topic: str, cross_match_config: dict):
        """Alert brokering task run by dask.distributed workers

        :param alert: decoded alert from Kafka stream
        :param topic: Kafka stream topic name for bookkeeping
        :return:
        """
        candid = alert["candid"]
        object_id = alert["objectId"]

        # candid not in db, ingest decoded avro packet into db
        alert, prv_candidates, _ = alert_mongify(alert)

        # prv_candidates: pop nulls - save space
        prv_candidates = [
            {kk: vv for kk, vv in prv_candidate.items() if vv is not None}
            for prv_candidate in prv_candidates
        ]

        alert_aux, xmatches = None, None
        # cross-match with external catalogs if objectId not in collection_alerts_aux:
        if (
            retry(mongo.db[collection_alerts_aux].count_documents)(
                {"_id": object_id}, limit=1
            )
            == 0
        ):
            xmatches = alert_filter__xmatch(
                alert, cross_match_config=cross_match_config
            )

            alert_aux = {
                "_id": object_id,
                "cross_matches": xmatches,
                "prv_candidates": prv_candidates,
            }

            retry(mongo.insert_one)(
                collection=collection_alerts_aux, document=alert_aux
            )

        else:
            retry(mongo.db[collection_alerts_aux].update_one)(
                {"_id": object_id},
                {
                    "$addToSet": {
                        "prv_candidates": {"$each": prv_candidates},
                    }
                },
                upsert=True,
            )

        # clean up after thyself
        del (
            alert,
            prv_candidates,
            xmatches,
            alert_aux,
            candid,
            object_id,
        )
        return

    file_name, rm_file, pausing_times = argument_list
    try:
        # connect to MongoDB:
        mongo = get_mongo_client()

        # first, we decompress the tar.gz file
        # the result should be a directory with the same name as the file, with the file contents
        # but first check if its not already unpacked:
        dir_name = file_name.replace(".tar.gz", "")

        # the topic should be in the file_name:
        # its either ztf_public or ztf_partnership
        topic = "ztf_public" if "public" in file_name else "ztf_partnership"

        if not os.path.exists(dir_name):
            log(f"Unpacking {file_name}...")
            os.mkdir(dir_name)
            os.system(f"tar -xzf {file_name} -C {dir_name}")
            log(f"Done unpacking {file_name}")

        # grab all the .avro files in the directory:
        avro_files = [str(f) for f in pathlib.Path(dir_name).glob("*.avro")]

        nb_alerts = len(avro_files)
        for i, avro_file in enumerate(avro_files):
            if i % 1000 == 0 and pausing_times is not None:
                if pausing_times[0] <= utc_now().hour <= pausing_times[1]:
                    log(
                        f"Pausing ingestion for {pausing_times[1] - pausing_times[0]} hours"
                    )
                    time.sleep(abs(pausing_times[1] - pausing_times[0]) * 3600)
                    log("Resuming ingestion")

            # ingest the avro file:
            with timer(f"Processing alert {i + 1}/{nb_alerts}"):
                try:
                    msg_decoded = decode_message(avro_file)
                    for record in msg_decoded:
                        if (
                            retry(mongo.db[collection_alerts].count_documents)(
                                {"candid": record["candid"]}, limit=1
                            )
                            == 0
                        ):
                            process_alert(
                                record,
                                topic=topic,
                                cross_match_config=cross_match_config,
                            )

                        # clean up after thyself
                        del msg_decoded
                except Exception as e:
                    log(f"Failed to process alert {avro_file}: {str(e)}")
                    continue

            if rm_file:
                os.remove(avro_file)

    except Exception as e:
        traceback.print_exc()
        log(e)
        return

    try:
        if rm_file:
            os.remove(file_name)
            # also remove the directory with the contents
            os.system(f"rm -rf {dir_name}")
    finally:
        pass


def run(
    path: str,
    mindate: str = None,
    maxdate: str = None,
    num_proc: int = multiprocessing.cpu_count(),
    rm: bool = False,
    pausing_times: Sequence = None,
):
    """Preprocess and Ingest ZTF alerts into Kowalski's aux table

    :param path: local path to matchfiles
    :param mindate: min date to process
    :param maxdate: max date to process
    :param num_proc: number of processes to use
    :param rm: remove files after processing
    :param pausing_times: time window (in hours) in which to pause processing (UTC)
    :return:
    """

    # make sure the path is an absolute path
    path = os.path.abspath(path)

    files = [str(f) for f in pathlib.Path(path).glob("*.tar.gz")]

    # sort the files by date, if provided
    if mindate is not None:
        mindate = datetime.datetime.strptime(mindate, "%Y%m%d")
        files = [
            f
            for f in files
            if mindate
            <= datetime.datetime.strptime(
                pathlib.Path(f).name.split("_")[2].split(".")[0], "%Y%m%d"
            )
        ]
    if maxdate is not None:
        maxdate = datetime.datetime.strptime(maxdate, "%Y%m%d")
        files = [
            f
            for f in files
            if datetime.datetime.strptime(
                pathlib.Path(f).name.split("_")[2].split(".")[0], "%Y%m%d"
            )
            <= maxdate
        ]

    if pausing_times is not None:
        # verify that its a list of length 2, with floats (hours)
        if len(pausing_times) != 2:
            raise Exception(
                "pausing_times must be a list of length 2, with floats (hours)"
            )
        if not all(isinstance(x, float) or isinstance(x, int) for x in pausing_times):
            raise Exception(
                "pausing_times must be a list of length 2, with floats (hours)"
            )

    input_list = [(f, rm, pausing_times) for f in files]

    with multiprocessing.Pool(processes=num_proc) as pool:
        for _ in tqdm(pool.imap(process_file, input_list), total=len(files)):
            pass


if __name__ == "__main__":
    fire.Fire(run)
