__all__ = [
    "add_admin",
    "ccd_quad_to_rc",
    "check_password_hash",
    "compute_dmdt",
    "compute_hash",
    "datetime_to_jd",
    "datetime_to_mjd",
    "days_to_hmsm",
    "deg2hms",
    "deg2dms",
    "desi_dr8_url",
    "forgiving_true",
    "generate_password_hash",
    "get_default_args",
    "great_circle_distance",
    "in_ellipse",
    "init_db",
    "init_db_sync",
    "jd_to_date",
    "jd_to_datetime",
    "memoize",
    "mjd_to_datetime",
    "Mongo",
    "radec_str2rad",
    "radec_str2geojson",
    "radec2lb",
    "sdss_url",
    "str_to_numeric",
    "time_stamp",
    "timer",
    "TimeoutHTTPAdapter",
    "uid",
    "ZTFAlert",
    "retry",
]

import base64
import datetime
import gzip
import hashlib
import inspect
import io
import math
import secrets
import string
import sys
import time
import traceback
from contextlib import contextmanager
from copy import deepcopy
from typing import Optional, Sequence

import bcrypt
import bson.json_util as bju
import numpy as np
import pandas as pd
import pymongo
from astropy.io import fits
from motor.motor_asyncio import AsyncIOMotorClient
from numba import jit
from pymongo.errors import BulkWriteError, OperationFailure
from requests.adapters import HTTPAdapter

from kowalski.log import time_stamp, log

LOG_DIR = "./logs"

pi = 3.141592653589793


DEFAULT_TIMEOUT = 5  # seconds

# create a decorator that retries a function call until there is no exception
# up to max_retries times with a timeout of timeout seconds


def retry(func, max_retries=10, timeout=6):
    def wrapper_retry(*args, **kwargs):
        n_retries = 0
        exception = None
        while n_retries < max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # this kind of error is not retryable
                if type(
                    e
                ) == OperationFailure and "Unrecognized pipeline stage name" in str(e):
                    raise e
                time.sleep(timeout)
                n_retries += 1
                exception = e
        raise exception

    return wrapper_retry


@contextmanager
def status(message):
    """
    Borrowed from https://github.com/cesium-ml/baselayer/

    :param message: message to print
    :return:
    """
    print(f"[·] {message}", end="")
    sys.stdout.flush()
    try:
        yield
    except Exception:
        print(f"\r[✗] {message}")
        raise
    else:
        print(f"\r[✓] {message}")


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        try:
            timeout = kwargs.get("timeout")
            if timeout is None:
                kwargs["timeout"] = self.timeout
            return super().send(request, **kwargs)
        except AttributeError:
            kwargs["timeout"] = DEFAULT_TIMEOUT


def forgiving_true(expression):
    return True if expression in ("t", "True", "true", "1", 1, True) else False


@contextmanager
def timer(description, verbose: bool = True):
    tic = time.time()
    yield
    toc = time.time()
    if verbose:
        log(f"{description} took {toc-tic} s")


def memoize(function):
    """Memoize function values on previously used arguments"""

    def memoized_function(*x):
        if x not in memoized_function.cache:
            memoized_function.cache[x] = function(*x)
        return memoized_function.cache[x]

    memoized_function.cache = dict()
    return memoized_function


def get_default_args(func):
    """Get default parameter values of a function. Useful for testing
    See https://stackoverflow.com/questions/12627118/get-a-function-arguments-default-value

    :param func:
    :return:
    """
    signature = inspect.signature(func)
    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }


def generate_password_hash(password, salt_rounds=12):
    password_bin = password.encode("utf-8")
    hashed = bcrypt.hashpw(password_bin, bcrypt.gensalt(salt_rounds))
    encoded = base64.b64encode(hashed)
    return encoded.decode("utf-8")


def check_password_hash(encoded, password):
    password = password.encode("utf-8")
    encoded = encoded.encode("utf-8")

    hashed = base64.b64decode(encoded)
    is_correct = bcrypt.hashpw(password, hashed) == hashed
    return is_correct


async def init_db(config, verbose=True):
    """
    Initialize db if necessary: create the sole non-admin user
    """
    if config["database"].get("srv", False) is True:
        conn_string = "mongodb+srv://"
    else:
        conn_string = "mongodb://"

    if (
        config["database"]["admin_username"] is not None
        and config["database"]["admin_password"] is not None
    ):
        conn_string += f"{config['database']['admin_username']}:{config['database']['admin_password']}@"

    conn_string += f"{config['database']['host']}"
    if config["database"]["srv"] is not True:
        conn_string += f":{config['database']['port']}"

    if config["database"]["replica_set"] is not None:
        conn_string += f"/?replicaSet={config['database']['replica_set']}"

    _client = AsyncIOMotorClient(conn_string)

    # to fix: on srv (like atlas) we can't do this
    if config["database"]["srv"] is not True:
        user_ids = []
        async for _u in _client.admin.system.users.find({}, {"_id": 1}):
            user_ids.append(_u["_id"])

        db_name = config["database"]["db"]
        username = config["database"]["username"]

        _mongo = _client[db_name]

        if f"{db_name}.{username}" not in user_ids:
            await _mongo.command(
                "createUser",
                config["database"]["username"],
                pwd=config["database"]["password"],
                roles=["readWrite"],
            )
            if verbose:
                print("Successfully initialized db")

        _mongo.client.close()


def init_db_sync(config, verbose=False):
    """
    Initialize db if necessary: create the sole non-admin user
    """
    if config["database"].get("srv, False") is True:
        conn_string = "mongodb+srv://"
    else:
        conn_string = "mongodb://"

    if (
        config["database"]["admin_username"] is not None
        and config["database"]["admin_password"] is not None
    ):
        conn_string += f"{config['database']['admin_username']}:{config['database']['admin_password']}@"

    conn_string += f"{config['database']['host']}"
    if config["database"]["srv"] is not True:
        conn_string += f":{config['database']['port']}"

    if config["database"]["replica_set"] is not None:
        conn_string += f"/?replicaSet={config['database']['replica_set']}"

    client = pymongo.MongoClient(conn_string)

    # to fix: on srv (like atlas) we can't do this
    if config["database"].get("srv", False) is not True:
        user_ids = []
        for _u in client.admin.system.users.find({}, {"_id": 1}):
            user_ids.append(_u["_id"])

        db_name = config["database"]["db"]
        username = config["database"]["username"]

        _mongo = client[db_name]

        if f"{db_name}.{username}" not in user_ids:
            _mongo.command(
                "createUser",
                config["database"]["username"],
                pwd=config["database"]["password"],
                roles=["readWrite"],
            )
            if verbose:
                log("Successfully initialized db")

        _mongo.client.close()


async def add_admin(_mongo, config):
    """
        Create admin user for the web interface if it does not exists already
    :return:
    """
    ex_admin = await _mongo.users.find_one({"_id": config["server"]["admin_username"]})
    if ex_admin is None or len(ex_admin) == 0:
        try:
            await _mongo.users.insert_one(
                {
                    "_id": config["server"]["admin_username"],
                    "email": "kowalski@caltech.edu",
                    "password": generate_password_hash(
                        config["server"]["admin_password"]
                    ),
                    "permissions": {},
                    "last_modified": datetime.datetime.utcnow(),
                }
            )
        except Exception as e:
            print(f"Got error: {str(e)}")
            _err = traceback.format_exc()
            print(_err)


class Mongo:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 27017,
        replica_set: Optional[str] = None,
        username: str = None,
        password: str = None,
        db: str = None,
        srv: bool = False,
        verbose=0,
        **kwargs,
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.replica_set = replica_set

        if srv is True:
            conn_string = "mongodb+srv://"
        else:
            conn_string = "mongodb://"

        if self.username is not None and self.password is not None:
            conn_string += f"{self.username}:{self.password}@"

        if srv is True:
            conn_string += f"{self.host}"
        else:
            conn_string += f"{self.host}:{self.port}"

        if db is not None:
            conn_string += f"/{db}"

        if self.replica_set is not None:
            conn_string += f"?replicaSet={self.replica_set}"

        self.client = pymongo.MongoClient(conn_string)
        self.db = self.client.get_database(db)

        self.verbose = verbose

    def insert_one(
        self, collection: str, document: dict, transaction: bool = False, **kwargs
    ):
        # note to future me: single-document operations in MongoDB are atomic
        # turn on transactions only if running a replica set
        try:
            if transaction:
                with self.client.start_session() as session:
                    with session.start_transaction():
                        self.db[collection].insert_one(document, session=session)
            else:
                self.db[collection].insert_one(document)
        except Exception as e:
            if self.verbose:
                print(
                    time_stamp(),
                    f"Error inserting document into collection {collection}: {str(e)}",
                )
                traceback.print_exc()

    def insert_many(
        self, collection: str, documents: list, transaction: bool = False, **kwargs
    ):
        ordered = kwargs.get("ordered", False)
        try:
            if transaction:
                with self.client.start_session() as session:
                    with session.start_transaction():
                        self.db[collection].insert_many(
                            documents, ordered=ordered, session=session
                        )
            else:
                self.db[collection].insert_many(documents, ordered=ordered)
        except BulkWriteError as bwe:
            if self.verbose:
                print(
                    time_stamp(),
                    f"Error inserting documents into collection {collection}: {str(bwe.details)}",
                )
                traceback.print_exc()
        except Exception as e:
            if self.verbose:
                print(
                    time_stamp(),
                    f"Error inserting documents into collection {collection}: {str(e)}",
                )
                traceback.print_exc()

    def update_one(
        self,
        collection: str,
        filt: dict,
        update: dict,
        transaction: bool = False,
        **kwargs,
    ):
        upsert = kwargs.get("upsert", True)

        try:
            if transaction:
                with self.client.start_session() as session:
                    with session.start_transaction():
                        self.db[collection].update_one(
                            filter=filt,
                            update=update,
                            upsert=upsert,
                            session=session,
                        )
            else:
                self.db[collection].update_one(
                    filter=filt, update=update, upsert=upsert
                )
        except Exception as e:
            if self.verbose:
                print(
                    time_stamp(),
                    f"Error inserting document into collection {collection}: {str(e)}",
                )
                traceback.print_exc()

    def delete_one(self, collection: str, document: dict, **kwargs):
        try:
            self.db[collection].delete_one(document)
        except Exception as e:
            log(
                f"Error deleting document from collection {collection}: {str(e)}",
            )
            traceback.print_exc()

    def close(self):
        try:
            self.client.close()
            return True
        except Exception as e:
            log(f"Error closing connection: {str(e)}")
            return False


def radec_str2rad(_ra_str, _dec_str):
    """
    :param _ra_str: 'H:M:S'
    :param _dec_str: 'D:M:S'
    :return: ra, dec in rad
    """
    # convert to rad:
    _ra = list(map(float, _ra_str.split(":")))
    _ra = (_ra[0] + _ra[1] / 60.0 + _ra[2] / 3600.0) * pi / 12.0
    _dec = list(map(float, _dec_str.split(":")))
    _sign = -1 if _dec_str.strip()[0] == "-" else 1
    _dec = (
        _sign
        * (abs(_dec[0]) + abs(_dec[1]) / 60.0 + abs(_dec[2]) / 3600.0)
        * pi
        / 180.0
    )

    return _ra, _dec


def radec_str2geojson(ra_str, dec_str):

    # hms -> ::, dms -> ::
    if isinstance(ra_str, str) and isinstance(dec_str, str):
        if ("h" in ra_str) and ("m" in ra_str) and ("s" in ra_str):
            ra_str = ra_str[:-1]  # strip 's' at the end
            for char in ("h", "m"):
                ra_str = ra_str.replace(char, ":")
        if ("d" in dec_str) and ("m" in dec_str) and ("s" in dec_str):
            dec_str = dec_str[:-1]  # strip 's' at the end
            for char in ("d", "m"):
                dec_str = dec_str.replace(char, ":")

        if (":" in ra_str) and (":" in dec_str):
            ra, dec = radec_str2rad(ra_str, dec_str)
            # convert to geojson-friendly degrees:
            ra = ra * 180.0 / pi - 180.0
            dec = dec * 180.0 / pi
        else:
            raise Exception("unrecognized string ra/dec format.")
    else:
        # already in degrees?
        ra = float(ra_str)
        # geojson-friendly ra:
        ra -= 180.0
        dec = float(dec_str)

    return ra, dec


def compute_hash(_task):
    """
        Compute hash for a hashable task
    :return:
    """
    ht = hashlib.blake2b(digest_size=16)
    ht.update(_task.encode("utf-8"))
    hsh = ht.hexdigest()

    return hsh


alphabet = string.ascii_lowercase + string.digits


def uid(length: int = 6, prefix: str = ""):
    return prefix + "".join(secrets.choice(alphabet) for _ in range(length))


def deg2hms(x):
    """Transform degrees to *hours:minutes:seconds* strings.

    Parameters
    ----------
    x : float
        The degree value c [0, 360) to be written as a sexagesimal string.

    Returns
    -------
    out : str
        The input angle written as a sexagesimal string, in the
        form, hours:minutes:seconds.

    """
    if not 0.0 <= x < 360.0:
        raise ValueError("Bad RA value in degrees")
    _h = np.floor(x * 12.0 / 180.0)
    _m = np.floor((x * 12.0 / 180.0 - _h) * 60.0)
    _s = ((x * 12.0 / 180.0 - _h) * 60.0 - _m) * 60.0
    hms = f"{_h:02.0f}:{_m:02.0f}:{_s:07.4f}"
    return hms


def deg2dms(x):
    """Transform degrees to *degrees:arcminutes:arcseconds* strings.

    Parameters
    ----------
    x : float
        The degree value c [-90, 90] to be converted.

    Returns
    -------
    out : str
        The input angle as a string, written as degrees:minutes:seconds.

    """
    if not -90.0 <= x <= 90.0:
        raise ValueError("Bad Dec value in degrees")
    _d = np.floor(abs(x)) * np.sign(x)
    _m = np.floor(np.abs(x - _d) * 60.0)
    _s = np.abs(np.abs(x - _d) * 60.0 - _m) * 60.0
    dms = f"{_d:02.0f}:{_m:02.0f}:{_s:06.3f}"
    return dms


@jit
def great_circle_distance(ra1_deg, dec1_deg, ra2_deg, dec2_deg):
    """
        Distance between two points on the sphere
    :param ra1_deg:
    :param dec1_deg:
    :param ra2_deg:
    :param dec2_deg:
    :return: distance in degrees
    """
    # this is orders of magnitude faster than astropy.coordinates.Skycoord.separation
    DEGRA = np.pi / 180.0
    ra1, dec1, ra2, dec2 = (
        ra1_deg * DEGRA,
        dec1_deg * DEGRA,
        ra2_deg * DEGRA,
        dec2_deg * DEGRA,
    )
    delta_ra = np.abs(ra2 - ra1)
    distance = np.arctan2(
        np.sqrt(
            (np.cos(dec2) * np.sin(delta_ra)) ** 2
            + (
                np.cos(dec1) * np.sin(dec2)
                - np.sin(dec1) * np.cos(dec2) * np.cos(delta_ra)
            )
            ** 2
        ),
        np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(delta_ra),
    )

    return distance * 180.0 / np.pi


@jit
def in_ellipse(alpha, delta0, alpha1, delta01, d0, axis_ratio, PA0):
    """
        Check if a given point (alpha, delta0)
        is within an ellipse specified by
        center (alpha1, delta01), maj_ax (d0), axis ratio and positional angle
        All angles are in decimal degrees
        Adapted from q3c: https://github.com/segasai/q3c/blob/master/q3cube.c
    :param alpha:
    :param delta0:
    :param alpha1:
    :param delta01:
    :param d0:
    :param axis_ratio:
    :param PA0:
    :return:
    """
    DEGRA = np.pi / 180.0

    # convert degrees to radians
    d_alpha = (alpha1 - alpha) * DEGRA
    delta1 = delta01 * DEGRA
    delta = delta0 * DEGRA
    PA = PA0 * DEGRA
    d = d0 * DEGRA
    e = np.sqrt(1.0 - axis_ratio * axis_ratio)

    t1 = np.cos(d_alpha)
    t22 = np.sin(d_alpha)
    t3 = np.cos(delta1)
    t32 = np.sin(delta1)
    t6 = np.cos(delta)
    t26 = np.sin(delta)
    t9 = np.cos(d)
    t55 = np.sin(d)

    if (t3 * t6 * t1 + t32 * t26) < 0:
        return False

    t2 = t1 * t1

    t4 = t3 * t3
    t5 = t2 * t4

    t7 = t6 * t6
    t8 = t5 * t7

    t10 = t9 * t9
    t11 = t7 * t10
    t13 = np.cos(PA)
    t14 = t13 * t13
    t15 = t14 * t10
    t18 = t7 * t14
    t19 = t18 * t10

    t24 = np.sin(PA)

    t31 = t1 * t3

    t36 = 2.0 * t31 * t32 * t26 * t6
    t37 = t31 * t32
    t38 = t26 * t6
    t45 = t4 * t10

    t56 = t55 * t55
    t57 = t4 * t7
    t60 = (
        -t8
        + t5 * t11
        + 2.0 * t5 * t15
        - t5 * t19
        - 2.0 * t1 * t4 * t22 * t10 * t24 * t13 * t26
        - t36
        + 2.0 * t37 * t38 * t10
        - 2.0 * t37 * t38 * t15
        - t45 * t14
        - t45 * t2
        + 2.0 * t22 * t3 * t32 * t6 * t24 * t10 * t13
        - t56
        + t7
        - t11
        + t4
        - t57
        + t57 * t10
        + t19
        - t18 * t45
    )
    t61 = e * e
    t63 = t60 * t61 + t8 + t57 - t4 - t7 + t56 + t36

    return t63 > 0


# Rotation matrix for the conversion : x_galactic = R * x_equatorial (J2000)
# http://adsabs.harvard.edu/abs/1989A&A...218..325M
RGE = np.array(
    [
        [-0.054875539, -0.873437105, -0.483834992],
        [+0.494109454, -0.444829594, +0.746982249],
        [-0.867666136, -0.198076390, +0.455983795],
    ]
)


def radec2lb(ra, dec):
    """
        Convert $R.A.$ and $Decl.$ into Galactic coordinates $l$ and $b$
    ra [deg]
    dec [deg]

    return l [deg], b [deg]
    """
    ra_rad, dec_rad = np.deg2rad(ra), np.deg2rad(dec)
    u = np.array(
        [
            np.cos(ra_rad) * np.cos(dec_rad),
            np.sin(ra_rad) * np.cos(dec_rad),
            np.sin(dec_rad),
        ]
    )

    ug = np.dot(RGE, u)

    x, y, z = ug
    galactic_l = np.arctan2(y, x)
    galactic_b = np.arctan2(z, (x * x + y * y) ** 0.5)
    return np.rad2deg(galactic_l), np.rad2deg(galactic_b)


def datetime_to_jd(_t: datetime.datetime) -> float:
    """
    Calculate Julian Date from datetime.datetime
    """

    a = np.floor((14 - _t.month) / 12)
    y = _t.year + 4800 - a
    m = _t.month + 12 * a - 3

    jdn = (
        _t.day
        + np.floor((153 * m + 2) / 5.0)
        + 365 * y
        + np.floor(y / 4.0)
        - np.floor(y / 100.0)
        + np.floor(y / 400.0)
        - 32045
    )

    _jd = (
        jdn
        + (_t.hour - 12.0) / 24.0
        + _t.minute / 1440.0
        + _t.second / 86400.0
        + _t.microsecond / 86400000000.0
    )

    return _jd


def datetime_to_mjd(_t: datetime.datetime) -> float:
    """
    Calculate Modified Julian Date
    """
    _jd = datetime_to_jd(_t)
    _mjd = _jd - 2400000.5
    return _mjd


def days_to_hmsm(days):
    """
    Convert fractional days to hours, minutes, seconds, and microseconds.
    Precision beyond microseconds is rounded to the nearest microsecond.
    Parameters
    ----------
    days : float
        A fractional number of days. Must be less than 1.
    Returns
    -------
    hour : int
        Hour number.
    min : int
        Minute number.
    sec : int
        Second number.
    micro : int
        Microsecond number.
    Raises
    ------
    ValueError
        If `days` is >= 1.
    Examples
    --------
    >>> days_to_hmsm(0.1)
    (2, 24, 0, 0)
    """
    hours = days * 24.0
    hours, hour = math.modf(hours)

    mins = hours * 60.0
    mins, min = math.modf(mins)

    secs = mins * 60.0
    secs, sec = math.modf(secs)

    micro = round(secs * 1.0e6)

    return int(hour), int(min), int(sec), int(micro)


def jd_to_date(jd):
    """
    Convert Julian Day to date.
    Algorithm from 'Practical Astronomy with your Calculator or Spreadsheet',
        4th ed., Duffet-Smith and Zwart, 2011.
    Parameters
    ----------
    jd : float
        Julian Day
    Returns
    -------
    year : int
        Year as integer. Years preceding 1 A.D. should be 0 or negative.
        The year before 1 A.D. is 0, 10 B.C. is year -9.
    month : int
        Month as integer, Jan = 1, Feb. = 2, etc.
    day : float
        Day, may contain fractional part.
    Examples
    --------
    Convert Julian Day 2446113.75 to year, month, and day.
    >>> jd_to_date(2446113.75)
    (1985, 2, 17.25)
    """
    jd = jd + 0.5

    F, Ii = math.modf(jd)
    Ii = int(Ii)

    A = math.trunc((Ii - 1867216.25) / 36524.25)

    if Ii > 2299160:
        B = Ii + 1 + A - math.trunc(A / 4.0)
    else:
        B = Ii

    C = B + 1524

    D = math.trunc((C - 122.1) / 365.25)

    E = math.trunc(365.25 * D)

    G = math.trunc((C - E) / 30.6001)

    day = C - E + F - math.trunc(30.6001 * G)

    if G < 13.5:
        month = G - 1
    else:
        month = G - 13

    if month > 2.5:
        year = D - 4716
    else:
        year = D - 4715

    return year, month, day


def jd_to_datetime(_jd):
    """
    Convert a Julian Day to an `jdutil.datetime` object.
    Parameters
    ----------
    _jd : float
        Julian day.
    Returns
    -------
    dt : `jdutil.datetime` object
        `jdutil.datetime` equivalent of Julian day.
    Examples
    --------
    >>> jd_to_datetime(2446113.75)
    datetime(1985, 2, 17, 6, 0)
    """
    year, month, day = jd_to_date(_jd)

    frac_days, day = math.modf(day)
    day = int(day)

    hour, min_, sec, micro = days_to_hmsm(frac_days)

    return datetime.datetime(year, month, day, hour, min_, sec, micro)


def mjd_to_datetime(_mjd):
    _jd = _mjd + 2400000.5

    return jd_to_datetime(_jd)


def sdss_url(ra: float, dec: float):
    """
    Construct URL for public Sloan Digital Sky Survey (SDSS) cutout.

    from SkyPortal
    """
    return (
        f"http://skyserver.sdss.org/dr12/SkyserverWS/ImgCutout/getjpeg"
        f"?ra={ra}&dec={dec}&scale=0.3&width=200&height=200"
        f"&opt=G&query=&Grid=on"
    )


def desi_dr8_url(ra, dec):
    """
    Construct URL for public DESI DR8 cutout.

    from SkyPortal
    """
    return (
        f"http://legacysurvey.org/viewer/jpeg-cutout?ra={ra}"
        f"&dec={dec}&size=200&layer=dr8&pixscale=0.262&bands=grz"
    )


DMDT_INTERVALS = {
    "crts": {
        "dm_intervals": [
            -8,
            -5,
            -3,
            -2.5,
            -2,
            -1.5,
            -1,
            -0.5,
            -0.3,
            -0.2,
            -0.1,
            0,
            0.1,
            0.2,
            0.3,
            0.5,
            1,
            1.5,
            2,
            2.5,
            3,
            5,
            8,
        ],
        "dt_intervals": [
            0.0,
            1.0 / 145,
            2.0 / 145,
            3.0 / 145,
            4.0 / 145,
            1.0 / 25,
            2.0 / 25,
            3.0 / 25,
            1.5,
            2.5,
            3.5,
            4.5,
            5.5,
            7,
            10,
            20,
            30,
            60,
            90,
            120,
            240,
            600,
            960,
            2000,
            4000,
        ],
    },
    "v20200205": {
        "dm_intervals": [
            -8,
            -5,
            -4,
            -3,
            -2.5,
            -2,
            -1.5,
            -1,
            -0.5,
            -0.3,
            -0.2,
            -0.1,
            -0.05,
            0,
            0.05,
            0.1,
            0.2,
            0.3,
            0.5,
            1,
            1.5,
            2,
            2.5,
            3,
            4,
            5,
            8,
        ],
        "dt_intervals": [
            0.0,
            4.0 / 145,
            1.0 / 25,
            2.0 / 25,
            3.0 / 25,
            0.3,
            0.75,
            1,
            1.5,
            2.5,
            3.5,
            4.5,
            5.5,
            7,
            10,
            20,
            30,
            45,
            60,
            90,
            120,
            180,
            240,
            360,
            500,
            650,
            2000,
        ],
    },
    "v20200318": {
        "dm_intervals": [
            -8,
            -4.5,
            -3,
            -2.5,
            -2,
            -1.5,
            -1.25,
            -0.75,
            -0.5,
            -0.3,
            -0.2,
            -0.1,
            -0.05,
            0,
            0.05,
            0.1,
            0.2,
            0.3,
            0.5,
            0.75,
            1.25,
            1.5,
            2,
            2.5,
            3,
            4.5,
            8,
        ],
        "dt_intervals": [
            0.0,
            4.0 / 145,
            1.0 / 25,
            2.0 / 25,
            3.0 / 25,
            0.3,
            0.5,
            0.75,
            1,
            1.5,
            2.5,
            3.5,
            4.5,
            5.5,
            7,
            10,
            20,
            30,
            45,
            60,
            75,
            90,
            120,
            150,
            180,
            210,
            240,
        ],
    },
}


@jit
def pwd_for(a: Sequence):
    """
    Compute pairwise differences with for loops
    """
    return np.array([a[j] - a[i] for i in range(len(a)) for j in range(i + 1, len(a))])


def compute_dmdt(jd: Sequence, mag: Sequence, dmdt_ints_v: str = "v20200318"):
    """Compute dmdt matrix for time series (jd, mag)
    See arXiv:1709.06257

    :param jd:
    :param mag:
    :param dmdt_ints_v:
    :return:
    """
    jd_diff = pwd_for(jd)
    mag_diff = pwd_for(mag)

    dmdt, ex, ey = np.histogram2d(
        jd_diff,
        mag_diff,
        bins=[
            DMDT_INTERVALS[dmdt_ints_v]["dt_intervals"],
            DMDT_INTERVALS[dmdt_ints_v]["dm_intervals"],
        ],
    )

    dmdt = np.transpose(dmdt)
    norm = np.linalg.norm(dmdt)
    if norm != 0.0:
        dmdt /= np.linalg.norm(dmdt)
    else:
        dmdt = np.zeros_like(dmdt)

    return dmdt


@jit
def ccd_quad_to_rc(ccd: int, quad: int) -> int:
    """Convert ZTF CCD/QUADRANT to readout channel number

    :param ccd:
    :param quad:
    :return:
    """
    if ccd not in range(1, 17):
        raise ValueError("Bad CCD number")
    if quad not in range(1, 5):
        raise ValueError("Bad QUADRANT number")
    b = (ccd - 1) * 4
    rc = b + quad - 1
    return rc


def str_to_numeric(s):
    try:
        return int(s)
    except ValueError:
        return float(s)


class ZTFAlert:
    def __init__(self, alert, alert_history, models, label=None, **kwargs):
        self.kwargs = kwargs

        self.label = label

        self.alert = deepcopy(alert)

        # ADD EXTRA FEATURES
        peakmag_jd = alert["candidate"]["jd"]
        peakmag = 30
        maxmag = 0
        # find the mjd of the peak magnitude
        for a in alert_history:
            if a.get("magpsf", None) is not None:
                if a["magpsf"] < peakmag:
                    peakmag = a["magpsf"]
                    peakmag_jd = a["jd"]
                if a["magpsf"] > maxmag:
                    maxmag = a["magpsf"]

        first_alert_jd = min(
            alert["candidate"].get("jdstarthist", None),
            min(
                [alert["candidate"]["jd"]]
                + [a["jd"] for a in alert_history if a.get("magpsf", None) is not None]
            ),
        )

        # add a peakmag_so_far field to the alert (min of all magpsf)
        self.alert["candidate"]["peakmag_so_far"] = peakmag

        # add a maxmag_so_far field to the alert (max of all magpsf)
        self.alert["candidate"]["maxmag_so_far"] = maxmag

        # add a days_since_peak field to the alert (jd - peakmag_jd)
        self.alert["candidate"]["days_since_peak"] = (
            self.alert["candidate"]["jd"] - peakmag_jd
        )

        # add a days_to_peak field to the alert (peakmag_jd - first_alert_jd)
        self.alert["candidate"]["days_to_peak"] = peakmag_jd - first_alert_jd

        # add an age field to the alert: (jd - first_alert_jd)
        self.alert["candidate"]["age"] = self.alert["candidate"]["jd"] - first_alert_jd

        # number of non-detections: ncovhist - ndethist
        self.alert["candidate"]["nnondet"] = alert["candidate"].get(
            "ncovhist", 0
        ) - alert["candidate"].get("ndethist", 0)

        triplet_normalize = kwargs.get("triplet_normalize", True)
        to_tpu = kwargs.get("to_tpu", False)
        # dmdt_up_to_candidate_jd = kwargs.get("dmdt_up_to_candidate_jd", True)

        triplet = self.make_triplet(normalize=triplet_normalize, to_tpu=to_tpu)
        features = self.make_features_by_model(models)
        # dmdt = self.make_dmdt(up_to_candidate_jd=dmdt_up_to_candidate_jd)

        self.data = {
            "triplet": triplet,
            "features": features
            # "dmdt": dmdt
        }

        del peakmag_jd, peakmag, maxmag, first_alert_jd

    def make_triplet(self, normalize: bool = True, to_tpu: bool = False):
        """
        Feed in alert packet
        """
        cutout_dict = dict()

        for cutout in ("science", "template", "difference"):
            cutout_data = bju.loads(
                bju.dumps([self.alert[f"cutout{cutout.capitalize()}"]["stampData"]])
            )[0]

            # unzip
            with gzip.open(io.BytesIO(cutout_data), "rb") as f:
                with fits.open(io.BytesIO(f.read()), ignore_missing_simple=True) as hdu:
                    data = hdu[0].data
                    # replace nans with zeros
                    cutout_dict[cutout] = np.nan_to_num(data)
                    # normalize
                    if normalize:
                        cutout_dict[cutout] /= np.linalg.norm(cutout_dict[cutout])

            # pad to 63x63 if smaller
            shape = cutout_dict[cutout].shape
            if shape != (63, 63):
                cutout_dict[cutout] = np.pad(
                    cutout_dict[cutout],
                    [(0, 63 - shape[0]), (0, 63 - shape[1])],
                    mode="constant",
                    constant_values=1e-9,
                )

        triplet = np.zeros((63, 63, 3))
        triplet[:, :, 0] = cutout_dict["science"]
        triplet[:, :, 1] = cutout_dict["template"]
        triplet[:, :, 2] = cutout_dict["difference"]

        if to_tpu:
            # Edge TPUs require additional processing
            triplet = np.rint(triplet * 128 + 128).astype(np.uint8).flatten()

        return triplet

    def make_features(self, feature_names=None, norms=None):
        features = []
        for feature_name in feature_names:
            feature = self.alert["candidate"].get(feature_name)
            if feature is None and feature_name == "drb":
                feature = self.alert["candidate"].get("rb")
            if norms is not None:
                feature /= norms.get(feature, 1)
            features.append(feature)

        features = np.array(features)
        return features

    def make_features_by_model(self, models):
        features_by_model = {}
        for model_name in models.keys():
            feature_names = models[model_name]["feature_names"]
            feature_norms = models[model_name]["feature_norms"]
            if feature_names is not False:
                features_by_model[model_name] = self.make_features(
                    feature_names=feature_names, norms=feature_norms
                )
            else:
                features_by_model[model_name] = np.array([])

        return features_by_model

    def make_dmdt(self, up_to_candidate_jd=True, min_points=4):
        df_candidate = pd.DataFrame(self.alert["candidate"], index=[0])

        df_prv_candidates = pd.DataFrame(self.alert["prv_candidates"])
        df_light_curve = pd.concat(
            [df_candidate, df_prv_candidates], ignore_index=True, sort=False
        )

        ztf_filters = {1: "ztfg", 2: "ztfr", 3: "ztfi"}
        df_light_curve["filter"] = df_light_curve["fid"].apply(lambda x: ztf_filters[x])
        df_light_curve["magsys"] = "ab"
        df_light_curve["mjd"] = df_light_curve["jd"] - 2400000.5

        df_light_curve["mjd"] = df_light_curve["mjd"].apply(lambda x: np.float64(x))
        df_light_curve["magpsf"] = df_light_curve["magpsf"].apply(
            lambda x: np.float32(x)
        )
        df_light_curve["sigmapsf"] = df_light_curve["sigmapsf"].apply(
            lambda x: np.float32(x)
        )

        df_light_curve = (
            df_light_curve.drop_duplicates(subset=["mjd", "magpsf"])
            .reset_index(drop=True)
            .sort_values(by=["mjd"])
        )

        dmdt = np.zeros((26, 26, 3))
        for i, fid in enumerate([1, 2, 3]):
            mask_fid = df_light_curve.fid == fid
            if sum(mask_fid) >= min_points:
                dmdt[:, :, i] = compute_dmdt(
                    df_light_curve.loc[mask_fid, "jd"],
                    df_light_curve.loc[mask_fid, "mag"],
                )
            else:
                dmdt[:, :, i] = np.zeros((26, 26))

        return dmdt
