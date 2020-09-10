import bcrypt
import base64
import datetime
import hashlib
import math
from motor.motor_asyncio import AsyncIOMotorClient
from numba import jit
import numpy as np
import os
import pymongo
from pymongo.errors import BulkWriteError
import secrets
import string
import traceback
import yaml


pi = 3.141592653589793


def load_config(path='/app', config_file='config.yaml'):
    """
        Load config and secrets
    """
    with open(os.path.join(path, config_file)) as cyaml:
        config = yaml.load(cyaml, Loader=yaml.FullLoader)

    return config


def time_stamp():
    """

    :return: UTC time -> string
    """
    return datetime.datetime.utcnow().strftime('%Y%m%d_%H:%M:%S')


def generate_password_hash(password, salt_rounds=12):
    password_bin = password.encode('utf-8')
    hashed = bcrypt.hashpw(password_bin, bcrypt.gensalt(salt_rounds))
    encoded = base64.b64encode(hashed)
    return encoded.decode('utf-8')


def check_password_hash(encoded, password):
    password = password.encode('utf-8')
    encoded = encoded.encode('utf-8')

    hashed = base64.b64decode(encoded)
    is_correct = bcrypt.hashpw(password, hashed) == hashed
    return is_correct


async def init_db(config, verbose=True):
    """
        Initialize db if necessary: create the sole non-admin user
    """
    _client = AsyncIOMotorClient(username=config['database']['admin_username'],
                                 password=config['database']['admin_password'],
                                 host=config['database']['host'],
                                 port=config['database']['port'])

    # _id: db_name.user_name
    user_ids = []
    async for _u in _client.admin.system.users.find({}, {'_id': 1}):
        user_ids.append(_u['_id'])

    # print(user_ids)

    db_name = config['database']['db']
    username = config['database']['username']

    # print(f'{db_name}.{username}')
    # print(user_ids)

    _mongo = _client[db_name]

    if f'{db_name}.{username}' not in user_ids:
        await _mongo.command('createUser', config['database']['username'],
                             pwd=config['database']['password'], roles=['readWrite'])
        if verbose:
            print('Successfully initialized db')

    _mongo.client.close()


async def add_admin(_mongo, config):
    """
        Create admin user for the web interface if it does not exists already
    :return:
    """
    ex_admin = await _mongo.users.find_one({'_id': config['server']['admin_username']})
    if ex_admin is None or len(ex_admin) == 0:
        try:
            await _mongo.users.insert_one({'_id': config['server']['admin_username'],
                                           'email': 'kowalski@caltech.edu',
                                           'password': generate_password_hash(config['server']['admin_password']),
                                           'permissions': {},
                                           'last_modified': datetime.datetime.utcnow()
                                           })
        except Exception as e:
            print(f'Got error: {str(e)}')
            _err = traceback.format_exc()
            print(_err)


class Mongo(object):
    def __init__(
            self,
            host: str = '127.0.0.1', port: str = '27017',
            username: str = None, password: str = None,
            db: str = None,
            verbose=0,
            **kwargs
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        self.client = pymongo.MongoClient(host=self.host, port=self.port)
        self.db = self.client[db]
        # authenticate
        self.db.authenticate(self.username, self.password)

        self.verbose = verbose

    def insert_one(self, collection: str, document: dict, transaction: bool = False, **kwargs):
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
                print(time_stamp(), f"Error inserting document into collection {collection}: {str(e)}")
                traceback.print_exc()

    def insert_many(self, collection: str, documents: list, transaction: bool = False, **kwargs):
        ordered = kwargs.get("ordered", False)
        try:
            if transaction:
                with self.client.start_session() as session:
                    with session.start_transaction():
                        self.db[collection].insert_many(documents, ordered=ordered, session=session)
            else:
                self.db[collection].insert_many(documents, ordered=ordered)
        except BulkWriteError as bwe:
            if self.verbose:
                print(time_stamp(), f"Error inserting documents into collection {collection}: {str(bwe.details)}")
                traceback.print_exc()
        except Exception as e:
            if self.verbose:
                print(time_stamp(), f"Error inserting documents into collection {collection}: {str(e)}")
                traceback.print_exc()

    def update_one(self, collection: str, filt: dict, update: dict, transaction: bool = False, **kwargs):
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
                    filter=filt,
                    update=update,
                    upsert=upsert
                )
        except Exception as e:
            if self.verbose:
                print(time_stamp(), f"Error inserting document into collection {collection}: {str(e)}")
                traceback.print_exc()


def radec_str2rad(_ra_str, _dec_str):
    """
    :param _ra_str: 'H:M:S'
    :param _dec_str: 'D:M:S'
    :return: ra, dec in rad
    """
    # convert to rad:
    _ra = list(map(float, _ra_str.split(':')))
    _ra = (_ra[0] + _ra[1] / 60.0 + _ra[2] / 3600.0) * pi / 12.
    _dec = list(map(float, _dec_str.split(':')))
    _sign = -1 if _dec_str.strip()[0] == '-' else 1
    _dec = _sign * (abs(_dec[0]) + abs(_dec[1]) / 60.0 + abs(_dec[2]) / 3600.0) * pi / 180.

    return _ra, _dec


def radec_str2geojson(ra_str, dec_str):

    # hms -> ::, dms -> ::
    if isinstance(ra_str, str) and isinstance(dec_str, str):
        if ('h' in ra_str) and ('m' in ra_str) and ('s' in ra_str):
            ra_str = ra_str[:-1]  # strip 's' at the end
            for char in ('h', 'm'):
                ra_str = ra_str.replace(char, ':')
        if ('d' in dec_str) and ('m' in dec_str) and ('s' in dec_str):
            dec_str = dec_str[:-1]  # strip 's' at the end
            for char in ('d', 'm'):
                dec_str = dec_str.replace(char, ':')

        if (':' in ra_str) and (':' in dec_str):
            ra, dec = radec_str2rad(ra_str, dec_str)
            # convert to geojson-friendly degrees:
            ra = ra * 180.0 / pi - 180.0
            dec = dec * 180.0 / pi
        else:
            raise Exception('unrecognized string ra/dec format.')
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
    ht.update(_task.encode('utf-8'))
    hsh = ht.hexdigest()

    return hsh


alphabet = string.ascii_lowercase + string.digits


def uid(length: int = 6, prefix: str = ''):
    return prefix + ''.join(secrets.choice(alphabet) for _ in range(length))


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
        raise ValueError('Bad RA value in degrees')
    _h = np.floor(x * 12.0 / 180.)
    _m = np.floor((x * 12.0 / 180. - _h) * 60.0)
    _s = ((x * 12.0 / 180. - _h) * 60.0 - _m) * 60.0
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
        raise ValueError('Bad Dec value in degrees')
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
    ra1, dec1, ra2, dec2 = ra1_deg * DEGRA, dec1_deg * DEGRA, ra2_deg * DEGRA, dec2_deg * DEGRA
    delta_ra = np.abs(ra2 - ra1)
    distance = np.arctan2(np.sqrt((np.cos(dec2) * np.sin(delta_ra)) ** 2
                                  + (np.cos(dec1) * np.sin(dec2) - np.sin(dec1) * np.cos(dec2) * np.cos(
        delta_ra)) ** 2),
                          np.sin(dec1) * np.sin(dec2) + np.cos(dec1) * np.cos(dec2) * np.cos(delta_ra))

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
    t60 = -t8 + t5 * t11 + 2.0 * t5 * t15 - t5 * t19 - \
          2.0 * t1 * t4 * t22 * t10 * t24 * t13 * t26 - t36 + \
          2.0 * t37 * t38 * t10 - 2.0 * t37 * t38 * t15 - t45 * t14 - t45 * t2 + \
          2.0 * t22 * t3 * t32 * t6 * t24 * t10 * t13 - t56 + t7 - t11 + t4 - t57 + t57 * t10 + t19 - t18 * t45
    t61 = e * e
    t63 = t60 * t61 + t8 + t57 - t4 - t7 + t56 + t36

    return t63 > 0


# Rotation matrix for the conversion : x_galactic = R * x_equatorial (J2000)
# http://adsabs.harvard.edu/abs/1989A&A...218..325M
RGE = np.array([[-0.054875539, -0.873437105, -0.483834992],
                [+0.494109454, -0.444829594, +0.746982249],
                [-0.867666136, -0.198076390, +0.455983795]])


@jit
def radec2lb(ra, dec):
    """
            Convert $R.A.$ and $Decl.$ into Galactic coordinates $l$ and $b$
        ra [deg]
        dec [deg]

        return l [deg], b [deg]
    """
    ra_rad, dec_rad = np.deg2rad(ra), np.deg2rad(dec)
    u = np.array([np.cos(ra_rad) * np.cos(dec_rad),
                  np.sin(ra_rad) * np.cos(dec_rad),
                  np.sin(dec_rad)])

    ug = np.dot(RGE, u)

    x, y, z = ug
    l = np.arctan2(y, x)
    b = np.arctan2(z, (x * x + y * y) ** .5)
    return np.rad2deg(l), np.rad2deg(b)


def datetime_to_jd(_t: datetime.datetime) -> float:
    """
    Calculate Julian Date from datetime.datetime
    """

    a = np.floor((14 - _t.month) / 12)
    y = _t.year + 4800 - a
    m = _t.month + 12 * a - 3

    jdn = _t.day + np.floor((153 * m + 2) / 5.) + 365 * y + np.floor(y / 4.) - np.floor(y / 100.) + np.floor(
        y / 400.) - 32045

    _jd = jdn + (_t.hour - 12.) / 24. + _t.minute / 1440. + _t.second / 86400. + _t.microsecond / 86400000000.

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
    hours = days * 24.
    hours, hour = math.modf(hours)

    mins = hours * 60.
    mins, min = math.modf(mins)

    secs = mins * 60.
    secs, sec = math.modf(secs)

    micro = round(secs * 1.e6)

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

    F, I = math.modf(jd)
    I = int(I)

    A = math.trunc((I - 1867216.25) / 36524.25)

    if I > 2299160:
        B = I + 1 + A - math.trunc(A / 4.)
    else:
        B = I

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
