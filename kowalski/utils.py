import bcrypt
import base64
import datetime
import hashlib
import json
from motor.motor_asyncio import AsyncIOMotorClient
from numba import jit
import numpy as np
import os
import secrets
import string
import traceback


pi = 3.141592653589793


def load_config(path='/app', config_file='config.json', secrets_file='secrets.json'):
    """
        Load config and secrets
    """
    with open(os.path.join(path, config_file)) as cjson:
        config = json.load(cjson)

    with open(os.path.join(path, secrets_file)) as sjson:
        secrets = json.load(sjson)

    for k in secrets:
        if k in config:
            config[k].update(secrets.get(k, {}))
        else:
            config[k] = secrets.get(k, {})

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
    _client = AsyncIOMotorClient(username=config['database']['admin'],
                                 password=config['database']['admin_pwd'],
                                 host=config['database']['host'],
                                 port=config['database']['port'])

    # _id: db_name.user_name
    user_ids = []
    async for _u in _client.admin.system.users.find({}, {'_id': 1}):
        user_ids.append(_u['_id'])

    # print(user_ids)

    db_name = config['database']['db']
    username = config['database']['user']

    # print(f'{db_name}.{username}')
    # print(user_ids)

    _mongo = _client[db_name]

    if f'{db_name}.{username}' not in user_ids:
        await _mongo.command('createUser', config['database']['user'],
                             pwd=config['database']['pwd'], roles=['readWrite'])
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


@jit
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
    assert 0.0 <= x < 360.0, 'Bad RA value in degrees'
    # ac = Angle(x, unit='degree')
    # hms = str(ac.to_string(unit='hour', sep=':', pad=True))
    # print(str(hms))
    _h = np.floor(x * 12.0 / 180.)
    _m = np.floor((x * 12.0 / 180. - _h) * 60.0)
    _s = ((x * 12.0 / 180. - _h) * 60.0 - _m) * 60.0
    hms = '{:02.0f}:{:02.0f}:{:07.4f}'.format(_h, _m, _s)
    # print(hms)
    return hms


@jit
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
    assert -90.0 <= x <= 90.0, 'Bad Dec value in degrees'
    # ac = Angle(x, unit='degree')
    # dms = str(ac.to_string(unit='degree', sep=':', pad=True))
    # print(dms)
    _d = np.floor(abs(x)) * np.sign(x)
    _m = np.floor(np.abs(x - _d) * 60.0)
    _s = np.abs(np.abs(x - _d) * 60.0 - _m) * 60.0
    dms = '{:02.0f}:{:02.0f}:{:06.3f}'.format(_d, _m, _s)
    # print(dms)
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