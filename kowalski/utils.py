import bcrypt
import base64
import datetime
import json
from motor.motor_asyncio import AsyncIOMotorClient
import os
import traceback


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

