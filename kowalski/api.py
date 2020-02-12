import aiofiles
from aiohttp import web
from astropy.io import fits
import asyncio
from ast import literal_eval
from bson.json_util import dumps, loads
import datetime
import gzip
import io
import jwt
from matplotlib.colors import LogNorm
import matplotlib.pyplot as plt
from middlewares import auth_middleware, auth_required
from motor.motor_asyncio import AsyncIOMotorClient
from multidict import MultiDict
import numpy as np
import os
import pathlib
import shutil
import traceback
from utils import add_admin, check_password_hash, compute_hash, generate_password_hash, \
    init_db, load_config, radec_str2geojson, uid
import uvloop


config = load_config(config_file='config_api.json')

routes = web.RouteTableDef()


''' authentication and authorization '''


@routes.post('/api/auth')
async def auth(request):
    """
        Authenticate
        todo: swagger!
    """
    try:
        try:
            post_data = await request.json()
        except Exception as _e:
            # print(f'Cannot extract json() from request, trying post(): {str(_e)}')
            post_data = await request.post()

        # must contain 'username' and 'password'
        if ('username' not in post_data) or (len(post_data['username']) == 0):
            return web.json_response({'status': 'error', 'message': 'missing username'}, status=400)
        if ('password' not in post_data) or (len(post_data['password']) == 0):
            return web.json_response({'status': 'error', 'message': 'missing password'}, status=400)

        # connecting from penquins: check penquins version
        if 'penquins.__version__' in post_data:
            penquins_version = post_data['penquins.__version__']
            if penquins_version not in config['misc']['supported_penquins_versions']:
                return web.json_response({'status': 'error',
                                          'message': 'unsupported version of penquins: '
                                                     f'{post_data["penquins.__version__"]}'}, status=400)

        username = str(post_data['username'])
        password = str(post_data['password'])

        try:
            # user exists and passwords match?
            select = await request.app['mongo'].users.find_one({'_id': username})
            if check_password_hash(select['password'], password):
                payload = {
                    'user_id': username,
                    'exp': datetime.datetime.utcnow() + datetime.timedelta(
                        seconds=request.app['JWT']['JWT_EXP_DELTA_SECONDS'])
                }
                jwt_token = jwt.encode(payload,
                                       request.app['JWT']['JWT_SECRET'],
                                       request.app['JWT']['JWT_ALGORITHM'])

                return web.json_response({'status': 'success', 'token': jwt_token.decode('utf-8')})

            else:
                return web.json_response({'status': 'error', 'message': 'wrong credentials'}, status=401)

        except Exception as _e:
            # print(f'{datetime.datetime.now()} got error: {str(_e)}')
            # _err = traceback.format_exc()
            # print(_err)
            return web.json_response({'status': 'error', 'message': 'wrong credentials'}, status=401)

    except Exception as _e:
        # print(f'{datetime.datetime.now()} got error: {str(_e)}')
        # _err = traceback.format_exc()
        # print(_err)
        return web.json_response({'status': 'error', 'message': 'auth failed'}, status=500)


@routes.get('/', name='root')
@auth_required
async def root_handler(request):
    """
        Ping pong
    :param request:
    :return:
    """
    return web.json_response({'status': 'success', 'message': 'greetings from Kowalski!'}, status=200)


''' users api '''


@routes.put('/api/users')
@auth_required
async def add_user(request):
    """
        Add new user
    :return:
    """
    _data = await request.json()

    if request.user == config['server']['admin_username']:
        try:
            username = _data.get('user', '')
            password = _data.get('password', '')
            email = _data.get('email', '')
            permissions = _data.get('permissions', '{}')

            if len(username) == 0 or len(password) == 0:
                return web.json_response({'status': 'error',
                                          'message': 'username and password must be set'}, status=500)

            if len(permissions) == 0:
                permissions = '{}'

            # add user to coll_usr collection:
            await request.app['mongo'].users.insert_one(
                {'_id': username,
                 'email': email,
                 'password': generate_password_hash(password),
                 'permissions': literal_eval(str(permissions)),
                 'last_modified': datetime.datetime.now()}
            )

            return web.json_response({'status': 'success', 'message': f'added user {username}'}, status=200)

        except Exception as _e:
            return web.json_response({'status': 'error', 'message': f'failed to add user: {_e}'}, status=500)
    else:
        return web.json_response({'status': 'error', 'message': 'must be admin to add users'}, status=403)


@routes.delete('/api/users')
@auth_required
async def remove_user(request):
    """
        Remove user
    :return:
    """
    _data = await request.json()

    if request.user == config['server']['admin_username']:
        try:
            username = _data.get('user', None)
            if username == config['server']['admin_username']:
                return web.json_response({'status': 'error', 'message': 'cannot remove the superuser!'}, status=500)

            # try to remove the user:
            if username is not None:
                await request.app['mongo'].users.delete_one({'_id': username})

            return web.json_response({'status': 'success',
                                      'message': f'successfully removed user {username}'}, status=200)

        except Exception as _e:
            return web.json_response({'status': 'error',
                                      'message': f'failed to remove user: {_e}'}, status=500)
    else:
        return web.json_response({'status': 'error', 'message': 'must be admin to delete users'}, status=403)


@routes.post('/api/users')
@auth_required
async def edit_user(request):
    """
        Edit user info
    :return:
    """
    _data = await request.json()

    if request.user == config['server']['admin_username']:
        try:
            _id = _data.get('_user', None)
            username = _data.get('edit-user', '')
            password = _data.get('edit-password', '')

            if _id == config['server']['admin_username'] and username != config['server']['admin_username']:
                return web.json_response({'status': 'error',
                                          'message': 'cannot change the admin username!'}, status=500)

            if len(username) == 0:
                return web.json_response({'status': 'error',
                                          'message': 'username must be set'}, status=500)

            # change username:
            if _id != username:
                select = await request.app['mongo'].users.find_one({'_id': _id})
                select['_id'] = username
                await request.app['mongo'].users.insert_one(select)
                await request.app['mongo'].users.delete_one({'_id': _id})

            # change password:
            if len(password) != 0:
                await request.app['mongo'].users.update_one(
                    {'_id': username},
                    {
                        '$set': {
                            'password': generate_password_hash(password)
                        },
                        '$currentDate': {'last_modified': True}
                    }
                )

            return web.json_response({'status': 'success',
                                      'message': f'successfully edited user {_id}'}, status=200)

        except Exception as _e:
            return web.json_response({'status': 'error',
                                      'message': f'failed to edit user: {_e}'}, status=500)
    else:
        return web.json_response({'status': 'error', 'message': 'must be admin to edit users'}, status=403)


''' query apis '''


def parse_query(task, save: bool = False):
    # save auxiliary stuff
    kwargs = task.get('kwargs', dict())

    # reduce!
    task_reduced = {'user': task['user'], 'query': dict(), 'kwargs': kwargs}

    prohibited_collections = ('users', 'filters', 'queries')

    if task['query_type'] == 'estimated_document_count':
        # specify task type:
        task_reduced['query_type'] = 'estimated_document_count'

        if task['user'] != config['server']['admin_username']:
            if str(task['query']['catalog']) in prohibited_collections:
                raise Exception('protected collection')

        task_reduced['query']['catalog'] = task['query']['catalog']

    elif task['query_type'] == 'find':
        # specify task type:
        task_reduced['query_type'] = 'find'

        if task['user'] != config['server']['admin_username']:
            if str(task['query']['catalog']) in prohibited_collections:
                raise Exception('protected collection')

        task_reduced['query']['catalog'] = task['query']['catalog']

        # construct filter
        _filter = task['query']['filter']
        if isinstance(_filter, str):
            # passed string? evaluate:
            catalog_filter = literal_eval(_filter.strip())
        elif isinstance(_filter, dict):
            # passed dict?
            catalog_filter = _filter
        else:
            raise ValueError('unsupported filter specification')

        task_reduced['query']['filter'] = catalog_filter

        # construct projection
        if 'projection' in task['query']:
            _projection = task['query']['projection']
            if isinstance(_projection, str):
                # passed string? evaluate:
                catalog_projection = literal_eval(_projection.strip())
            elif isinstance(_filter, dict):
                # passed dict?
                catalog_projection = _projection
            else:
                raise ValueError('Unsupported projection specification')
        else:
            catalog_projection = dict()

        task_reduced['query']['projection'] = catalog_projection

    elif task['query_type'] == 'find_one':
        # specify task type:
        task_reduced['query_type'] = 'find_one'

        if task['user'] != config['server']['admin_username']:
            if str(task['query']['catalog']) in prohibited_collections:
                raise Exception('protected collection')

        task_reduced['query']['catalog'] = task['query']['catalog']

        # construct filter
        _filter = task['query']['filter']
        if isinstance(_filter, str):
            # passed string? evaluate:
            catalog_filter = literal_eval(_filter.strip())
        elif isinstance(_filter, dict):
            # passed dict?
            catalog_filter = _filter
        else:
            raise ValueError('Unsupported filter specification')

        task_reduced['query']['filter'] = catalog_filter

    elif task['query_type'] == 'count_documents':
        # specify task type:
        task_reduced['query_type'] = 'count_documents'

        if task['user'] != config['server']['admin_username']:
            if str(task['query']['catalog']) in prohibited_collections:
                raise Exception('protected collection')

        task_reduced['query']['catalog'] = task['query']['catalog']

        # construct filter
        _filter = task['query']['filter']
        if isinstance(_filter, str):
            # passed string? evaluate:
            catalog_filter = literal_eval(_filter.strip())
        elif isinstance(_filter, dict):
            # passed dict?
            catalog_filter = _filter
        else:
            raise ValueError('Unsupported filter specification')

        task_reduced['query']['filter'] = catalog_filter

    elif task['query_type'] == 'aggregate':
        # specify task type:
        task_reduced['query_type'] = 'aggregate'

        if task['user'] != config['server']['admin_username']:
            if str(task['query']['catalog']) in prohibited_collections:
                raise Exception('protected collection')

        task_reduced['query']['catalog'] = task['query']['catalog']

        # construct pipeline
        _pipeline = task['query']['pipeline']
        if isinstance(_pipeline, str):
            # passed string? evaluate:
            catalog_pipeline = literal_eval(_pipeline.strip())
        elif isinstance(_pipeline, list) or isinstance(_pipeline, tuple):
            # passed dict?
            catalog_pipeline = _pipeline
        else:
            raise ValueError('Unsupported pipeline specification')

        task_reduced['query']['pipeline'] = catalog_pipeline

    elif task['query_type'] == 'cone_search':
        # specify task type:
        task_reduced['query_type'] = 'cone_search'
        # cone search radius:
        cone_search_radius = float(task['object_coordinates']['cone_search_radius'])
        # convert to rad:
        if task['object_coordinates']['cone_search_unit'] == 'arcsec':
            cone_search_radius *= np.pi / 180.0 / 3600.
        elif task['object_coordinates']['cone_search_unit'] == 'arcmin':
            cone_search_radius *= np.pi / 180.0 / 60.
        elif task['object_coordinates']['cone_search_unit'] == 'deg':
            cone_search_radius *= np.pi / 180.0
        elif task['object_coordinates']['cone_search_unit'] == 'rad':
            cone_search_radius *= 1
        else:
            raise Exception('unknown cone search unit: must be in [arcsec, arcmin, deg, rad]')

        if isinstance(task['object_coordinates']['radec'], str):
            radec = task['object_coordinates']['radec'].strip()

            # comb radecs for a single source as per Tom's request:
            if radec[0] not in ('[', '(', '{'):
                ra, dec = radec.split()
                if ('s' in radec) or (':' in radec):
                    radec = f"[('{ra}', '{dec}')]"
                else:
                    radec = f"[({ra}, {dec})]"

            # print(task['object_coordinates']['radec'])
            objects = literal_eval(radec)
            # print(type(objects), isinstance(objects, dict), isinstance(objects, list))
        elif isinstance(task['object_coordinates']['radec'], list) or \
                isinstance(task['object_coordinates']['radec'], tuple) or \
                isinstance(task['object_coordinates']['radec'], dict):
            objects = task['object_coordinates']['radec']
        else:
            raise Exception('bad source coordinates')

        # this could either be list/tuple [(ra1, dec1), (ra2, dec2), ..] or dict {'name': (ra1, dec1), ...}
        if isinstance(objects, list) or isinstance(objects, tuple):
            object_coordinates = objects
            object_names = [str(obj_crd).replace('.', '_') for obj_crd in object_coordinates]
        elif isinstance(objects, dict):
            object_names, object_coordinates = zip(*objects.items())
            object_names = list(map(str, object_names))
            object_names = [on.replace('.', '_') for on in object_names]
        else:
            raise ValueError('Unsupported object coordinates specs')

        # print(object_names, object_coordinates)

        for catalog in task['catalogs']:

            if task['user'] != config['server']['admin_username']:
                if str(catalog) in prohibited_collections:
                    raise Exception('protected collection')

            task_reduced['query'][catalog] = dict()
            # parse catalog query:
            # construct filter
            _filter = task['catalogs'][catalog]['filter']
            if isinstance(_filter, str):
                # passed string? evaluate:
                catalog_query = literal_eval(_filter.strip())
            elif isinstance(_filter, dict):
                # passed dict?
                catalog_query = _filter
            else:
                raise ValueError('unsupported filter specification')

            # construct projection
            _projection = task['catalogs'][catalog]['projection']
            if isinstance(_projection, str):
                # passed string? evaluate:
                catalog_projection = literal_eval(_projection.strip())
            elif isinstance(_filter, dict):
                # passed dict?
                catalog_projection = _projection
            else:
                raise ValueError('unsupported projection specification')

            # parse coordinate list

            if isinstance(_projection, str):
                # passed string? evaluate:
                catalog_projection = literal_eval(_projection.strip())
            elif isinstance(_filter, dict):
                # passed dict?
                catalog_projection = _projection

            for oi, obj_crd in enumerate(object_coordinates):
                # convert ra/dec into GeoJSON-friendly format
                _ra, _dec = radec_str2geojson(*obj_crd)
                object_position_query = dict()
                object_position_query['coordinates.radec_geojson'] = {
                    '$geoWithin': {'$centerSphere': [[_ra, _dec], cone_search_radius]}}
                # use stringified object coordinates as dict keys and merge dicts with cat/obj queries:
                task_reduced['query'][catalog][object_names[oi]] = ({**object_position_query, **catalog_query},
                                                                    {**catalog_projection})

    elif task['query_type'] == 'info':

        # specify task type:
        task_reduced['query_type'] = 'info'
        task_reduced['query'] = task['query']

    if save:
        task_hashable = dumps(task_reduced)
        # compute hash for task. this is used as key in DB
        task_hash = compute_hash(task_hashable)

        # mark as enqueued in DB:
        t_stamp = datetime.datetime.utcnow()
        if 'query_expiration_interval' not in kwargs:
            # default expiration interval:
            t_expires = t_stamp + datetime.timedelta(days=int(config['misc']['query_expiration_interval']))
        else:
            # custom expiration interval:
            t_expires = t_stamp + datetime.timedelta(days=int(kwargs['query_expiration_interval']))

        # dump task_hashable to file, as potentially too big to store in mongo
        # save task:
        user_tmp_path = os.path.join(config['path']['path_queries'], task['user'])
        # mkdir if necessary
        if not os.path.exists(user_tmp_path):
            os.makedirs(user_tmp_path)
        task_file = os.path.join(user_tmp_path, f'{task_hash}.task.json')

        with open(task_file, 'w') as f_task_file:
            f_task_file.write(dumps(task))

        task_doc = {'task_id': task_hash,
                    'user': task['user'],
                    'task': task_file,
                    'result': None,
                    'status': 'enqueued',
                    'created': t_stamp,
                    'expires': t_expires,
                    'last_modified': t_stamp}

        return task_hash, task_reduced, task_doc

    else:
        return '', task_reduced, {}


async def execute_query(mongo, task_hash, task_reduced, task_doc, save: bool = False):

    db = mongo

    if save:
        # mark query as enqueued:
        await db.queries.insert_one(task_doc)

    result = dict()
    query_result = None

    _query = task_reduced

    result['user'] = _query.get('user')
    result['kwargs'] = _query.get('kwargs', dict())

    # by default, long-running queries will be killed after config['misc']['max_time_ms'] ms
    max_time_ms = int(result['kwargs'].get('max_time_ms', config['misc']['max_time_ms']))
    assert max_time_ms >= 1, 'bad max_time_ms, must be int>=1'

    try:

        # cone search:
        if _query['query_type'] == 'cone_search':

            known_kwargs = ('skip', 'hint', 'limit', 'sort')
            kwargs = {kk: vv for kk, vv in _query['kwargs'].items() if kk in known_kwargs}
            kwargs['comment'] = str(_query['user'])

            # iterate over catalogs as they represent
            query_result = dict()
            for catalog in _query['query']:
                query_result[catalog] = dict()
                # iterate over objects:
                for obj in _query['query'][catalog]:
                    # project?
                    if len(_query['query'][catalog][obj][1]) > 0:
                        _select = db[catalog].find(_query['query'][catalog][obj][0],
                                                   _query['query'][catalog][obj][1],
                                                   max_time_ms=max_time_ms, **kwargs)
                    # return the whole documents by default
                    else:
                        _select = db[catalog].find(_query['query'][catalog][obj][0],
                                                   max_time_ms=max_time_ms, **kwargs)
                    # mongodb does not allow having dots in field names -> replace with underscores
                    query_result[catalog][obj.replace('.', '_')] = await _select.to_list(length=None)

        # convenience general search subtypes:
        elif _query['query_type'] == 'find':
            # print(query)

            known_kwargs = ('skip', 'hint', 'limit', 'sort')
            kwargs = {kk: vv for kk, vv in _query['kwargs'].items() if kk in known_kwargs}
            kwargs['comment'] = str(_query['user'])

            # project?
            if len(_query['query']['projection']) > 0:

                _select = db[_query['query']['catalog']].find(_query['query']['filter'],
                                                              _query['query']['projection'],
                                                              max_time_ms=max_time_ms, **kwargs)
            # return the whole documents by default
            else:
                _select = db[_query['query']['catalog']].find(_query['query']['filter'],
                                                              max_time_ms=max_time_ms, **kwargs)

            # todo: replace with inspect.iscoroutinefunction(object)?
            if isinstance(_select, int) or isinstance(_select, float) or isinstance(_select, tuple) or \
                    isinstance(_select, list) or isinstance(_select, dict) or (_select is None):
                query_result = _select
            else:
                query_result = await _select.to_list(length=None)

        elif _query['query_type'] == 'find_one':
            # print(query)

            known_kwargs = ('skip', 'hint', 'limit', 'sort')
            kwargs = {kk: vv for kk, vv in _query['kwargs'].items() if kk in known_kwargs}
            kwargs['comment'] = str(_query['user'])

            _select = db[_query['query']['catalog']].find_one(_query['query']['filter'],
                                                              max_time_ms=max_time_ms)

            query_result = await _select

        elif _query['query_type'] == 'count_documents':

            known_kwargs = ('skip', 'hint', 'limit')
            kwargs = {kk: vv for kk, vv in _query['kwargs'].items() if kk in known_kwargs}
            kwargs['comment'] = str(_query['user'])

            _select = db[_query['query']['catalog']].count_documents(_query['query']['filter'],
                                                                     maxTimeMS=max_time_ms)

            query_result = await _select

        elif _query['query_type'] == 'estimated_document_count':

            known_kwargs = ('maxTimeMS', )
            kwargs = {kk: vv for kk, vv in _query['kwargs'].items() if kk in known_kwargs}
            kwargs['comment'] = str(_query['user'])

            _select = db[_query['query']['catalog']].estimated_document_count(_query['query']['filter'],
                                                                              maxTimeMS=max_time_ms)

            query_result = await _select

        elif _query['query_type'] == 'aggregate':

            known_kwargs = ('allowDiskUse', 'maxTimeMS', 'batchSize')
            kwargs = {kk: vv for kk, vv in _query['kwargs'].items() if kk in known_kwargs}
            kwargs['comment'] = str(_query['user'])

            _select = db[_query['query']['catalog']].aggregate(_query['query']['pipeline'],
                                                               allowDiskUse=True,
                                                               maxTimeMS=max_time_ms)

            query_result = await _select.to_list(length=None)

        elif _query['query_type'] == 'info':
            # collection/catalog info

            if _query['query']['command'] == 'catalog_names':

                # get available catalog names
                catalogs = await db.list_collection_names()
                # exclude system collections
                catalogs_system = (config['database']['collection_users'],
                                   config['database']['collection_queries'])

                query_result = [c for c in sorted(catalogs)[::-1] if c not in catalogs_system]

            elif _query['query']['command'] == 'catalog_info':

                catalog = _query['query']['catalog']

                stats = await db.command('collstats', catalog)

                query_result = stats

            elif _query['query']['command'] == 'index_info':

                catalog = _query['query']['catalog']

                stats = await db[catalog].index_information()

                query_result = stats

            elif _query['query']['command'] == 'db_info':

                stats = await db.command('dbstats')
                query_result = stats

        # success!
        result['status'] = 'success'
        result['message'] = 'query successfully executed'

        if not save:
            # dump result back
            result['data'] = query_result

        else:
            # save task result:
            user_tmp_path = os.path.join(config['path']['path_queries'], _query['user'])
            # print(user_tmp_path)
            # mkdir if necessary
            if not os.path.exists(user_tmp_path):
                os.makedirs(user_tmp_path)
            task_result_file = os.path.join(user_tmp_path, f'{task_hash}.result.json')

            # save location in db:
            result['result'] = task_result_file

            async with aiofiles.open(task_result_file, 'w') as f_task_result_file:
                task_result = dumps(query_result)
                await f_task_result_file.write(task_result)

        # print(task_hash, result)

        # db book-keeping:
        if save:
            # mark query as done:
            await db.queries.update_one({'user': _query['user'], 'task_id': task_hash},
                                        {'$set': {'status': result['status'],
                                                  'last_modified': datetime.datetime.utcnow(),
                                                  'result': result['result']}}
                                        )

        # return task_hash, dumps(result)
        return task_hash, result

    except Exception as e:
        print(f'{datetime.datetime.now()} got error: {str(e)}')
        _err = traceback.format_exc()
        print(_err)

        # book-keeping:
        if save:
            # save task result with error message:
            user_tmp_path = os.path.join(config['path']['path_queries'], _query['user'])
            # print(user_tmp_path)
            # mkdir if necessary
            if not os.path.exists(user_tmp_path):
                os.makedirs(user_tmp_path)
            task_result_file = os.path.join(user_tmp_path, f'{task_hash}.result.json')

            # save location in db:
            # result['user'] = query['user']
            result['status'] = 'error'
            result['message'] = _err

            async with aiofiles.open(task_result_file, 'w') as f_task_result_file:
                task_result = dumps(result)
                await f_task_result_file.write(task_result)

            # mark query as failed:
            await db.queries.update_one({'user': _query['user'], 'task_id': task_hash},
                                        {'$set': {'status': result['status'],
                                                  'last_modified': datetime.datetime.utcnow(),
                                                  'result': None}}
                                        )

        else:
            result['status'] = 'error'
            result['message'] = _err

            return task_hash, result

        raise Exception('query failed badly')


@routes.post('/api/queries')
@auth_required
async def query(request):
    """
        Query Kowalski

    :return:
    """
    try:
        try:
            _query = await request.json()
        except Exception as _e:
            print(f'{datetime.datetime.utcnow()} Cannot extract json() from request, trying post(): {str(_e)}')
            _query = await request.post()

        # parse query
        known_query_types = ('cone_search', 'count_documents', 'estimated_document_count',
                             'find', 'find_one', 'aggregate', 'info')

        assert _query['query_type'] in known_query_types, \
            f'query_type {_query["query_type"]} not in {str(known_query_types)}'

        _query['user'] = request.user

        # by default, [unless enqueue_only is requested]
        # all queries are not registered in the db and the task/results are not stored on disk as json files
        # giving a significant execution speed up. this behaviour can be overridden.
        save = _query.get('kwargs', dict()).get('save', False)

        task_hash, task_reduced, task_doc = parse_query(_query, save=save)

        # execute query:
        if not save:
            task_hash, result = await execute_query(request.app['mongo'], task_hash, task_reduced, task_doc, save)

            return web.json_response(result, status=200, dumps=dumps)
        else:
            # only schedule query execution. store query and results, return query id to user
            asyncio.create_task(execute_query(request.app['mongo'], task_hash, task_reduced, task_doc, save))
            return web.json_response({'status': 'success', 'query_id': task_hash, 'message': 'query enqueued'},
                                     status=200, dumps=dumps)

    except Exception as _e:
        print(f'{datetime.datetime.utcnow()} Got error: {str(_e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'status': 'error', 'message': f'failure: {_err}'}, status=500)


@routes.get('/api/queries/{task_id}')
@auth_required
async def query_grab(request):
    """
        Grab query / result.

    :return:
    """

    # get user:
    user = request.user

    # get query params
    task_id = request.match_info['task_id']
    _data = request.query

    try:
        part = _data.get('part', 'result')

        _query = await request.app['mongo'].queries.find_one({'user': user,
                                                              'task_id': {'$eq': task_id}}, {'status': 1})

        if part == 'task':
            task_file = os.path.join(config['path']['path_queries'], user, f'{task_id}.task.json')
            async with aiofiles.open(task_file, 'r') as f_task_file:
                return web.json_response(await f_task_file.read(), status=200)

        elif part == 'result':
            if query['status'] == 'enqueued':
                return web.json_response({'status': 'success', 'message': f'query not finished yet'}, status=200)

            task_result_file = os.path.join(config['path']['path_queries'], user, f'{task_id}.result.json')

            async with aiofiles.open(task_result_file, 'r') as f_task_result_file:
                return web.json_response(await f_task_result_file.read(), status=200)

        else:
            return web.json_response({'status': 'error', 'message': 'part not recognized'}, status=500)

    except Exception as _e:
        print(f'{datetime.datetime.utcnow()} Got error: {str(_e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'status': 'error', 'message': f'failure: {_err}'}, status=500)


@routes.delete('/api/queries/{task_id}')
@auth_required
async def query_delete(request):
    """
        Delete Query from DB programmatically.

    :return:
    """

    # get user:
    user = request.user

    # get query params
    task_id = request.match_info['task_id']

    try:
        if task_id != 'all':
            await request.app['mongo'].queries.delete_one({'user': user, 'task_id': {'$eq': task_id}})

            # remove files containing task and result
            for p in pathlib.Path(os.path.join(config['path']['path_queries'], user)).glob(f'{task_id}*'):
                p.unlink()

        else:
            await request.app['mongo'].queries.delete_many({'user': user})

            # remove all files containing task and result
            if os.path.exists(os.path.join(config['path']['path_queries'], user)):
                shutil.rmtree(os.path.join(config['path']['path_queries'], user))

        return web.json_response({'status': 'success', 'message': f'removed query: {task_id}'}, status=200)

    except Exception as _e:
        print(f'{datetime.datetime.utcnow()} Got error: {str(_e)}')
        _err = traceback.format_exc()
        print(_err)
        return web.json_response({'status': 'error', 'message': f'failure: {_err}'}, status=500)


''' filters apis '''


@routes.get('/api/filters/{filter_id}')
@auth_required
async def filter_get(request):
    """
        todo: Retrieve user-defined filter by id
    :param request:
    :return:
    """
    pass


@routes.post('/api/filters')
@auth_required
async def filter_post(request):
    """
        todo: Save user user-defined filter assigning unique id
        store as serialized extended json string, use literal_eval to convert to dict at execution
        run a simple sanity check before saving
        https://www.npmjs.com/package/bson
    :param request:
    :return:
    """
    pass


@routes.delete('/api/filters/{filter_id}')
@auth_required
async def filter_delete(request):
    """
        todo?: Delete user-defined filter by id
    :param request:
    :return:
    """
    pass


''' lab apis '''


@routes.get('/lab/ztf-alerts/{candid}/cutout/{cutout}/{file_format}')
@auth_required
async def ztf_alert_get_cutout_handler(request):
    """
        Serve cutouts as fits or png
    :param request:
    :return:
    """
    candid = int(request.match_info['candid'])
    cutout = request.match_info['cutout'].capitalize()
    file_format = request.match_info['file_format']

    assert cutout in ['Science', 'Template', 'Difference']
    assert file_format in ['fits', 'png']

    alert = await request.app['mongo']['ZTF_alerts'].find_one({'candid': candid},
                                                              {f'cutout{cutout}': 1},
                                                              max_time_ms=60000)

    cutout_data = loads(dumps([alert[f'cutout{cutout}']['stampData']]))[0]

    # unzipped fits name
    fits_name = pathlib.Path(alert[f"cutout{cutout}"]["fileName"]).with_suffix('')

    # unzip and flip about y axis on the server side
    with gzip.open(io.BytesIO(cutout_data), 'rb') as f:
        with fits.open(io.BytesIO(f.read())) as hdu:
            header = hdu[0].header
            data_flipped_y = np.flipud(hdu[0].data)

    if file_format == 'fits':
        hdu = fits.PrimaryHDU(data_flipped_y, header=header)
        # hdu = fits.PrimaryHDU(data_flipped_y)
        hdul = fits.HDUList([hdu])

        stamp_fits = io.BytesIO()
        hdul.writeto(fileobj=stamp_fits)

        return web.Response(body=stamp_fits.getvalue(), content_type='image/fits',
                            headers=MultiDict({'Content-Disposition': f'Attachment;filename={fits_name}'}), )

    if file_format == 'png':
        buff = io.BytesIO()
        plt.close('all')
        fig = plt.figure()
        fig.set_size_inches(4, 4, forward=False)
        ax = plt.Axes(fig, [0., 0., 1., 1.])
        ax.set_axis_off()
        fig.add_axes(ax)

        # remove nans:
        img = np.array(data_flipped_y)
        img = np.nan_to_num(img)

        if cutout != 'Difference':
            # img += np.min(img)
            img[img <= 0] = np.median(img)
            # plt.imshow(img, cmap='gray', norm=LogNorm(), origin='lower')
            plt.imshow(img, cmap=plt.cm.bone, norm=LogNorm(), origin='lower')
        else:
            # plt.imshow(img, cmap='gray', origin='lower')
            plt.imshow(img, cmap=plt.cm.bone, origin='lower')
        plt.savefig(buff, dpi=42)

        buff.seek(0)
        plt.close('all')
        return web.Response(body=buff, content_type='image/png')


@routes.get('/lab/zuds-alerts/{candid}/cutout/{cutout}/{file_format}')
@auth_required
async def zuds_alert_get_cutout_handler(request):
    """
        Serve cutouts as fits or png
    :param request:
    :return:
    """

    candid = int(request.match_info['candid'])
    cutout = request.match_info['cutout'].capitalize()
    file_format = request.match_info['file_format']

    assert cutout in ['Science', 'Template', 'Difference']
    assert file_format in ['fits', 'png']

    alert = await request.app['mongo']['ZUDS_alerts'].find_one({'candid': candid},
                                                               {f'cutout{cutout}': 1},
                                                               max_time_ms=60000)

    cutout_data = loads(dumps([alert[f'cutout{cutout}']]))[0]

    # unzipped fits name
    fits_name = f"{candid}.cutout{cutout}.fits"

    # unzip and flip about y axis on the server side
    with gzip.open(io.BytesIO(cutout_data), 'rb') as f:
        with fits.open(io.BytesIO(f.read())) as hdu:
            header = hdu[0].header
            # no need to flip it since Danny does that on his end
            # data_flipped_y = np.flipud(hdu[0].data)
            data_flipped_y = hdu[0].data

    if file_format == 'fits':
        hdu = fits.PrimaryHDU(data_flipped_y, header=header)
        # hdu = fits.PrimaryHDU(data_flipped_y)
        hdul = fits.HDUList([hdu])

        stamp_fits = io.BytesIO()
        hdul.writeto(fileobj=stamp_fits)

        return web.Response(body=stamp_fits.getvalue(), content_type='image/fits',
                            headers=MultiDict({'Content-Disposition': f'Attachment;filename={fits_name}'}), )

    if file_format == 'png':
        buff = io.BytesIO()
        plt.close('all')
        fig = plt.figure()
        fig.set_size_inches(4, 4, forward=False)
        ax = plt.Axes(fig, [0., 0., 1., 1.])
        ax.set_axis_off()
        fig.add_axes(ax)

        # remove nans:
        img = np.array(data_flipped_y)
        img = np.nan_to_num(img)

        if cutout != 'Difference':
            # img += np.min(img)
            img[img <= 0] = np.median(img)
            # plt.imshow(img, cmap='gray', norm=LogNorm(), origin='lower')
            plt.imshow(img, cmap=plt.cm.bone, norm=LogNorm(), origin='lower')
        else:
            # plt.imshow(img, cmap='gray', origin='lower')
            plt.imshow(img, cmap=plt.cm.bone, origin='lower')
        plt.savefig(buff, dpi=42)

        buff.seek(0)
        plt.close('all')
        return web.Response(body=buff, content_type='image/png')


async def app_factory():
    """
        App Factory
    :return:
    """

    # init db if necessary
    await init_db(config=config)

    # Database connection
    client = AsyncIOMotorClient(f"mongodb://{config['database']['user']}:{config['database']['pwd']}@" +
                                f"{config['database']['host']}:{config['database']['port']}/{config['database']['db']}",
                                maxPoolSize=config['database']['max_pool_size'])
    mongo = client[config['database']['db']]

    # add site admin if necessary
    await add_admin(mongo, config=config)

    # init app with auth middleware
    app = web.Application(middlewares=[auth_middleware])

    # store mongo connection
    app['mongo'] = mongo

    # mark all enqueued tasks failed on startup
    await app['mongo'].queries.update_many({'status': 'enqueued'},
                                           {'$set': {'status': 'error', 'last_modified': datetime.datetime.utcnow()}})

    # graciously close mongo client on shutdown
    async def close_mongo(_app):
        _app['mongo'].client.close()

    app.on_cleanup.append(close_mongo)

    # set up JWT for user authentication/authorization
    app['JWT'] = {'JWT_SECRET': config['server']['JWT_SECRET_KEY'],
                  'JWT_ALGORITHM': 'HS256',
                  'JWT_EXP_DELTA_SECONDS': 30 * 86400 * 3}

    # route table
    app.add_routes(routes)

    return app


''' Tests '''


class TestAPIs(object):
    # python -m pytest -s api.py
    # python -m pytest api.py

    async def test_auth(self, aiohttp_client):
        """
            Test authorization: /api/auth
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        _auth = await client.post(f'/api/auth',
                                  json={"username": config['server']['admin_username'],
                                        "password": config['server']['admin_password']})
        assert _auth.status == 200

        credentials = await _auth.json()
        assert 'token' in credentials

    async def test_auth_error(self, aiohttp_client):
        """
            Test authorization with invalid credentials: /api/auth
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        _auth = await client.post(f'/api/auth', json={"username": "noname", "password": "nopass"})
        assert _auth.status == 401

        credentials = await _auth.json()
        # print(credentials)
        assert credentials['status'] == 'error'
        assert credentials['message'] == 'wrong credentials'

    async def test_users(self, aiohttp_client):
        """
            Test user management: /api/users
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        # check JWT authorization
        _auth = await client.post(f'/api/auth',
                                  json={"username": config['server']['admin_username'],
                                        "password": config['server']['admin_password']})
        assert _auth.status == 200
        # print(await auth.text())
        # print(await auth.json())
        credentials = await _auth.json()
        assert 'token' in credentials

        access_token = credentials['token']

        headers = {'Authorization': access_token}

        # adding a user
        resp = await client.put('/api/users', json={'user': 'test_user', 'password': uid(6)}, headers=headers)
        assert resp.status == 200
        # text = await resp.text()
        # text = await resp.json()

        # editing user credentials
        resp = await client.post('/api/users', json={'_user': 'test_user',
                                                     'edit-user': 'test_user',
                                                     'edit-password': uid(6)}, headers=headers)
        assert resp.status == 200
        resp = await client.post('/api/users', json={'_user': 'test_user',
                                                     'edit-user': 'test_user_edited',
                                                     'edit-password': ''}, headers=headers)
        assert resp.status == 200

        # deleting a user
        resp = await client.delete('/api/users', json={'user': 'test_user_edited'}, headers=headers)
        assert resp.status == 200

    async def test_query_save(self, aiohttp_client):
        """
            Test query with db registering and saving results to disk: /api/queries
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        # authorize
        _auth = await client.post(f'/api/auth',
                                  json={"username": config['server']['admin_username'],
                                        "password": config['server']['admin_password']})
        assert _auth.status == 200
        credentials = await _auth.json()
        assert credentials['status'] == 'success'
        assert 'token' in credentials

        access_token = credentials['token']

        headers = {'Authorization': access_token}

        collection = 'ZTF_alerts'

        # test query with book-keeping
        qu = {"query_type": "find_one",
              "query": {
                  "catalog": collection,
                  "filter": {},
              },
              "kwargs": {"save": True, "_id": uid(32)}
              }
        # print(qu)
        resp = await client.post('/api/queries', json=qu, headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        # print(result)
        assert result['status'] == 'success'

        # todo: test getting 'task' and 'result'

        # remove enqueued query
        resp = await client.delete(f'/api/queries/{result["query_id"]}', headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        assert result['status'] == 'success'

    # todo: test multiple query types without book-keeping (the default and almost exclusively used scenario):
    #  - find_one
    #  - find
    #  - info
    #  - count_documents
    #  - estimated_document_count
    #  - aggregate

    async def test_query_find_one(self, aiohttp_client):
        """
            Test {"query_type": "find_one", ...}: /api/queries
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        # authorize
        _auth = await client.post(f'/api/auth',
                                  json={"username": config['server']['admin_username'],
                                        "password": config['server']['admin_password']})
        assert _auth.status == 200
        # print(await auth.text())
        # print(await auth.json())
        credentials = await _auth.json()
        assert credentials['status'] == 'success'
        assert 'token' in credentials

        access_token = credentials['token']

        headers = {'Authorization': access_token}

        collection = 'ZTF_alerts'

        # check query without book-keeping
        qu = {"query_type": "find_one",
              "query": {
                  "catalog": collection,
                  "filter": {},
              },
              "kwargs": {"save": False}
              }
        # print(qu)
        resp = await client.post('/api/queries', json=qu, headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        # print(result)
        assert result['status'] == 'success'
        assert result['message'] == 'query successfully executed'
        assert 'data' in result

    async def test_query_info(self, aiohttp_client):
        """
            Test {"query_type": "info", ...}: /api/queries
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        # authorize
        _auth = await client.post(f'/api/auth',
                                  json={"username": config['server']['admin_username'],
                                        "password": config['server']['admin_password']})
        assert _auth.status == 200
        # print(await auth.text())
        # print(await auth.json())
        credentials = await _auth.json()
        assert credentials['status'] == 'success'
        assert 'token' in credentials

        access_token = credentials['token']

        headers = {'Authorization': access_token}

        collection = 'ZTF_alerts'

        # check catalog_names info
        qu = {"query_type": "info", "query": {"command": "catalog_names"}}
        # print(qu)
        resp = await client.post('/api/queries', json=qu, headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        # print(result)
        assert result['status'] == 'success'
        assert result['message'] == 'query successfully executed'
        assert 'data' in result

    # todo: test raising errors

    async def test_query_unauthorized(self, aiohttp_client):
        """
            Test an unauthorized query: /api/queries
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        headers = {'Authorization': 'no_token'}

        # check catalog_names info
        qu = {"query_type": "info", "query": {"command": "catalog_names"}}
        # print(qu)
        resp = await client.post('/api/queries', json=qu, headers=headers, timeout=5)
        # print(resp)
        assert resp.status == 400
        result = await resp.json()
        # print(result)
        assert result['status'] == 'error'
        assert result['message'] == 'token is invalid'


uvloop.install()


if __name__ == '__main__':

    web.run_app(app_factory(), port=config['server']['port'])
