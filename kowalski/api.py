from aiohttp import web
from ast import literal_eval
from bson.json_util import dumps
import datetime
import jwt
from middlewares import auth_middleware, auth_required
import numpy as np
import os
from utils import check_password_hash, compute_hash, generate_password_hash, load_config, radec_str2geojson


config = load_config()

routes = web.RouteTableDef()


@routes.post('/api/auth')
async def auth(request):
    """
        Authenticate
        todo: swagger!
    """
    try:
        post_data = await request.json()
    except Exception as _e:
        # print(f'Cannot extract json() from request, trying post(): {str(_e)}')
        post_data = await request.post()

    # must contain 'username' and 'password'
    if ('username' not in post_data) or (len(post_data['username']) == 0):
        return web.json_response({'status': 'error', 'message': 'Missing "username"'}, status=400)
    if ('password' not in post_data) or (len(post_data['password']) == 0):
        return web.json_response({'status': 'error', 'message': 'Missing "password"'}, status=400)

    # connecting from penquins: check penquins version
    if 'penquins.__version__' in post_data:
        penquins_version = post_data['penquins.__version__']
        if penquins_version not in config['misc']['supported_penquins_versions']:
            return web.json_response({'status': 'error', 'message': 'Unsupported version of penquins.'}, status=400)

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
            return web.json_response({'status': 'error', 'message': 'Wrong credentials'}, status=401)

    except Exception as e:
        return web.json_response({'status': 'error', 'message': 'Wrong credentials'}, status=401)


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


''' queries api '''


def parse_query(task, save: bool = False):
    # save auxiliary stuff
    kwargs = task.get('kwargs', dict())

    # reduce!
    task_reduced = {'user': task['user'], 'query': dict(), 'kwargs': kwargs}

    prohibited_collections = ('users', 'stats', 'queries')

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

        go_on = True

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

