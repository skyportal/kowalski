from aiohttp import web
from ast import literal_eval
import datetime
import jwt
from middlewares import auth_middleware, auth_required
from utils import check_password_hash, generate_password_hash, load_config


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
            username = _data.get('user', None)
            password = _data.get('password', None)
            permissions = _data.get('permissions', '{}')

            if len(username) == 0 or len(password) == 0:
                return web.json_response({'status': 'error',
                                          'message': 'username and password must be set'}, status=500)

            if len(permissions) == 0:
                permissions = '{}'

            # add user to coll_usr collection:
            await request.app['mongo'].users.insert_one(
                {'_id': username,
                 'password': generate_password_hash(password),
                 'permissions': literal_eval(str(permissions)),
                 'last_modified': datetime.datetime.now()}
            )

            return web.json_response({'message': 'success'}, status=200)

        except Exception as _e:
            print(f'Got error: {str(_e)}')
            _err = traceback.format_exc()
            print(_err)
            return web.json_response({'message': f'Failed to add user: {_err}'}, status=500)
    else:
        return web.json_response({'message': '403 Forbidden'}, status=403)


@routes.delete('/users')
@login_required
async def remove_user(request):
    """
        Remove user from DB
    :return:
    """
    # get session:
    session = await get_session(request)

    _data = await request.json()
    # print(_data)

    if session['user_id'] == config['server']['admin_username']:
        try:
            # get username from request
            username = _data['user'] if 'user' in _data else None
            if username == config['server']['admin_username']:
                return web.json_response({'message': 'Cannot remove the superuser!'}, status=500)

            # try to remove the user:
            await request.app['mongo'].users.delete_one({'_id': username})

            return web.json_response({'message': 'success'}, status=200)

        except Exception as _e:
            print(f'Got error: {str(_e)}')
            _err = traceback.format_exc()
            print(_err)
            return web.json_response({'message': f'Failed to remove user: {_err}'}, status=500)
    else:
        return web.json_response({'message': '403 Forbidden'}, status=403)


@routes.post('/users')
@login_required
async def edit_user(request):
    """
        Edit user info
    :return:
    """
    # get session:
    session = await get_session(request)

    _data = await request.json()
    # print(_data)

    if session['user_id'] == config['server']['admin_username']:
        try:
            _id = _data['_user'] if '_user' in _data else None
            username = _data['edit-user'] if 'edit-user' in _data else None
            password = _data['edit-password'] if 'edit-password' in _data else None
            # permissions = _data['edit-permissions'] if 'edit-permissions' in _data else '{}'

            if _id == config['server']['admin_username'] and username != config['server']['admin_username']:
                return web.json_response({'message': 'Cannot change the admin username!'}, status=500)

            if len(username) == 0:
                return web.json_response({'message': 'username must be set'}, status=500)

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

            return web.json_response({'message': 'success'}, status=200)

        except Exception as _e:
            print(f'Got error: {str(_e)}')
            _err = traceback.format_exc()
            print(_err)
            return web.json_response({'message': f'Failed to remove user: {_err}'}, status=500)
    else:
        return web.json_response({'message': '403 Forbidden'}, status=403)