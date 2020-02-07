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


@routes.delete('/users')
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


@routes.post('/users')
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


