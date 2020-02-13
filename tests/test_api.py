# import sys
# sys.path.append('../kowalski')
from api import app_factory
from utils import load_config, uid


config = load_config(config_file='config_api.json')


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

    async def test_query_find(self, aiohttp_client):
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
        credentials = await _auth.json()
        assert credentials['status'] == 'success'
        assert 'token' in credentials

        access_token = credentials['token']

        headers = {'Authorization': access_token}

        collection = 'ZTF_alerts'

        # check query without book-keeping
        qu = {"query_type": "find",
              "query": {
                  "catalog": collection,
                  "filter": {'candid': {"$lt": 0}},
                  "projection": {"_id": 0, "candid": 1},
              },
              "kwargs": {"save": False, "limit": 1}
              }
        # print(qu)
        resp = await client.post('/api/queries', json=qu, headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        # print(result)
        assert result['status'] == 'success'
        assert result['message'] == 'query successfully executed'
        assert 'data' in result
        # should always return a list, even if it's empty
        assert isinstance(result['data'], list)

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

    async def test_query_count_documents(self, aiohttp_client):
        """
            Test {"query_type": "count_documents", ...}: /api/queries
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

        # check catalog_names info
        qu = {"query_type": "count_documents",
              "query": {"catalog": collection,
                        "filter": {"candid": {"$lt": 0}}
                        }
              }
        # print(qu)
        resp = await client.post('/api/queries', json=qu, headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        # print(result)
        assert result['status'] == 'success'
        assert result['message'] == 'query successfully executed'
        assert 'data' in result
        assert result['data'] == 0

    async def test_query_estimated_document_count(self, aiohttp_client):
        """
            Test {"query_type": "estimated_document_count", ...}: /api/queries
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

        # check catalog_names info
        qu = {"query_type": "estimated_document_count",
              "query": {"catalog": collection, "filter": {"candid": {"$lt": 0}}}}
        # print(qu)
        resp = await client.post('/api/queries', json=qu, headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        # print(result)
        assert result['status'] == 'success'
        assert result['message'] == 'query successfully executed'
        assert 'data' in result
        # assert result['data'] == 0

    # test raising errors

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
