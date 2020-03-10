# import sys
# sys.path.append('../kowalski')
from api import app_factory
import pytest
from utils import load_config, uid


config = load_config(config_file='config_api.json')


class TestAPIs(object):
    # python -m pytest -s api.py
    # python -m pytest api.py

    async def auth_admin(self, aiohttp_client):
        """
            Fixture to get authorization token for admin
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

        return credentials

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
        credentials = await self.auth_admin(aiohttp_client)
        access_token = credentials['token']

        headers = {'Authorization': f'Bearer {access_token}'}

        test_user = 'test_user'
        test_user_edited = 'test_user_edited'

        # adding a user
        resp = await client.post('/api/users', json={'username': test_user, 'password': uid(6)}, headers=headers)
        assert resp.status == 200

        # editing user data
        resp = await client.put(f'/api/users/{test_user}', json={'password': uid(6)}, headers=headers)
        assert resp.status == 200
        resp = await client.put(f'/api/users/{test_user}', json={'username': test_user_edited}, headers=headers)
        assert resp.status == 200

        # deleting a user
        resp = await client.delete(f'/api/users/{test_user_edited}', headers=headers)
        assert resp.status == 200

    async def test_query_save(self, aiohttp_client):
        """
            Test query with db registering and saving results to disk: /api/queries
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        # authorize
        credentials = await self.auth_admin(aiohttp_client)
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

    # todo: test filters api

    async def test_filters(self, aiohttp_client):
        """
            Test saving, testing, retrieving, and removing a user-defined filter: /api/filters
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        # authorize as admin, regular users cannot do this
        credentials = await self.auth_admin(aiohttp_client)
        access_token = credentials['token']

        headers = {'Authorization': f'Bearer {access_token}'}

        collection = 'ZTF_alerts'

        user_filter = {
            "group_id": 0,
            "science_program_id": 0,
            "catalog": collection,
            "pipeline": [
                {
                    "$match": {
                        "candidate.drb": {
                            "$gt": 0.9999
                        },
                        "cross_matches.CLU_20190625.0": {
                            "$exists": False
                        }
                    }
                },
                {
                    "$addFields": {
                        "annotations.author": "dd",
                        "annotations.mean_rb": {"$avg": "$prv_candidates.rb"}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "candid": 1,
                        "objectId": 1,
                        "annotations": 1
                    }
                }
            ]
        }

        # test:
        resp = await client.post('/api/filters/test', json=user_filter, headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        # print(result)
        assert result['status'] == 'success'

        # save:
        resp = await client.post('/api/filters', json=user_filter, headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        # print(result)
        assert result['status'] == 'success'
        assert 'data' in result
        assert '_id' in result['data']
        filter_id = result['data']['_id']

        # retrieve
        resp = await client.get(f'/api/filters/{filter_id}', headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        assert result['status'] == 'success'
        assert result['message'] == f'retrieved filter_id {filter_id}'

        # remove filter
        resp = await client.delete(f'/api/filters/{filter_id}', headers=headers, timeout=5)
        assert resp.status == 200
        result = await resp.json()
        assert result['status'] == 'success'
        assert result['message'] == f'removed filter: {filter_id}'

    # test raising errors

    async def test_bad_filter(self, aiohttp_client):
        """
            Test trying to save a bad filter: /api/filters/test
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        credentials = await self.auth_admin(aiohttp_client)
        access_token = credentials['token']

        headers = {'Authorization': f'Bearer {access_token}'}

        collection = 'ZTF_alerts'

        user_filter = {
            "group_id": 0,
            "science_program_id": 0,
            "catalog": collection,
            "pipeline": [
                {
                    "$matc": {  # <-- should be "$match"
                        "candidate.drb": {
                            "$gt": 0.9999
                        },
                        "cross_matches.CLU_20190625.0": {
                            "$exists": False
                        }
                    }
                },
                {
                    "$addFields": {
                        "annotations.author": "dd",
                        "annotations.mean_rb": {"$avg": "$prv_candidates.rb"}
                    }
                },
                {
                    "$project": {
                        "_id": 0,
                        "candid": 1,
                        "objectId": 1,
                        "annotations": 1
                    }
                }
            ]
        }

        # test:
        resp = await client.post('/api/filters/test', json=user_filter, headers=headers, timeout=5)
        assert resp.status == 400
        result = await resp.json()
        # print(result)
        assert result['status'] == 'error'

    # test multiple query types without book-keeping (the default and almost exclusively used scenario):
    #  - find_one
    #  - find
    #  - info
    #  - count_documents
    #  - estimated_document_count
    #  - aggregate
    #  - cone_search

    async def test_query_cone_search(self, aiohttp_client):
        """
            Test {"query_type": "cone_search", ...}: /api/queries
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        # authorize
        credentials = await self.auth_admin(aiohttp_client)
        access_token = credentials['token']

        headers = {'Authorization': f'Bearer {access_token}'}

        collection = 'ZTF_alerts'

        # check query without book-keeping
        qu = {"query_type": "cone_search",
              "query": {
                  "object_coordinates": {
                      "cone_search_radius": 2,
                      "cone_search_unit": "arcsec",
                      "radec": {"object1": [71.6577756, -10.2263957]}
                  },
                  "catalogs": {
                      "ZTF_alerts": {
                          "filter": {},
                          "projection": {"_id": 0, "candid": 1, "objectId": 1}
                      }
                  }
              },
              "kwargs": {
                  "filter_first": False
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
        # should always return a dict, even if it's empty
        assert isinstance(result['data'], dict)

    async def test_query_find_one(self, aiohttp_client):
        """
            Test {"query_type": "find_one", ...}: /api/queries
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        # authorize
        credentials = await self.auth_admin(aiohttp_client)
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
            Test {"query_type": "find", ...}: /api/queries
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        # authorize
        credentials = await self.auth_admin(aiohttp_client)
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

    async def test_query_aggregate(self, aiohttp_client):
        """
            Test {"query_type": "aggregate", ...}: /api/queries
        :param aiohttp_client:
        :return:
        """
        client = await aiohttp_client(await app_factory())

        # authorize
        credentials = await self.auth_admin(aiohttp_client)
        access_token = credentials['token']

        headers = {'Authorization': access_token}

        collection = 'ZTF_alerts'

        # check query without book-keeping
        qu = {"query_type": "aggregate",
              "query": {
                  "catalog": collection,
                  "pipeline": [{'$match': {'candid': 1127561445515015011}},
                               {"$project": {"_id": 0, "candid": 1}}],
              },
              "kwargs": {"max_time_ms": 2000}
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
        credentials = await self.auth_admin(aiohttp_client)
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
        credentials = await self.auth_admin(aiohttp_client)
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
        credentials = await self.auth_admin(aiohttp_client)
        access_token = credentials['token']

        headers = {'Authorization': access_token}

        collection = 'ZTF_alerts'

        # check catalog_names info
        qu = {"query_type": "estimated_document_count",
              "query": {"catalog": collection}}
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
