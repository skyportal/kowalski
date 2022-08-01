from penquins import Kowalski

username = "admin"
password = "admin"

# for local install of kowalski
protocol, host, port = "http", "localhost", 4000


kowalski = Kowalski(
    username=username,
    password=password,
    protocol=protocol,
    host=host,
    port=port
)

## Run once and print to manually save
# token = kowalski.token
# print(token)
token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJ1c2VyX2lkIjoiYWRtaW4iLCJjcmVhdGVkX2F0IjoiMjAyMi0wOC0wMVQyMTo0NjoyOS4wNzMyMzUrMDA6MDAifQ.6JsF5UGvzWnJOEe4u6A1HxgWpPSTkNz7WM7oAeqj63Q'

kowalski = Kowalski(
    token=token,
    protocol=protocol,
    host=host,
    port=port
)

def retrieve_catalogs():
    """Retrieve available catalog names"""
    query = {
        "query_type": "info",
        "query": {
            "command": "catalog_names",
        }
    }

    response = kowalski.query(query=query)
    data = response.get("data")
    return data

def find_cand(catalog, objectId):
    """
    Find candidate in kowalski

    Arguments:
        catalog(str): catalog to query
        objectId(str): objectId querying for
    """
    q = {
        "query_type": "find",
        "query": {
            "catalog": catalog,
            "filter": {
                "objectId": objectId
            },
            "projection": {
                "_id": 0,
                "candid": 1
            }
        }
    }

    response = kowalski.query(query=q)
    data = response.get("data")
    return data


def cone_search(catalog, objectId, ra, dec):
    """
    Run a cone_search query:

    Arguments:
        catalog(str): catalog to query
        objectId(str): objectId you're searching for
        ra(int): obj's ra
        dec(int): obj's dec
    """
    query = {"query_type": "cone_search",
            "query": {
                "object_coordinates": {
                    "cone_search_radius": 2,
                    "cone_search_unit": "arcsec",
                    "radec": {
                        objectId: [
                            ra,
                            dec
                        ]
                    }
                },
                "catalogs": {
                    catalog: {
                        "filter": {},
                        "projection": {
                            # "_id": 0,
                            # "candid": 1,
                            # "objectId": 1
                            "cutoutDifference": 0,
                            "cutoutTemplate": 0,
                            "cutoutScience": 0


                        }
                    }
                }
            },
            "kwargs": {
                "filter_first": False
            }
        }

    response = kowalski.query(query=query)
    data = response.get("data")
    return data

def nearest(catalog, ra, dec, limit = 7):
    """
    Query for 7 nearest sources to a sky position, 
    sorted by the spheric distance, 
    with a near query

    Arguments:
        catalog(str): e.g."ZTF_sources_20210401"
        ra(int):
        dec(int):
    """
    query = {
        "query_type": "near",
        "query": {
            "max_distance": 2,
            "distance_units": "arcsec",
            "radec": {"query_coords": [ra, dec]},
            "catalogs": {
                catalog: {
                    "filter": {},
                    "projection": {"_id": 1},
                }
            },
        },
        "kwargs": {
            "max_time_ms": 10000,
            "limit": limit,
        },
    }

    response = k.query(query=query)
    data = response.get("data")
    return data

if __name__ == "__main__":
    print(f'ping! {kowalski.ping()}')
    print(f'catalog names avail: {retrieve_catalogs()}')
    print(f'wntr_alerts query: {find_cand("WNTR_alerts", "WNTR21aaaaaab")}')
    print(f'cone search query: {cone_search("WNTR_alerts", "null", 160.68674841985782, 34.37037400826267)}')

