from utils import Mongo
from utils import load_config
import argparse

config = load_config(config_file="config.yaml")["kowalski"]
mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            replica_set=config["database"]["replica_set"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            verbose=True,
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--count', action='store_true')
    parser.add_argument('--clear',action='store_true')
    parser.add_argument('--coll',default='all',type=str)

    args = parser.parse_args()

    if args.coll == 'all':
        surveys = ['ztf','pgir','wntr']
    else:
        surveys = [args.coll]
    
    if args.count:
        for srv in surveys:
            collection_alerts = config["database"]["collections"][f"alerts_{srv}"]
            n_alerts = mongo.db[collection_alerts].count_documents({})
            print(srv, n_alerts)

    if args.clear:
        for srv in surveys:
            collection_alerts = config["database"]["collections"][f"alerts_{srv}"]
            mongo.db[collection_alerts].remove({})
