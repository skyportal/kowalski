import argparse

from kowalski.utils import Mongo, load_config


config = load_config(config_file="config.yaml")["kowalski"]
mongo = Mongo(
    host=config["database"]["host"],
    port=config["database"]["port"],
    replica_set=config["database"]["replica_set"],
    username=config["database"]["username"],
    password=config["database"]["password"],
    db=config["database"]["db"],
    srv=config["database"]["srv"],
    verbose=True,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", action="store_true")
    parser.add_argument("--clear", action="store_true")
    parser.add_argument("--coll", default="all", type=str)
    parser.add_argument("--find", action="store_true")
    args = parser.parse_args()

    if args.coll == "all":
        surveys = ["ztf", "pgir", "wntr"]
    else:
        surveys = [args.coll]

    if args.count:
        for srv in surveys:
            collection_alerts = config["database"]["collections"][f"alerts_{srv}"]
            collection_alerts_aux = config["database"]["collections"][
                f"alerts_{srv}_aux"
            ]

            n_alerts = mongo.db[collection_alerts].count_documents({})
            n_alerts_aux = mongo.db[collection_alerts_aux].count_documents({})

            print(srv, n_alerts, n_alerts_aux)

    if args.clear:
        for srv in surveys:
            collection_alerts = config["database"]["collections"][f"alerts_{srv}"]
            collection_alerts_aux = config["database"]["collections"][
                f"alerts_{srv}_aux"
            ]

            mongo.db[collection_alerts].remove({})
            mongo.db[collection_alerts_aux].remove({})

    if args.find:
        for srv in surveys:
            collection_alerts = config["database"]["collections"][f"alerts_{srv}"]
            cursor = mongo.db[collection_alerts].find()
            print([r for r in cursor])
