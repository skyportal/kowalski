from tns_watcher import get_tns
from utils import load_config, log, Mongo


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


class TestTNSWatcher:
    """
    Test TNS monitoring
    """

    def test_tns_watcher(self):
        log("Connecting to DB")
        mongo = Mongo(
            host=config["database"]["host"],
            port=config["database"]["port"],
            username=config["database"]["username"],
            password=config["database"]["password"],
            db=config["database"]["db"],
            verbose=True,
        )
        log("Successfully connected")

        collection = config["database"]["collections"]["tns"]

        log(
            "Grabbing 1 page with 5 entries from the TNS and ingesting that into the database"
        )
        get_tns(
            grab_all=False,
            num_pages=1,
            entries_per_page=5,
        )
        log("Done")

        fetched_entries = list(mongo.db[collection].find({}, {"_id": 1}))

        assert len(fetched_entries) > 0
