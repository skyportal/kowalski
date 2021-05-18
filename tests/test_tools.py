import pytest
from random import randrange

from ingest_ztf_source_features import run as run_ztf_source_features
from ingest_vlass import run as run_vlass
from ingest_igaps import run as run_igaps
from ingest_ztf_matchfiles import run as run_ztf_matchfiles
from utils import get_default_args, load_config, log, Mongo


""" load config and secrets """
config = load_config(config_file="config.yaml")["kowalski"]


@pytest.fixture(autouse=True, scope="class")
def mongo_fixture(request):
    log("Connecting to DB")
    mongo = Mongo(
        host=config["database"]["host"],
        port=config["database"]["port"],
        replica_set=config["database"]["replica_set"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        db=config["database"]["db"],
        verbose=True,
    )
    log("Successfully connected")

    request.cls.mongo = mongo


class TestTools:
    """
    Test (mostly data ingestion) tools
    """

    def test_ingest_ztf_source_features(self):
        tag = get_default_args(run_ztf_source_features).get("tag")
        collection = f"ZTF_source_features_{tag}"

        run_ztf_source_features(
            path="/app/data/ztf_source_features",
            tag=tag,
            xmatch=False,
            num_processes=1,
        )

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == 123

    def test_ingest_ztf_matchfiles(self):
        tag = str(randrange(10000000, 99999999, 1))
        sources_collection = f"ZTF_sources_{tag}"
        exposures_collection = f"ZTF_exposures_{tag}"
        run_ztf_matchfiles(
            path="/app/data/ztf_matchfiles",
            tag=tag,
            num_proc=1,
        )

        ingested_sources = list(self.mongo.db[sources_collection].find({}, {"_id": 1}))
        ingested_exposures = list(
            self.mongo.db[exposures_collection].find({}, {"_id": 1})
        )
        log(f"Ingested lightcurves for {len(ingested_sources)} sources")
        log(f"Ingested {len(ingested_exposures)} exposures")

        assert len(ingested_sources) == 16
        assert len(ingested_exposures) == 10

    def test_ingest_vlass(self):

        collection = "VLASS_DR1"

        run_vlass(
            path="/app/data/catalogs",
            num_processes=1,
        )

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == 27

    def test_ingest_igaps(self):

        collection = "IGAPS_DR2"

        run_igaps(
            path="/app/data/catalogs",
            num_processes=1,
        )

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == 100
