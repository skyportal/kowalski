import pytest

from ingest_ztf_source_features import run as run_ztf_source_features
from ingest_vlass import run as run_vlass
from ingest_igaps import run as run_igaps
from ingest_ztf_public import run as run_ztf_public
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
            path="/data",
            tag=tag,
            xmatch=False,
            num_processes=1,
        )

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == 123

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

    def test_ingest_ztf_public(self):
        tag = get_default_args(run_ztf_public).get("tag")
        collection = f"ZTF_public_sources_{tag}"

        run_ztf_public(path="/app/data/catalogs", num_proc=1)

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == 100
