from random import randrange

import pytest
from kowalski.ingesters.ingest_igaps import run as run_igaps
from kowalski.ingesters.ingest_ptf_matchfiles import run as run_ptf_matchfiles
from kowalski.ingesters.ingest_vlass import run as run_vlass
from kowalski.ingesters.ingest_ztf_matchfiles import run as run_ztf_matchfiles
from kowalski.ingesters.ingest_ztf_public import run as run_ztf_public
from kowalski.ingesters.ingest_ztf_source_classifications import (
    run as run_ztf_source_classifications,
)
from kowalski.ingesters.ingest_ztf_source_features import run as run_ztf_source_features
from kowalski.ingesters.ingest_catalog import run as run_catalog
from kowalski.utils import Mongo, get_default_args
from kowalski.config import load_config
from kowalski.log import log

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


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
        srv=config["database"]["srv"],
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

        # check if the collection exists, drop it if it does
        if collection in self.mongo.db.list_collection_names():
            log(f"Collection {collection} already exists, dropping it...")
            try:
                self.mongo.db[collection].drop()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to drop the existing ZTF source features collection: {e}"
                )

        run_ztf_source_features(
            path="data/ztf_source_features",
            tag=tag,
            xmatch=False,
            num_processes=1,
        )

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == 123

    def test_ingest_ztf_source_classifications(self):
        tag = get_default_args(run_ztf_source_classifications).get("tag")
        collection = f"ZTF_source_classifications_{tag}"

        # check if the collection exists, drop it if it does
        if collection in self.mongo.db.list_collection_names():
            log(f"Collection {collection} already exists, dropping it...")
            try:
                self.mongo.db[collection].drop()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to drop the existing ZTF source classifications collection: {e}"
                )

        run_ztf_source_classifications(
            path="data/ztf_source_classifications/",
            tag=tag,
            num_processes=1,
        )

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested classifications of {len(ingested_entries)} sources")

        assert len(ingested_entries) == 9875

    def test_ingest_ztf_matchfiles(self):
        tag = str(randrange(10000000, 99999999, 1))
        sources_collection = f"ZTF_sources_{tag}"
        exposures_collection = f"ZTF_exposures_{tag}"

        # check if the collections exist, drop them if they do
        if sources_collection in self.mongo.db.list_collection_names():
            log(f"Collection {sources_collection} already exists, dropping it...")
            try:
                self.mongo.db[sources_collection].drop()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to drop the existing ZTF sources collection: {e}"
                )

        if exposures_collection in self.mongo.db.list_collection_names():
            log(f"Collection {exposures_collection} already exists, dropping it...")
            try:
                self.mongo.db[exposures_collection].drop()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to drop the existing ZTF exposures collection: {e}"
                )

        run_ztf_matchfiles(
            path="data/ztf_matchfiles",
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

        # check if the collection exists, drop it if it does
        if collection in self.mongo.db.list_collection_names():
            log(f"Collection {collection} already exists, dropping it...")
            try:
                self.mongo.db[collection].drop()
            except Exception as e:
                raise RuntimeError(f"Failed to drop the existing VLASS collection: {e}")

        run_vlass(
            path="data/catalogs",
            num_processes=1,
        )

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == 27

    def test_ingest_igaps(self):
        collection = "IGAPS_DR2"

        # check if the collection exists, drop it if it does
        if collection in self.mongo.db.list_collection_names():
            log(f"Collection {collection} already exists, dropping it...")
            try:
                self.mongo.db[collection].drop()
            except Exception as e:
                raise RuntimeError(f"Failed to drop the existing IGAPS collection: {e}")

        run_igaps(
            path="data/catalogs",
            num_processes=1,
        )

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == 100

    def test_ingest_ztf_public(self):
        tag = get_default_args(run_ztf_public).get("tag")
        collection = f"ZTF_public_sources_{tag}"

        # check if the collection exists, drop it if it does
        if collection in self.mongo.db.list_collection_names():
            log(f"Collection {collection} already exists, dropping it...")
            try:
                self.mongo.db[collection].drop()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to drop the existing ZTF public collection: {e}"
                )

        run_ztf_public(path="data/catalogs", num_proc=1)

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == 5449

    def test_ingest_ptf(self):
        sources_collection = "PTF_sources"
        exposures_collection = "PTF_exposures"

        # check if the collections exist, drop them if they do
        if sources_collection in self.mongo.db.list_collection_names():
            log(f"Collection {sources_collection} already exists, dropping it...")
            try:
                self.mongo.db[sources_collection].drop()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to drop the existing PTF sources collection: {e}"
                )

        if exposures_collection in self.mongo.db.list_collection_names():
            log(f"Collection {exposures_collection} already exists, dropping it...")
            try:
                self.mongo.db[exposures_collection].drop()
            except Exception as e:
                raise RuntimeError(
                    f"Failed to drop the existing PTF exposures collection: {e}"
                )

        run_ptf_matchfiles(
            path="data/catalogs",
            num_proc=1,
        )

        ingested_sources = list(self.mongo.db[sources_collection].find({}, {"_id": 1}))
        ingested_exposures = list(
            self.mongo.db[exposures_collection].find({}, {"_id": 1})
        )
        log(f"Ingested lightcurves for {len(ingested_sources)} sources")
        log(f"Ingested {len(ingested_exposures)} exposures")

        assert len(ingested_sources) == 1145
        assert len(ingested_exposures) == 2

    def test_ingest_catalog_from_fits(self):
        collection = "FITS_CATALOG"

        # check if the collection exists, drop it if it does
        if collection in self.mongo.db.list_collection_names():
            log(f"Collection {collection} already exists, dropping it...")
            try:
                self.mongo.db[collection].drop()
            except Exception as e:
                raise RuntimeError(f"Failed to drop the existing CLU collection: {e}")

        total_good_docs, _ = run_catalog(
            catalog_name=collection,
            path="data/catalogs/igaps-dr1-215a-small.fits.gz",
            max_docs=100,
            format="fits",
        )

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == total_good_docs

    def test_ingest_catalog_from_csv(self):
        collection = "CSV_CATALOG"

        # check if the collection exists, drop it if it does
        if collection in self.mongo.db.list_collection_names():
            log(f"Collection {collection} already exists, dropping it...")
            try:
                self.mongo.db[collection].drop()
            except Exception as e:
                raise RuntimeError(f"Failed to drop the existing CLU collection: {e}")

        total_good_docs, _ = run_catalog(
            catalog_name=collection,
            path="data/catalogs/CIRADA_VLASS1QL_table1_components_v1_head100.csv",
            batch_size=10,
            max_docs=100,
            format="csv",
        )

        ingested_entries = list(self.mongo.db[collection].find({}, {"_id": 1}))
        log(f"Ingested features of {len(ingested_entries)} sources")

        assert len(ingested_entries) == total_good_docs
