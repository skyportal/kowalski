import datetime

import pytest
import lxml
import xmlschema

from kowalski.alert_brokers.alert_broker_gcn import GCNAlertWorker
from kowalski.config import load_config
from kowalski.log import log
from kowalski.tools.gcn_utils import get_dateobs, get_trigger

""" load config and secrets """
config = load_config(config_files=["config.yaml"])["kowalski"]


@pytest.fixture(autouse=True, scope="class")
def worker_fixture(request):
    log("Initializing alert processing worker class instance")
    request.cls.worker = GCNAlertWorker()
    log("Successfully initialized")
    # do not attempt monitoring filters
    request.cls.run_forever = False


@pytest.fixture(autouse=True, scope="class")
def alert_fixture(request):
    log("Loading a sample GCN notice")
    notice = "GCN"
    request.cls.notice_name = notice
    sample_avro = f"data/gcn_notices/{notice}.xml"
    print("sample_avro", sample_avro)
    with open(sample_avro, "r") as f:
        alert = f.read()
        schema = "data/schema/VOEvent-v2.0.xsd"
        voevent_schema = xmlschema.XMLSchema(schema)
        if voevent_schema.is_valid(alert):
            # check if is string
            try:
                alert = alert.encode("ascii")
            except AttributeError:
                pass
            root = lxml.etree.fromstring(alert)
        else:
            raise ValueError("xml file is not valid VOEvent")

        dateobs = get_dateobs(root)
        triggerid = get_trigger(root)
        alert = {
            "dateobs": dateobs,
            "triggerid": triggerid,
            "notice_type": "gcn.classic.voevent.FERMI_GBM_FIN_POS",
            "notice_content": lxml.etree.tostring(root),
            "date_created": datetime.datetime.utcnow(),
        }
        request.cls.alert = alert
        request.cls.root = root


class TestAlertBrokerGCN:
    """Test individual components/methods of the GCN alert processor used by the broker"""

    def test_generate_skymap(self):
        """Test generating a skymap from the alert"""
        skymap = self.worker.generate_skymap(self.root, self.alert["notice_type"])
        assert skymap is not None
        assert type(skymap) == dict

    def test_generate_contours(self):
        """Test generating contours from the alert"""
        skymap = self.worker.generate_skymap(self.root, self.alert["notice_type"])
        assert skymap is not None
        contours = self.worker.generate_contours(skymap)
        assert contours is not None
        assert type(contours) == dict
        assert "center" in contours
        assert "contour50" in contours
        assert "contour90" in contours

    def test_crossmatch_gcn_with_point(self):
        """Test crossmatching a GCN alert with a point source"""
        skymap = self.worker.generate_skymap(self.root, self.alert["notice_type"])
        assert skymap is not None
        contours = self.worker.generate_contours(skymap)
        assert contours is not None
        alert = self.alert
        alert["localization"] = contours

        # if there is already an alert in the database, delete it
        self.worker.mongo.db[self.worker.collection_alerts].delete_many(
            {"dateobs": alert["dateobs"], "triggerid": alert["triggerid"]},
        )

        self.worker.mongo.insert_one(
            collection=self.worker.collection_alerts, document=alert
        )

        # now we try to crossmatch a point source with the alert
        # for reference, the source is ZTF23aaebovb
        contour = "contour90"
        ra = 210.1037407
        dec = 64.2197152

        ra_geojson = float(ra)
        ra_geojson -= 180.0
        dec_geojson = float(dec)

        object_position_query = dict()
        object_position_query[f"localization.{contour}"] = {
            "$geoIntersects": {
                "$geometry": [ra_geojson, dec_geojson],
            }
        }
        candidate_datetime = self.alert["dateobs"] + datetime.timedelta(days=1)

        object_position_query["dateobs"] = {
            "$gte": candidate_datetime - datetime.timedelta(days=7),
            "$lte": candidate_datetime,
        }

        catalog_filter = {}
        catalog_projection = {"_id": 0, "dateobs": 1}

        s = self.worker.mongo.db[self.worker.collection_alerts].find(
            {**object_position_query, **catalog_filter}, {**catalog_projection}
        )
        xmatches = list(s)
        assert len(xmatches) == 1
        assert xmatches[0]["dateobs"] == self.alert["dateobs"]

        # this candidate should also match with contour50
        del object_position_query[f"localization.{contour}"]
        contour = "contour50"
        object_position_query[f"localization.{contour}"] = {
            "$geoIntersects": {
                "$geometry": [ra_geojson, dec_geojson],
            }
        }

        s = self.worker.mongo.db[self.worker.collection_alerts].find(
            {**object_position_query, **catalog_filter}, {**catalog_projection}
        )
        xmatches = list(s)
        assert len(xmatches) == 1
        assert xmatches[0]["dateobs"] == self.alert["dateobs"]

        # let's take a candidate with some random position outside of the contour to verify that it does not match.
        # we take it at the exact opposite side of the sky
        ra = 13.000929
        dec = -2.4187257

        ra_geojson = float(ra)
        ra_geojson -= 180.0
        dec_geojson = float(dec)

        contour = "contour90"
        object_position_query[f"localization.{contour}"] = {
            "$geoIntersects": {
                "$geometry": [ra_geojson, dec_geojson],
            }
        }

        s = self.worker.mongo.db[self.worker.collection_alerts].find(
            {**object_position_query, **catalog_filter}, {**catalog_projection}
        )
        xmatches = list(s)
        assert len(xmatches) == 0
