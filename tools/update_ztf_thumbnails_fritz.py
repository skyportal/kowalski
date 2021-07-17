from astropy.io import fits
from astropy.visualization import (
    AsymmetricPercentileInterval,
    LinearStretch,
    LogStretch,
    ImageNormalize,
)
import base64
import datetime
import fire
import gzip
import io
import matplotlib.pyplot as plt
import numpy as np
import pathlib
from penquins import Kowalski
import requests
import sys
from typing import Optional, Union

parent_dir = pathlib.Path(__file__).parent.absolute().parent
sys.path.append(str(parent_dir / "kowalski"))
from utils import load_config, log  # noqa: E402


""" load config and secrets """
config = load_config(path=str(parent_dir), config_file="config.yaml")["kowalski"]


KOWALSKI_PROTOCOL = "https"
KOWALSKI_HOST = "kowalski.caltech.edu"
KOWALSKI_PORT = 443

FRITZ_BASE_URL = "https://fritz.science"
FRITZ_TOKEN = config["skyportal"]["token"]


k = Kowalski(
    username=config["server"]["admin_username"],
    password=config["server"]["admin_password"],
    protocol=KOWALSKI_PROTOCOL,
    host=KOWALSKI_HOST,
    port=KOWALSKI_PORT,
    verbose=False,
)

log(f"Kowalski connection OK: {k.ping()}")


def fritz_api(method, endpoint, data=None):
    headers = {"Authorization": f"token {FRITZ_TOKEN}"}
    if method != "GET":
        response = requests.request(method, endpoint, json=data, headers=headers)
    else:
        response = requests.request(method, endpoint, params=data, headers=headers)
    return response


def make_thumbnail(alert, ttype: str, ztftype: str):
    """
    Convert lossless FITS cutouts from ZTF alerts into PNGs

    :param alert: ZTF alert packet/dict
    :param ttype: <new|ref|sub>
    :param ztftype: <Science|Template|Difference>
    :return:
    """
    cutout_data = alert[f"cutout{ztftype}"]["stampData"]
    with gzip.open(io.BytesIO(cutout_data), "rb") as f:
        with fits.open(io.BytesIO(f.read())) as hdu:
            # header = hdu[0].header
            data_flipped_y = np.flipud(hdu[0].data)
    # fixme: png, switch to fits eventually
    buff = io.BytesIO()
    plt.close("all")
    fig = plt.figure()
    fig.set_size_inches(4, 4, forward=False)
    ax = plt.Axes(fig, [0.0, 0.0, 1.0, 1.0])
    ax.set_axis_off()
    fig.add_axes(ax)

    # replace nans with median:
    img = np.array(data_flipped_y)
    # replace dubiously large values
    xl = np.greater(np.abs(img), 1e20, where=~np.isnan(img))
    if img[xl].any():
        img[xl] = np.nan
    if np.isnan(img).any():
        median = float(np.nanmean(img.flatten()))
        img = np.nan_to_num(img, nan=median)

    norm = ImageNormalize(
        img, stretch=LinearStretch() if ztftype == "Difference" else LogStretch()
    )
    img_norm = norm(img)
    normalizer = AsymmetricPercentileInterval(lower_percentile=1, upper_percentile=100)
    vmin, vmax = normalizer.get_limits(img_norm)
    ax.imshow(img_norm, cmap="bone", origin="lower", vmin=vmin, vmax=vmax)
    plt.savefig(buff, dpi=42)

    buff.seek(0)
    plt.close("all")

    thumb = {
        "obj_id": alert["objectId"],
        "data": base64.b64encode(buff.read()).decode("utf-8"),
        "ttype": ttype,
    }

    return thumb


def alert_post_thumbnails(alert):
    for ttype, ztftype in [
        ("new", "Science"),
        ("ref", "Template"),
        ("sub", "Difference"),
    ]:
        thumb = make_thumbnail(alert, ttype, ztftype)

        response = fritz_api("POST", FRITZ_BASE_URL + "/api/thumbnail", thumb)

        log(response.json())


def get_alert_by_object_id(oid: str):
    q = {
        "query_type": "find",
        "query": {
            "catalog": "ZTF_alerts",
            "filter": {
                "objectId": oid,
            },
        },
        "kwargs": {"sort": [("candidate.jd", -1)], "limit": 1},
    }
    alert = k.query(query=q).get("data")[0]

    return alert


def main(
    start_date: Optional[Union[datetime.datetime, str, int]] = None,
    end_date: Optional[Union[datetime.datetime, str, int]] = None,
    num_per_page: int = 100,
):
    if isinstance(start_date, str) or isinstance(start_date, int):
        start_date = datetime.datetime.strptime(str(start_date), "%Y%m%d")
    if isinstance(end_date, str) or isinstance(end_date, int):
        end_date = datetime.datetime.strptime(str(end_date), "%Y%m%d")

    if start_date is None:
        start_date = datetime.datetime.utcnow()
    if end_date is None:
        end_date = datetime.datetime.utcnow() + datetime.timedelta(days=1)
    print(start_date, end_date)

    if end_date < start_date:
        raise ValueError("End date must be before start date.")

    # print(get_alert_by_object_id(oid="ZTF21abiuvdk"))
    log(f"Start date: {start_date.strftime('%Y-%m-%d')}")
    log(f"End date: {end_date.strftime('%Y-%m-%d')}")
    # return
    data = fritz_api(
        "GET",
        FRITZ_BASE_URL + "/api/candidates",
        {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "numPerPage": num_per_page,
        },
    ).json()["data"]
    total_matches = data["totalMatches"]
    candidates = data["candidates"]
    log(f"Found objects: {total_matches}")
    if total_matches == 0:
        return

    num_batches = total_matches // len(candidates)
    log(f"Fetched batch #1 of {num_batches + 1}")

    for i in range(num_batches):
        data = fritz_api(
            "GET",
            FRITZ_BASE_URL + "/api/candidates",
            {
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d"),
                "numPerPage": num_per_page,
                "pageNumber": i + 2,
            },
        ).json()["data"]
        candidates.extend(data["candidates"])
        log(f"Fetched batch #{i + 2} of {num_batches + 1}")

    processed = set()
    for candidate in candidates:
        oid = candidate["id"]
        thumbnail_types = [t["type"] for t in candidate["thumbnails"]]
        if (
            "new" not in thumbnail_types
            and oid not in processed
            and (oid.startswith("ZTF1") or oid.startswith("ZTF2"))
        ):
            try:
                log(oid)
                alert = get_alert_by_object_id(oid=oid)
                alert_post_thumbnails(alert)
                processed.add(oid)
            except Exception as e:
                log(f"Got error: {e}")
                continue


if __name__ == "__main__":
    fire.Fire(main)
