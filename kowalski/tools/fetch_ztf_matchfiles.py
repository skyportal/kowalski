from bs4 import BeautifulSoup
import fire
import multiprocessing as mp
import os
import os.path
import pandas as pd
import pathlib
import requests
import subprocess
from tqdm import tqdm
from typing import Sequence
from urllib.parse import urljoin

from utils import load_config


KOWALSKI_APP_PATH = os.environ.get(
    "KOWALSKI_APP_PATH", pathlib.Path(__file__).parent.absolute()
)
config = load_config(path=KOWALSKI_APP_PATH, config_file="config.yaml")["kowalski"]


def collect_urls(readout_channel: int) -> list:
    """Collect URLs of individual matchfiles from IPAC's depo

    Format as of April 2021:
    https://ztfweb.ipac.caltech.edu/ztf/ops/srcmatch/rc00/fr000201-000250/ztf_000245_zg_c01_q1_match.pytable

    :param readout_channel: int c [0, 63]
    :return:
    """
    base_url: str = "https://ztfweb.ipac.caltech.edu/ztf/ops/srcmatch/"

    base_url_readout_channel = urljoin(base_url, f"rc{readout_channel:02d}")

    response = requests.get(
        base_url_readout_channel,
        auth=(config["ztf_depot"]["username"], config["ztf_depot"]["password"]),
    )
    html = response.text

    soup = BeautifulSoup(html, "html.parser")
    links = soup.findAll("a")

    link_list = []

    for link in links:
        txt = link.getText()
        if "fr" not in txt:
            continue

        bu_fr = os.path.join(base_url_readout_channel, txt)

        response_fr = requests.get(
            bu_fr,
            auth=(config["ztf_depot"]["username"], config["ztf_depot"]["password"]),
        )
        html_fr = response_fr.text

        soup_fr = BeautifulSoup(html_fr, "html.parser")
        links_fr = soup_fr.findAll("a")

        for link_fr in links_fr:
            txt_fr = link_fr.getText()
            if txt_fr.endswith(".pytable"):
                link_list.append(
                    {
                        "rc": readout_channel,
                        "name": txt_fr,
                        "url": urljoin(bu_fr, txt_fr),
                    }
                )

    return link_list


def fetch_url(argument_list: Sequence):
    """Download matchfile from IPAC's depo given its url, save to base_path"""
    # unpack arguments
    base_path, url = argument_list

    path = base_path / pathlib.Path(url).name
    if not path.exists():
        subprocess.run(
            [
                "wget",
                f"--http-user={config['ztf_depot']['username']}",
                f"--http-passwd={config['ztf_depot']['password']}",
                "-q",
                "--timeout=600",
                "--waitretry=10",
                "--tries=5",
                "-O",
                str(path),
                url,
            ]
        )


def run(
    tag: str = "20210401",
    path_out: str = "./",
    force: bool = False,
    upload_to_gcp: bool = False,
    remove_upon_upload_to_gcp: bool = False,
):
    """Collect urls of matchfiles from IPAC's depo, download them, and optionally move to GCS

    :param tag: matchfiles release time tag
    :param path_out: output path for fetched data
    :param force: forcefully reload url list and re-try fetching everything
    :param upload_to_gcp: upload to Google Cloud Storage?
    :param remove_upon_upload_to_gcp: remove afterwards?
    :return:
    """

    path = pathlib.Path(path_out) / f"ztf_matchfiles_{tag}/"
    if not path.exists():
        path.mkdir(exist_ok=True, parents=True)

    n_rc = 64

    path_urls = pathlib.Path(path_out) / f"ztf_matchfiles_{tag}.csv"

    if not path_urls.exists() or force:
        # store urls
        urls = []

        print("Collecting urls of matchfiles to download:")

        # collect urls of matchfiles to download
        with mp.Pool(processes=min(mp.cpu_count(), 20)) as pool:
            for url_list in tqdm(pool.imap(collect_urls, range(0, n_rc)), total=n_rc):
                urls.extend(url_list)

        df_mf = pd.DataFrame.from_records(urls)
        print(df_mf)
        df_mf.to_csv(path_urls, index=False)

    else:
        df_mf = pd.read_csv(path_urls)
        print(df_mf)

    # check what's (already) on GCS:
    on_cloud = []
    if upload_to_gcp:
        for readout_channel in tqdm(range(0, n_rc), total=n_rc):
            on_cloud_readout_channel = (
                subprocess.check_output(
                    [
                        "gsutil",
                        "ls",
                        f"gs://ztf-matchfiles-{tag}/{readout_channel}/",
                    ]
                )
                .decode("utf-8")
                .strip()
                .split("\n")
            )
            on_cloud.extend(
                [
                    pathlib.Path(table).name
                    for table in on_cloud_readout_channel
                    if table.endswith("pytable")
                ]
            )

    # matchfiles that are not on GCS:
    mask_to_be_fetched = ~(df_mf["name"].isin(on_cloud))

    print(f"Downloading {mask_to_be_fetched.sum()} matchfiles:")

    argument_lists = [
        (path, row.url) for row in df_mf.loc[mask_to_be_fetched].itertuples()
    ]

    # download
    with mp.Pool(processes=4) as pool:
        for _ in tqdm(pool.imap(fetch_url, argument_lists), total=len(argument_lists)):
            pass

    # move to GCS:
    if upload_to_gcp:
        # move to gs
        subprocess.run(
            [
                "gsutil",
                "-m",
                "mv",
                str(path / "*.pytable"),
                f"gs://ztf-matchfiles-{tag}/",
            ]
        )
        # remove locally
        if remove_upon_upload_to_gcp:
            subprocess.run(["rm", "rf", f"/_tmp/ztf_matchfiles_{tag}/"])


if __name__ == "__main__":
    fire.Fire(run)
