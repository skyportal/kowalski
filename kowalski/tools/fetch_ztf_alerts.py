from bs4 import BeautifulSoup
import fire
import multiprocessing as mp
import pandas as pd
import pathlib
import requests
import subprocess
from tqdm import tqdm
from typing import Sequence
from urllib.parse import urljoin
import time

from kowalski.config import load_config
from kowalski.log import log


config = load_config(config_files=["config.yaml"])["kowalski"]


def collect_urls() -> list:
    """Collect URLs of ZTF alerts daily tarballs from UW's alert repo

    https://ztf.uw.edu/alerts/public/

    :param readout_channel: int c [0, 63]
    :return:
    """
    base_url: str = "https://ztf.uw.edu/alerts/public"

    response = requests.get(base_url)
    html = response.text

    soup = BeautifulSoup(html, "html.parser")
    links = soup.findAll("a")

    link_list = []

    for link in links:
        txt = link.getText()
        if txt.endswith(".tar.gz"):
            program = "public" if "public" in link["href"] else "private"
            url = urljoin(base_url, program + "/" + link["href"])
            name = url.split("/")[-1]
            date = name.split("_")[2].split(".")[0]
            # conver the date string to a proper date
            date = pd.to_datetime(date, format="%Y%m%d").date()
            link_list.append({"url": url, "name": name, "date": date})

    return link_list


def fetch_url(argument_list: Sequence):
    """Download ZTF alerts tarballs from UW's depo"""
    # unpack arguments
    base_path, url = argument_list

    path = base_path / pathlib.Path(url).name
    n_retries = 0
    temp_path = path.with_suffix(".tmp")
    while n_retries < 5:
        print(f"Downloading {url} to {path}")
        if not path.exists():
            try:
                subprocess.run(
                    [
                        "wget",
                        "-q",
                        "--timeout=6000",
                        "--waitretry=10",
                        "--tries=5",
                        "-O",
                        str(temp_path),
                        url,
                    ]
                )
            except Exception as e:
                log(f"Exception while downloading {url}: {e}, redownloading")
                # remove the file if it exists
                if path.exists():
                    subprocess.run(["rm", "-f", str(path)])
                n_retries += 1
                time.sleep(10)
                continue

            # remove the tmp extension
            subprocess.run(["mv", str(temp_path), str(path)])

        break

    if n_retries == 5:
        log(f"Failed to download {url} after 5 retries")


def run(
    path_out: str = "./data/ztf_alerts",
    mindate: str = None,
    maxdate: str = None,
    refresh_csv: bool = False,
    csv_only: bool = False,
):
    path = pathlib.Path(path_out)
    if not path.exists():
        path.mkdir(exist_ok=True, parents=True)

    path_urls = pathlib.Path(path_out) / "ztf_alerts.csv"
    path_exists = path_urls.exists()

    if not path_exists or refresh_csv:
        print("Collecting urls of alert tarballs to download:")

        urls = collect_urls()

        df_mf = pd.DataFrame.from_records(urls)
        df_mf.to_csv(path_urls, index=False)

    else:
        df_mf = pd.read_csv(path_urls)

    if not csv_only:

        print(f"Downloading {len(df_mf)} days of alerts")

        # convert date to date in the df
        # print the format of the date column
        # the format looks like YYYY-MM-DD
        df_mf.date = pd.to_datetime(df_mf.date, format="%Y-%m-%d").dt.date

        if mindate is not None:
            mindate = pd.to_datetime(mindate, format="%Y%m%d").date()
            df_mf = df_mf[df_mf.date >= mindate]

        if maxdate is not None:
            maxdate = pd.to_datetime(maxdate, format="%Y%m%d").date()
            df_mf = df_mf[df_mf.date <= maxdate]

        print(f"Downloading {len(df_mf)} days of alerts")

        argument_lists = [(path, row.url) for row in df_mf.itertuples()]

        # download
        with mp.Pool(processes=4) as pool:
            for _ in tqdm(
                pool.imap(fetch_url, argument_lists), total=len(argument_lists)
            ):
                pass

    else:
        print("CSV only, skipping download")
        print(f"CSV lists has {len(df_mf)} days of alerts")


if __name__ == "__main__":
    fire.Fire(run)
