from bs4 import BeautifulSoup
import fire
import multiprocessing as mp
import pathlib
import requests
import subprocess
from tqdm import tqdm
from typing import Sequence
from urllib.parse import urljoin
import time
from astropy.io import fits

from kowalski.log import log


def collect_urls() -> list:
    """Collect URLs of all the individual FITS files for the PS1 PSC dataset

    https://archive.stsci.edu/hlsps/ps1-psc/

    :return:
    """
    base_url: str = "https://archive.stsci.edu/hlsps/ps1-psc/"

    response = requests.get(
        base_url,
    )
    html = response.text

    soup = BeautifulSoup(html, "html.parser")
    links = soup.findAll("a")
    # remove the a which href does not start with <a href="hlsp_ps1-psc_ps1_gpc
    links = [
        link
        for link in links
        if link.get("href").startswith("hlsp_ps1-psc_ps1_gpc")
        and link.get("href").endswith(".fits")
    ]

    link_list = []

    for link in links:
        link_list.append({"name": link, "url": urljoin(base_url, link.get("href"))})

    return link_list


def fetch_url(argument_list: Sequence):
    """Download a file from a URL"""
    # unpack arguments
    base_path, url = argument_list

    path = base_path / pathlib.Path(url).name
    n_retries = 0
    while n_retries < 5:
        if not path.exists():
            try:
                subprocess.run(
                    [
                        "wget",
                        "-O",
                        str(path),
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
        # try to open the file and read the header and first row
        try:
            with fits.open(path) as hdul:
                _ = hdul[0].header
                log(f"Downloaded and validated {url} to {path}")
            break
        except Exception as e:
            log(f"Exception while reading {url}: {e}, redownloading")
            # remove the file if it exists
            if path.exists():
                subprocess.run(["rm", "-f", str(path)])
            n_retries += 1
            time.sleep(10)
            continue
        break

    if n_retries == 5:
        log(f"Failed to download {url} after 5 retries")


def run(
    path_out: str = "./data/",
    links_only: bool = False,
    n_processes: int = 4,
):
    """Download the PS1 PSC dataset from archive.stsci.edu/hlsps/ps1-psc/

    :param path_out: output path for fetched data
    :param links_only: only collect URLs, do not download
    :return:
    """

    # cap the number of processes to the number of available cores if it's higher
    # if not specified, default to 4
    n_processes = min(mp.cpu_count(), n_processes) if n_processes > 0 else 4

    links = collect_urls()
    if links_only:
        print(f"Links only, printing {len(links)} links and exiting:")
        for link in links:
            print(link["url"])
        return

    argument_lists = [(pathlib.Path(path_out), link["url"]) for link in links]

    # download
    with mp.Pool(processes=4) as pool:
        for _ in tqdm(pool.imap(fetch_url, argument_lists), total=len(argument_lists)):
            pass


if __name__ == "__main__":
    fire.Fire(run)
