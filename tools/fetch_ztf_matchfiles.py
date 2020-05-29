import argparse
from bs4 import BeautifulSoup
import multiprocessing as mp
from multiprocessing.pool import ThreadPool
import os
import pandas as pd
import pathlib
import requests
import subprocess
from tqdm.auto import tqdm


from utils import load_config


''' load config and secrets '''
# config = load_config(path='../', config_file='config.yaml')['kowalski']
config = load_config(config_file='config.yaml')['kowalski']


def collect_urls(rc):
    bu = os.path.join(base_url, f'rc{rc:02d}')

    response = requests.get(bu, auth=(config['ztf_depot']['username'], config['ztf_depot']['password']))
    html = response.text

    # link_list = []
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.findAll('a')

    for link in links:
        txt = link.getText()
        if 'fr' in txt:
            bu_fr = os.path.join(bu, txt)

            response_fr = requests.get(
                bu_fr,
                auth=(config['ztf_depot']['username'], config['ztf_depot']['password'])
            )
            html_fr = response_fr.text

            soup_fr = BeautifulSoup(html_fr, 'html.parser')
            links_fr = soup_fr.findAll('a')

            for link_fr in links_fr:
                txt_fr = link_fr.getText()
                if txt_fr.endswith('.pytable'):
                    # print('\t', txt_fr)
                    urls.append({'rc': rc, 'name': txt_fr, 'url': os.path.join(bu_fr, txt_fr)})
                    # fixme:
                    # break


def fetch_url(urlrc, source='ipac'):
    url, _rc = urlrc

    p = os.path.join(str(path), str(_rc), os.path.basename(url))
    if not os.path.exists(p):
        if source == 'ipac':
            subprocess.run(['wget',
                            f"--http-user={config['ztf_depot']['username']}",
                            f"--http-passwd={config['ztf_depot']['password']}",
                            '-q', '--timeout=600', '--waitretry=10',
                            '--tries=5', '-O', p, url])
        elif source == 'supernova':
            _url = url.replace('https://', '/media/Data2/Matchfiles/')
            subprocess.run(['scp',
                            f'duev@supernova.caltech.edu:{_url}',
                            path])

        # time.sleep(0.5)


def gunzip(f):
    subprocess.run(['gunzip', f])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('--tag', type=str, default='20200401', help='matchfile release time tag')

    args = parser.parse_args()

    t_tag = args.tag

    path_base = pathlib.Path('./')
    # path_base = pathlib.Path('/_tmp/')

    path = path_base / f'ztf_matchfiles_{t_tag}/'
    if not path.exists():
        path.mkdir(exist_ok=True, parents=True)

    for rc in range(0, 64):
        path_rc = path / str(rc)
        if not path_rc.exists():
            path_rc.mkdir(exist_ok=True, parents=True)

    path_urls = path_base / f'ztf_matchfiles_{t_tag}.csv'

    # n_rc = 1
    n_rc = 64

    if not path_urls.exists():

        base_url = 'https://ztfweb.ipac.caltech.edu/ztf/ops/srcmatch/'

        # store urls
        urls = []

        print('Collecting urls of matchfiles to download:')

        # collect urls of matchfiles to download
        with ThreadPool(processes=20) as pool:
            list(tqdm(pool.imap(collect_urls, range(0, n_rc)), total=n_rc))

        df_mf = pd.DataFrame.from_records(urls)
        print(df_mf)
        df_mf.to_csv(path_urls, index=False)

    else:
        df_mf = pd.read_csv(path_urls)
        print(df_mf)

    # check what's (already) on GCS:
    ongs = []
    for rc in tqdm(range(0, n_rc), total=n_rc):
        ongs_rc = subprocess.check_output([
            'gsutil', 'ls',
            f'gs://ztf-matchfiles-{t_tag}/{rc}/',
        ]).decode('utf-8').strip().split('\n')
        ongs += [pathlib.Path(ong).name for ong in ongs_rc if ong.endswith('pytable')]
    # print(ongs)

    # matchfiles that are not on GCS:
    # print(df_mf['name'].isin(ongs))
    w = ~(df_mf['name'].isin(ongs))

    print(f'Downloading {w.sum()} matchfiles:')

    url_list = [(r.url, r.rc) for r in tqdm(df_mf.loc[w].itertuples())]

    # download
    with mp.Pool(processes=4) as pool:
        list(tqdm(pool.imap(fetch_url, url_list), total=len(url_list)))

    # move to GCS:
    for rc in tqdm(range(0, n_rc), total=n_rc):
        # move to gs
        subprocess.run(["/usr/local/bin/gsutil",
                        "-m", "mv",
                        str(path / f"{rc}/*.pytable"),
                        f"gs://ztf-matchfiles-{t_tag}/{rc}/"])
        # remove locally
        # subprocess.run(["rm", "rf", f"/_tmp/ztf_matchfiles_{t_tag}/{rc}/"])
