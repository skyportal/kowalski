import argparse
from bs4 import BeautifulSoup
import multiprocessing as mp
import os
import pandas as pd
import pathlib
import requests
import subprocess
from tqdm.auto import tqdm


from utils import deg2dms, deg2hms, great_circle_distance, in_ellipse, load_config, radec2lb, time_stamp


''' load config and secrets '''
config = load_config(config_file='config.yaml')['kowalski']


def fetch_url(urlrc, source='ipac'):
    url, _rc = urlrc

    p = os.path.join(path, str(_rc), os.path.basename(url))
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

    path = f'/_tmp/ztf_matchfiles_{t_tag}/'
    if not os.path.exists(path):
        os.makedirs(path)

    for rc in range(0, 64):
        if not os.path.exists(os.path.join(path, str(rc))):
            os.makedirs(os.path.join(path, str(rc)))

    path_urls = pathlib.Path(f'/_tmp/ztf_matchfiles_{t_tag}.csv')

    if not path_urls.exists():

        base_url = 'https://ztfweb.ipac.caltech.edu/ztf/ops/srcmatch/'

        # store urls
        urls = []

        print('Collecting urls of matchfiles to download:')

        n_rc = 1
        # n_rc = 64

        # collect urls of matchfiles to download
        for rc in tqdm(range(0, n_rc), total=n_rc):

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

                    response_fr = requests.get(bu_fr,
                                               auth=(config['ztf_depot']['username'], config['ztf_depot']['password']))
                    html_fr = response_fr.text

                    soup_fr = BeautifulSoup(html_fr, 'html.parser')
                    links_fr = soup_fr.findAll('a')

                    for link_fr in links_fr:
                        txt_fr = link_fr.getText()
                        if txt_fr.endswith('.pytable'):
                            # print('\t', txt_fr)
                            urls.append({'rc': rc, 'name': txt_fr, 'url': os.path.join(bu_fr, txt_fr)})
                            break

        # n_matchfiles = len(urls)

        df_mf = pd.DataFrame.from_records(urls)
        print(df_mf)

    else:
        df_mf = pd.read_csv(path_urls)
        print(df_mf)

    # print(f'Downloading {n_matchfiles} matchfiles:')
    #
    # for rc, urls_rc in tqdm(urls.items(), total=n_rc):
    #     # download
    #     url_list = [(u, rc) for u in urls_rc]
    #     with mp.Pool(processes=4) as p:
    #         list(tqdm(p.imap(fetch_url, url_list), total=len(urls_rc)))
    #     # move to gs
    #     subprocess.run(["/usr/local/bin/gsutil",
    #                     "-m", "mv",
    #                     f"/_tmp/ztf_matchfiles_{t_tag}/{rc}/*.pytable",
    #                     f"gs://ztf-matchfiles-{t_tag}/{rc}/"])
    #     # remove locally
    #     # subprocess.run(["rm", "rf", f"/_tmp/ztf_matchfiles_{t_tag}/{rc}/"])
