import argparse
import multiprocessing
import os
import re

import requests

LANGUAGES = [
    "ar", "au", "br", "ca", "fr", "de", "in", "id", "it", "malaysia", "mx",
    "nz", "ph", "qc", "sg", "tw", "es", "th", "uk", "vn", "espanol", "us", "hk"
]
LISTINGS_DIR = 'sitemaps'


def get_listing(language: str):
    print('getting sitemap listing for {}.'.format(language))
    filename = 'sitemap-{}.xml'.format(language)
    response = requests.get('https://answers.yahoo.com/sitemaps/' + filename)
    if response.status_code == 403:
        print('likely no sitemaps for {}.'.format(language))
        return None
    assert response.status_code == 200 \
        and len(response.content) == int(response.headers['content-length'])
    if not os.path.isdir(LISTINGS_DIR):
        os.makedirs(LISTINGS_DIR)
    with open(os.path.join(LISTINGS_DIR, filename), 'wb') as f:
        f.write(response.content)


def get_sitemap(directory: str, url: str):
    filename = os.path.basename(url)
    print('getting sitemap {}.'.format(filename))
    while True:
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 403:
                return None
            assert response.status_code == 200
            break
        except KeyboardInterrupt as e:
            raise e
        except Exception as e:
            print('got {}, retrying.'.format(str(e)))
    with open(os.path.join(directory, filename), 'wb') as f:
        data = response.raw.read()
        assert len(data) == int(response.headers['content-length'])
        f.write(data)


def get_sitemaps(filepath: str):
    print('getting sitemaps from {}.'.format(filepath))
    directory = os.path.join('sitemaps',
                             filepath.rsplit('.', 1)[0].rsplit('-', 2)[1])
    if not os.path.isdir(directory):
        os.makedirs(directory)
    with open(filepath, 'r') as f, multiprocessing.Pool(20) as p:
        p.starmap(get_sitemap, [
            (directory, url)
            for url in re.findall(r'<loc>([^<]+)</loc>', f.read())
        ])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--listings', action='store_true',
                        help='download sitemap listings.')
    parser.add_argument('--sitemaps', action='store_true',
                        help='download sitemaps from listings.')
    args = parser.parse_args()
    assert args.listings or args.sitemaps
    if args.sitemaps and not os.path.isdir(LISTINGS_DIR):
        args.listings = True
    if args.listings:
        for language in LANGUAGES:
            get_listing(language)
    if args.sitemaps:
        for filename in os.listdir(LISTINGS_DIR):
            if not filename.endswith('.xml'):
                continue
            get_sitemaps(os.path.join(LISTINGS_DIR, filename))

