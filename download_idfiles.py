import os
import re
from datetime import date, timedelta

import requests


def decompress_files():
    os.system('gunzip -f *.gz')


def write_to_disk(file, url):
    r = requests.get(url)
    if r.status_code == 200:
        file.write(r.content)


def clear_folder():
    folder_content = os.listdir('.')
    for file in folder_content:
        collection, date = parse_filename(file)
        if date != YESTERDAY:
            print("removing {}".format(file))
            # os.remove(file)


def check_if_files_are_current():
    up_to_date_id_collections = []
    files = os.listdir('.')
    for file in files:
        collection, date = parse_filename(file)
        if date == YESTERDAY:
            up_to_date_id_collections.append(collection)

    return up_to_date_id_collections


def download_latest_ids():
    print("downloading yesterday's ids")

    # Download the newest ID files
    for id in ID_TYPES:
        filename = ID_TYPES[id].replace('MM_DD_YYYY', YESTERDAY)
        url = BASE_URL + filename
        # check if our file exists in gunzip or extracted format
        if os.path.exists(filename) or os.path.exists(filename.replace('.gz', '')):
            print("{} already exists. skipping...".format(filename))
            continue
        try:
            with open(filename, 'r+b') as file:
                write_to_disk(file, url)
        except FileNotFoundError:
            with open(filename, 'x+b') as file:
                write_to_disk(file, url)

    decompress_files()


def parse_filename(filename):
    try:
        collection, date_str = filename.split('_ids_')
    except ValueError:
        print("{} is probably not an ids file".format(filename))
        return [None, None]
    p = re.compile('[\d]{2}_[\d]{2}_[\d]{4}')
    results = re.search(p, date_str)

    if results:
        date_from_file = results.group(0)
        return collection, date_from_file
    return [None, None]


ID_TYPES = {
    'movies': 'movie_ids_MM_DD_YYYY.json.gz',
    'TV Series': 'tv_series_ids_MM_DD_YYYY.json.gz',
    'People': 'person_ids_MM_DD_YYYY.json.gz',
    'Collections': 'collection_ids_MM_DD_YYYY.json.gz',
    'TV Networks': 'tv_network_ids_MM_DD_YYYY.json.gz',
    'Keywords': 'keyword_ids_MM_DD_YYYY.json.gz',
    'Production Companies': 'production_company_ids_MM_DD_YYYY.json.gz'
}
FILE_TYPES = {
    'movie_ids_MM_DD_YYYY.json.gz': 'movies',
    'tv_series_ids_MM_DD_YYYY.json.gz': 'tv_series',
    'person_ids_MM_DD_YYYY.json.gz': 'person',
    'collection_ids_MM_DD_YYYY.json.gz': 'collection',
    'tv_network_ids_MM_DD_YYYY.json.gz': 'tv_network',
    'keyword_ids_MM_DD_YYYY.json.gz': 'keyword',
    'production_company_ids_MM_DD_YYYY.json.gz': 'production_company'
}
TODAY = '{:%m_%d_%Y}'.format(date.today())
YESTERDAY = '{:%m_%d_%Y}'.format(date.today() - timedelta(1))
BASE_URL = 'http://files.tmdb.org/p/exports/'

if __name__ == '__main__':
    try:
        os.chdir('ids')
    except FileNotFoundError:
        os.mkdir('ids')
        os.chdir('ids')
    # download_latest_ids()
    print(clear_folder())
